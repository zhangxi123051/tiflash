#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace pingcap
{
namespace kv
{

template <>
struct RpcTypeTraits<::mpp::EstablishMPPConnectionRequest>
{
    using RequestType = ::mpp::EstablishMPPConnectionRequest;
    using ResultType = ::mpp::MPPDataPacket;
    static std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> doRPCCall(
        grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req)
    {
        return client->stub->EstablishMPPConnection(context, req);
    }
    static std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket> > doAsyncRPCCall(
            grpc::ClientContext * context, std::shared_ptr<KvConnClient> client, const RequestType & req, grpc::CompletionQueue& cq, void* call)
    {
        return client->stub->AsyncEstablishMPPConnection(context, req, &cq, call);
    }
};

} // namespace kv
} // namespace pingcap

namespace DB
{

// ExchangeReceiver is in charge of receiving data from exchangeSender located in upstream tasks.
// TODO: Currently, there is a single thread to call the receiver, we need to consider reading parallely
// in the future.
class ExchangeReceiverInputStream : public IProfilingBlockInputStream
{
    TMTContext & context;
    std::chrono::seconds timeout;

    tipb::ExchangeReceiver exchange_receiver;
    ::mpp::TaskMeta task_meta;
    std::vector<std::thread> workers;
    // async grpc
    grpc::CompletionQueue grpc_com_queue;

    DAGSchema fake_schema;
    Block sample_block;

    // TODO: should be a concurrency bounded queue.
    std::mutex rw_mu;
    std::condition_variable cv;
    std::queue<Block> block_buffer;
    std::atomic_int live_workers;
    bool inited;
    bool meet_error;
    Exception err;

    Logger * log;

    void decodePacket(const mpp::MPPDataPacket & p)
    {
        tipb::SelectResponse resp;
        resp.ParseFromString(p.data());
        int chunks_size = resp.chunks_size();
        LOG_DEBUG(log, "get chunk size " + std::to_string(chunks_size));
        if (chunks_size == 0)
            return;
        for (int i = 0; i < chunks_size; i++)
        {
            Block block;
            const tipb::Chunk & chunk = resp.chunks(i);
            switch (resp.encode_type())
            {
                case tipb::EncodeType::TypeCHBlock:
                    block = CHBlockChunkCodec().decode(chunk, fake_schema);
                    break;
                case tipb::EncodeType::TypeChunk:
                    block = ArrowChunkCodec().decode(chunk, fake_schema);
                    break;
                case tipb::EncodeType::TypeDefault:
                    block = DefaultChunkCodec().decode(chunk, fake_schema);
                    break;
                default:
                    throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }
            std::lock_guard<std::mutex> lk(rw_mu);
            block_buffer.push(std::move(block));
            cv.notify_one();
        }
    }

#if SYNC
        // Check this error is retryable
    bool canRetry(const mpp::Error & err) { return err.msg().find("can't find") != std::string::npos; }
    void startAndRead(const String & raw)
    {
        try
        {
            startAndReadImpl(raw);
        }
        catch (Exception & e)
        {
            meet_error = true;
            err = e;
        }
        catch (std::exception & e)
        {
            meet_error = true;
            err = Exception(e.what());
        }
        catch (...)
        {
            meet_error = true;
            err = Exception("fatal error");
        }
        live_workers--;
        cv.notify_all();
    }

    void startAndReadImpl(const String & raw)
    {
        // TODO: Retry backoff.
        int max_retry = 60;
        std::chrono::seconds total_wait_time;
        for (int idx = 0; idx < max_retry; idx++)
        {
            auto sender_task = new mpp::TaskMeta();
            sender_task->ParseFromString(raw);
            auto req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
            req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
            req->set_allocated_sender_meta(sender_task);
            LOG_DEBUG(log, "begin start and read : " << req->DebugString());
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            grpc::ClientContext client_context;
            auto stream_resp = context.getCluster()->rpc_client->sendStreamRequest(sender_task->address(), &client_context, call);

            stream_resp->WaitForInitialMetadata();

            mpp::MPPDataPacket packet;

            bool needRetry = false;
            while (stream_resp->Read(&packet))
            {
                if (packet.has_error()) // This is the only way that down stream pass an error.
                {
                    if (canRetry(packet.error()))
                    {
                        needRetry = true;
                        break;
                    }
                    throw Exception("exchange receiver meet error : " + packet.error().msg());
                }

                LOG_DEBUG(log, "read success");
                decodePacket(packet);
            }
            if (needRetry)
            {
                if (timeout.count() > 0 && total_wait_time > timeout)
                {
                    break;
                }
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(1s);
                total_wait_time += 1s;
                stream_resp->Finish();
                continue;
            }

            LOG_DEBUG(log, "finish worker success" << std::to_string(live_workers));
            return;
        }
        // fail
        throw Exception(
            "cannot build connection after several tries, total wait time is " + std::to_string(total_wait_time.count()) + "s.");
    }
#else
    struct ExchangeCall
    {
        using RequestType = ::mpp::EstablishMPPConnectionRequest;
        using ResultType = ::mpp::MPPDataPacket;
        grpc::ClientContext client_context;
        std::shared_ptr<RequestType> req;
        mpp::MPPDataPacket packet;
        std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket> > reader;
        enum StateType {CONNECTED,TOREAD,DONE};
        StateType state_type;
        ExchangeCall(TMTContext& tmtContext, std::string meta_str, ::mpp::TaskMeta & task_meta, grpc::CompletionQueue& cq) {
            auto sender_task = new mpp::TaskMeta();
            sender_task->ParseFromString(meta_str);
            req = std::make_shared<mpp::EstablishMPPConnectionRequest>();
            req->set_allocated_receiver_meta(new mpp::TaskMeta(task_meta));
            req->set_allocated_sender_meta(sender_task);
            pingcap::kv::RpcCall<mpp::EstablishMPPConnectionRequest> call(req);
            reader = tmtContext.getCluster()->rpc_client->sendStreamRequestAsync(req->sender_meta().address(), &client_context, call, cq, (void*)this);
            state_type = CONNECTED;
        }

    };
    std::vector<std::unique_ptr<ExchangeCall> >exchangeCalls;
    void sendAsyncReq()
    {
        for(auto & meta : exchange_receiver.encoded_task_meta()) {
            live_workers++;
            exchangeCalls.emplace_back(new ExchangeCall(context, meta, task_meta, grpc_com_queue));
            LOG_DEBUG(log, "begin start and read : " << exchangeCalls.back()->req->DebugString());
        }
    }
    void proceedAsyncReq()
    {
        void* got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue "cq".
        while (grpc_com_queue.Next(&got_tag, &ok) && live_workers > 0) {
            ExchangeCall* call = static_cast<ExchangeCall*>(got_tag);
            if (!ok) {
                call->state_type = ExchangeCall::DONE;
            }
            switch (call->state_type) {
                case ExchangeCall::StateType::CONNECTED:{
                    call->reader->Read(&call->packet,(void*)call);
                    call->state_type = ExchangeCall::StateType::TOREAD;
                } break;
                case ExchangeCall::StateType::TOREAD:{
                    // the last read() asynchronously succeed!
                    if (call->packet.has_error()) // This is the only way that down stream pass an error.
                    {
                        throw Exception("exchange receiver meet error : " + call->packet.error().msg());
                    }
                    LOG_DEBUG(log, "read success");
                    decodePacket(call->packet);
                    // issue a new read request
                    call->reader->Read(&call->packet,(void*)call);
                }break;
                case ExchangeCall::StateType::DONE: {
                    live_workers--;
                    delete call;
                }break;
                default: {
                    throw Exception("exchange receiver meet unknown msg");
                }
            }
        }
    }
#endif
public:
    ExchangeReceiverInputStream(Context & context_, const ::tipb::ExchangeReceiver & exc, const ::mpp::TaskMeta & meta)
        : context(context_.getTMTContext()),
          timeout(context_.getSettings().mpp_task_timeout),
          exchange_receiver(exc),
          task_meta(meta),
          live_workers(0),
          inited(false),
          meet_error(false),
          log(&Logger::get("exchange_receiver"))
    {

        // generate sample block
        ColumnsWithTypeAndName columns;

        for (int i = 0; i < exc.field_types_size(); i++)
        {
            String name = "exchange_receiver_" + std::to_string(i);
            fake_schema.push_back(std::make_pair(name, ColumnInfo()));

            auto tp = getDataTypeByFieldType(exc.field_types(i));
            ColumnWithTypeAndName col(tp, name);
            columns.emplace_back(col);
        }

        sample_block = Block(columns);
    }

    ~ExchangeReceiverInputStream()
    {
        for (auto & worker : workers)
        {
            worker.join();
        }
        grpc_com_queue.Shutdown();
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return "ExchangeReceiver"; }

    void init()
    {
#if SYNC
        int task_size = exchange_receiver.encoded_task_meta_size();
        for (int i = 0; i < task_size; i++)
        {
            live_workers++;
            std::thread t(&ExchangeReceiverInputStream::startAndRead, this, std::ref(exchange_receiver.encoded_task_meta(i)));
            workers.push_back(std::move(t));
        }
#else
        sendAsyncReq();
        workers.emplace_back(std::thread(&ExchangeReceiverInputStream::proceedAsyncReq, this));
#endif
        inited = true;
    }

    Block readImpl() override
    {
        if (!inited)
            init();
        std::unique_lock<std::mutex> lk(rw_mu);
        cv.wait(lk, [&] { return block_buffer.size() > 0 || live_workers == 0 || meet_error; });
        if (meet_error)
        {
            throw err;
        }
        if (block_buffer.empty())
        {
            return {};
        }
        Block blk = block_buffer.front();
        block_buffer.pop();
        return blk;
    }
};
} // namespace DB

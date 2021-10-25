#pragma once

#include <Common/CurrentMetrics.h>
#include <Encryption/RandomAccessFile.h>

#include <string>
#include <string>
#include <queue>
#include <unordered_map>
#include <mutex>

extern std::unordered_map<String, std::queue<int>> glb_fp_map;
extern std::unordered_map<String, std::string*> glb_filebin_map;
extern std::mutex filebin_map_mutex;
extern std::mutex fp_map_mutex;
namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class PosixRandomAccessFile : public RandomAccessFile
{
protected:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

public:
    PosixRandomAccessFile(const std::string & file_name_, int flags, const ReadLimiterPtr & read_limiter_ = nullptr);

    ~PosixRandomAccessFile() override;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    ssize_t pread(char * buf, size_t size, off_t offset) const override;

    std::string getFileName() const override { return file_name; }

    bool isClosed() const override { return fd == -1; }

    int getFd() const override { return fd; }

    void close() override;

private:
    std::string file_name;
    int fd;
    ReadLimiterPtr read_limiter;
    const char* local_bin;
    long long local_bin_offset;
    long long local_cap;
};

} // namespace DB

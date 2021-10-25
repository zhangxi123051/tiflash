#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/RateLimiter.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>

std::unordered_map<String, std::queue<int>> glb_fp_map;
std::unordered_map<String, std::string*> glb_filebin_map;
std::mutex filebin_map_mutex;
std::mutex fp_map_mutex;

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_SELECT;
} // namespace ErrorCodes

PosixRandomAccessFile::PosixRandomAccessFile(const std::string & file_name_, int flags, const ReadLimiterPtr & read_limiter_)
    : file_name{file_name_}
    , read_limiter(read_limiter_)
    , local_bin_offset(0)
    , local_bin(nullptr)
    , local_cap(0)
{
    if (file_name_.size()> 4 && file_name_.substr(file_name_.size()-4) == ".dat") {
        // std::cerr<<"need fp_bin, "<<file_name_<<std::endl;
        std::unique_lock<std::mutex> lock(filebin_map_mutex);
        if (glb_filebin_map.count(file_name_)) {
            std::string *str = glb_filebin_map[file_name_];
            local_bin_offset = 0;
            local_bin = str->data();
            local_cap = str->size();
            // std::cerr<<"use cached fp_bin,  "<<file_name_<<std::endl;
        } else {
            lock.unlock();
            fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);
            ::lseek(fd, 0, SEEK_END);
            off_t size = ::lseek(fd, 0, SEEK_CUR);
            local_bin = new char[size];
            local_cap = size;
            ::lseek(fd, 0, SEEK_SET);
            ::read(fd, (void *)local_bin, size);
            {
                std::unique_lock<std::mutex> lock2(filebin_map_mutex);
                glb_filebin_map[file_name_] = new std::string(local_bin, size);
            }
        }
    }
    else {
        // std::cerr<<"miss fp_bin, "<<file_name_<<std::endl;
        std::unique_lock<std::mutex> lock(fp_map_mutex);
        if (glb_fp_map.count(file_name_)) {
            auto &q = glb_fp_map[file_name_];
            if (q.size()) {
                fd = q.front();
                q.pop();
                ::lseek(fd, 0, SEEK_SET);
                // std::cerr<<"use cached fp, q.size: "<<q.size()<<std::endl;
            } else {
                ProfileEvents::increment(ProfileEvents::FileOpen);
                // std::cerr<<"new fp, "<<file_name<<std::endl;
                fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);
                // file = std::make_shared<PosixRandomAccessFile>(file_path_, flags, read_limiter);
                if (fd >= 0) {
                    ::close(fd);
                }

            }
        } else {
            glb_fp_map[file_name_] = std::queue<int>();
            ProfileEvents::increment(ProfileEvents::FileOpen);
            // std::cerr<<"new fp_queue, "<<file_name<<std::endl;
            fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);
            // file = std::make_shared<PosixRandomAccessFile>(file_path_, flags, read_limiter);
        }
    }
    

#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif
    

    if (local_bin == nullptr && -1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }
#ifdef __APPLE__
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
        {
            ProfileEvents::increment(ProfileEvents::FileOpenFailed);
            throwFromErrno("Cannot set F_NOCACHE on file " + file_name, ErrorCodes::CANNOT_OPEN_FILE);
        }
    }
#endif
}

PosixRandomAccessFile::~PosixRandomAccessFile()
{
    if (local_bin) return;
    if (fd < 0)
        return;
    {
        std::unique_lock<std::mutex> lock(fp_map_mutex);
        
        if (glb_fp_map.count(file_name)) {
            auto &q = glb_fp_map[file_name];
            q.push(fd);
            // std::cerr<<"payback fp, "<<file_name<<" size: "<<q.size()<<std::endl;
        } else {
            glb_fp_map[file_name] = std::queue<int>();
            glb_fp_map[file_name].push(fd);
            // std::cerr<<"payback fp, "<<file_name<<" size: "<<glb_fp_map[file_name].size()<<std::endl;
        }
    }
    // ::close(fd);
}

void PosixRandomAccessFile::close()
{
    if (local_bin) return;
    if (fd < 0)
        return;
    // while (::close(fd) != 0)
    //     if (errno != EINTR)
    //         throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);

    // fd = -1;
    // metric_increment.destroy();
}

off_t PosixRandomAccessFile::seek(off_t offset, int whence)
{
    if (local_bin) {
        if (whence == SEEK_CUR) {
            local_bin_offset += offset;
        } else if (whence == SEEK_SET) {
            local_bin_offset = offset;
        } else if (whence == SEEK_END) {
            local_bin_offset = local_cap + offset;
        }
        return local_bin_offset;
    }
    return ::lseek(fd, offset, whence);
}

ssize_t PosixRandomAccessFile::read(char * buf, size_t size)
{
    if (local_bin) { 
        memcpy(buf, local_bin + local_bin_offset, size);
        ssize_t ret = local_bin_offset + size > local_cap? local_cap-local_bin_offset:size;
        local_bin_offset= local_bin_offset + size>local_cap? local_cap: local_bin_offset + size;
        return ret;
    }
    off_t bak_offset = ::lseek(fd, 0, SEEK_CUR);
    if (read_limiter != nullptr)
    {
        read_limiter->request(size);
    }
    ssize_t ret = ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    // ::lseek(fd, bak_offset, SEEK_SET);
    // ::read(fd, buf, size);
    return ret;
}

ssize_t PosixRandomAccessFile::pread(char * buf, size_t size, off_t offset) const
{
       if (local_bin) { 
        // local_bin_offset = (long long) offset;
        memcpy(buf, local_bin + offset, size);
        ssize_t ret = offset + size > local_cap? local_cap-offset:size;
        // local_bin_offset= local_bin_offset + size>local_cap? local_cap: local_bin_offset + size;
        return ret;
    }
    if (read_limiter != nullptr)
    {
        read_limiter->request(size);
    }
    ssize_t ret = ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    // ::pread(fd, buf, size, offset);
    return ret;
}

} // namespace DB

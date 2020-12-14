
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.




#include "raft/FileSystemAdaptor.h"
#include "logger/logger.h"
#include <string.h>
namespace horsedb {

bool PosixDirReader::is_valid() const {
    //return _dir_reader->d_name!=nullptr;
}

bool PosixDirReader::next() {
    //bool rc = _dir_reader->Next();
    //while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
        //rc = _dir_reader->Next();
    //}
    //return rc;
}

const char* PosixDirReader::name() const {
    //return _dir_reader->d_name;
}

PosixFileAdaptor::~PosixFileAdaptor() {
}


size_t file_pwrite(const string& data, int fd,off_t offset) 
{
    size_t size = data.size();
    offset=0;
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t written = write(fd, data.data()+offset, left);
        if (written >= 0) 
        {
            offset += written;
            left -= written;
        } 
        else if (errno == EINTR) 
        {
            continue;
        } 
        else 
        {
           TLOGERROR_RAFT(  "write falied, err: " << strerror(errno)
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size <<endl);
            return -1;
        }
    }

    return size - left;
}

size_t file_pread(string& portal, int fd, off_t offset, size_t size) 
{
    offset=0;
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) 
    {
        ssize_t read_len = read(fd, portal.data()+offset, static_cast<size_t>(left));
        if (read_len > 0) 
        {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) 
        {
            break;
        } else if (errno == EINTR) 
        {
            continue;
        } else 
        {
            TLOGERROR_RAFT( "read failed, err: " << strerror(errno)
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size<<endl);
            return -1;
        }
    }

    return size - left;
}

size_t PosixFileAdaptor::write(const string& data, off_t offset) 
{
    return file_pwrite(data, _fd, offset);
}

size_t PosixFileAdaptor::read( string &portal, off_t offset, size_t size) 
{
    return file_pread(portal, _fd, offset, size);
}

size_t PosixFileAdaptor::size() 
{
    off_t sz = lseek(_fd, 0, SEEK_END);
    return size_t(sz);
}

bool PosixFileAdaptor::sync() 
{
    return fsync(_fd) == 0;
}

bool PosixFileAdaptor::close() 
{
    if (_fd > 0) {
        bool res = ::close(_fd) == 0;
        _fd = -1;
        return res;
    }
    return true;
}


static pthread_once_t s_check_cloexec_once = PTHREAD_ONCE_INIT;
static bool s_support_cloexec_on_open = false;

static void check_cloexec(void) 
{
    int fd = ::open("/dev/zero", O_RDONLY | O_CLOEXEC, 0644);
    s_support_cloexec_on_open = (fd != -1);
    if (fd != -1) {
        ::close(fd);
    }
}

FileAdaptor* PosixFileSystemAdaptor::open(const std::string& path, int oflag,const horsedb::TarsStructBase* file_meta,
                              int* e) 
{//todo
    
    pthread_once(&s_check_cloexec_once, check_cloexec);
    bool cloexec = (oflag & O_CLOEXEC);
    if (cloexec && !s_support_cloexec_on_open) 
    {
        oflag &= (~O_CLOEXEC);
    }
    int fd = ::open(path.c_str(), oflag, 0644);
    
    if (fd == -1) {
        return NULL;
    }
    if (cloexec && !s_support_cloexec_on_open) 
    {
        //butil::make_close_on_exec(fd);
    }
    return new PosixFileAdaptor(fd);
}

bool PosixFileSystemAdaptor::delete_file(const std::string& path, bool recursive) 
{

    return ::remove(path.c_str());
}

bool PosixFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
    return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
    return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::create_directory(const std::string& path, int* error, bool create_parent_directories) 
{
    return ::mkdir(path.c_str(),0x0777);
}

bool PosixFileSystemAdaptor::path_exists(const std::string& path) 
{
    if (!path.empty() && access(path.c_str(), F_OK) == 0) 
    {
        return true;
    }
    return false;
}

bool PosixFileSystemAdaptor::directory_exists(const std::string& path) 
{
    if (!path.empty() && access(path.c_str(), F_OK) == 0) 
    {
        return true;
    }
    return false;
}

DirReader* PosixFileSystemAdaptor::directory_reader(const std::string& path) 
{
    return new PosixDirReader(path.c_str());
}


// Get a default file system adapotor, it's a singleton PosixFileSystemAdaptor.
FileSystemAdaptor* default_file_system(){return new PosixFileSystemAdaptor();};

bool create_sub_directory(const std::string& parent_path,
                          const std::string& sub_path,
                          FileSystemAdaptor* fs) 
{
    return true;

}


} //  namespace 

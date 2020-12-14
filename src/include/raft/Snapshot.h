
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



#ifndef _RAFT_SNAPSHOT_H
#define _RAFT_SNAPSHOT_H

#include <map>
#include <string>
#include <memory>

#include "raft/Storage.h"
#include "raft/RaftDB.h"

#include "raft/FileSystemAdaptor.h"
using namespace std;

namespace horsedb {

class LocalSnapshotMetaTable {
public:
    LocalSnapshotMetaTable();
    ~LocalSnapshotMetaTable();
    // Add file to the meta
    int add_file(const std::string& filename,  const LocalFileMeta& file_meta);
    int remove_file(const std::string& filename);
    int save_to_file(FileSystemAdaptor* fs, const std::string& path) const;

    int save_to_db() const;
    int load_from_db() ;
    int load_from_file(FileSystemAdaptor* fs, const std::string& path);
    int get_file_meta(const std::string& filename, LocalFileMeta* file_meta) const;
    void list_files(std::vector<std::string> *files) const;
    bool has_meta() { return _meta.lastIncludedIndex>0; }
    const SnapshotMeta& meta() { return _meta; }
    void set_meta(const SnapshotMeta& meta) { _meta = meta; }

    string getSnapshotMetaKey() const {return  string("SnapshotMetaKey") ;}

    void swap(LocalSnapshotMetaTable& rhs) 
    {
        _file_map=std::move(rhs._file_map) ;
        _meta=std::move(rhs._meta);
    }
private:
    // Intentionally copyable
    typedef std::map<std::string, LocalFileMeta> Map;
    Map    _file_map;
    SnapshotMeta _meta;
    shared_ptr<DBBase> _dbbase;
};

class LocalSnapshotWriter : public SnapshotWriter {
friend class LocalSnapshotStorage;
public:
    int64_t snapshot_index();
    virtual int init();
    virtual int save_meta(const SnapshotMeta& meta);
    virtual std::string get_path() { return _path; }
    // Add file to the snapshot. It would fail it the file doesn't exist nor
    // references to any other file.
    // Returns 0 on success, -1 otherwise.
    virtual int add_file(const std::string& filename, const LocalFileMeta& file_meta);
    // Remove a file from the snapshot, it doesn't guarantees that the real file
    // would be removed from the storage.
    virtual int remove_file(const std::string& filename);
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename,   LocalFileMeta* meta);
    // Sync meta table to disk
    int sync();
    FileSystemAdaptor* file_system() { return _fs.get(); }
private:
    // Users shouldn't create LocalSnapshotWriter Directly
    LocalSnapshotWriter(const std::string& path, 
                        FileSystemAdaptor* fs);
    virtual ~LocalSnapshotWriter();

    std::string _path;
    LocalSnapshotMetaTable _meta_table;
    shared_ptr<FileSystemAdaptor>  _fs;
};

class LocalSnapshotReader: public SnapshotReader {
friend class LocalSnapshotStorage;
public:
    LocalSnapshotReader(){}
    virtual ~LocalSnapshotReader();
    int64_t snapshot_index();
    virtual int init();
    virtual int load_meta(SnapshotMeta* meta);
    // Get the path of the Snapshot
    virtual std::string get_path() { return _path; }
    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy();
    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files);

    // Get the implementation-defined file_meta
    virtual int get_file_meta(const std::string& filename, LocalFileMeta* meta);
private:
    // Users shouldn't create LocalSnapshotReader Directly
    LocalSnapshotReader(const std::string& path,
                        horsedb::EndpointInfo server_addr,
                        FileSystemAdaptor* fs);

    void destroy_reader_in_file_service();

    std::string _path;
    LocalSnapshotMetaTable _meta_table;
    horsedb::EndpointInfo _addr;
    int64_t _reader_id;
    shared_ptr<FileSystemAdaptor> _fs;
    //shared_ptr<SnapshotThrottle> _snapshot_throttle;
};


class LocalSnapshotStorage : public SnapshotStorage {
friend class LocalSnapshotCopier;
public:
    explicit LocalSnapshotStorage(const std::string& path);
                         
    LocalSnapshotStorage() {}
    virtual ~LocalSnapshotStorage();

    static const char* _s_temp_path;

    virtual int init();
    virtual SnapshotWriter* create() ;
    virtual int close(SnapshotWriter* writer);
    virtual int close() ;

    virtual SnapshotReader* open() ;
    virtual int close(SnapshotReader* reader);
    virtual SnapshotReader* copy_from(const std::string& uri) ;

    virtual int set_filter_before_copy_remote();
    virtual int set_file_system_adaptor(FileSystemAdaptor* fs);



    void set_server_addr(horsedb::EndpointInfo server_addr) { _addr = server_addr; }
    bool has_server_addr() { return !_addr.isEmpty(); }
private:
    SnapshotWriter* create(bool from_empty) ;
    int destroy_snapshot(const std::string& path);
    int close(SnapshotWriter* writer, bool keep_data_on_error);
    void ref(const int64_t index);
    void unref(const int64_t index);

    std::mutex  _mutex;
    std::string _path;
    bool _filter_before_copy_remote;
    int64_t _last_snapshot_index;
    std::map<int64_t, int> _ref_map;
    horsedb::EndpointInfo _addr;
    shared_ptr<FileSystemAdaptor> _fs;

};

}  //  namespace raft

#endif //~RAFT_RAFT_SNAPSHOT_H


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

#ifndef  _STORAGE_
#define  _STORAGE_

#include "raft/Configuration.h"
#include "kv/DBBase.h"
#include "raft/RaftState.h"
#include "raft/FileSystemAdaptor.h"
#include "logger/logger.h"
namespace horsedb {

    class LogStorage {
public:
    virtual ~LogStorage() {}

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager) = 0;

    // first log index in log
    virtual int64_t first_log_index() = 0;

    // last log index in log
    virtual int64_t last_log_index() = 0;

    // get logentry by index
    virtual LogEntry* get_entry(const int64_t index) = 0;

    virtual bool get_entry(const int64_t index,LogEntry &tLogEntry) = 0;

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index) = 0;

    // append entries to log
    virtual int append_entry(const LogEntry& entry) = 0;

    // append entries to log and update IOMetric, return append success number 
    virtual int append_entries(const std::vector<LogEntry>& entries) = 0;

    // delete logs from storage's head, [first_log_index, first_index_kept) will be discarded
    virtual int truncate_prefix(const int64_t first_index_kept) = 0;

    // delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded
    virtual int truncate_suffix(const int64_t last_index_kept) = 0;

    // Drop all the existing logs and reset next log index to |next_log_index|.
    // This function is called after installing snapshot from leader
    virtual int reset(const int64_t next_log_index) = 0;


    // set term and votedfor information
    virtual int set_term_and_votedfor(const int64_t term, const PeerId& peer_id, const VersionedGroupId& group) = 0;

    // get term and votedfor information
    virtual int get_term_and_votedfor(int64_t* term, PeerId* peer_id, const VersionedGroupId& group) = 0;


    static  LogStorage* create(const string& type, shared_ptr<DBBase> &pDB);

    
};

// Snapshot 
class Snapshot:public RaftState  {
public:
    Snapshot() {}
    virtual ~Snapshot() {}

    // Get the path of the Snapshot
    virtual std::string get_path() = 0;

    // List all the existing files in the Snapshot currently
    virtual void list_files(std::vector<std::string> *files) = 0;

    // Get the implementation-defined file_meta
    // virtual int get_file_meta(const std::string& filename, 
    //                           ::google::protobuf::Message* file_meta) {
    //     (void)filename;
    //     file_meta->Clear();
    //     return 0;
    // }
};

class SnapshotWriter : public Snapshot {
public:
    SnapshotWriter() {}
    virtual ~SnapshotWriter() {}

    // Save the meta information of the snapshot which is used by the raft
    // framework.
    virtual int save_meta(const SnapshotMeta& meta) = 0;

    // Add a file to the snapshot.
    // |file_meta| is an implmentation-defined protobuf message 
    // All the implementation must handle the case that |file_meta| is NULL and
    // no error can be raised.
    // Note that whether the file will be created onto the backing storage is
    // implementation-defined.
    virtual int add_file(const std::string& filename) { 
        return add_file(filename, LocalFileMeta());
    }

    virtual int add_file(const std::string& filename, 
                         const LocalFileMeta& file_meta) = 0;

    // Remove a file from the snapshot
    // Note that whether the file will be removed from the backing storage is
    // implementation-defined.
    virtual int remove_file(const std::string& filename) = 0;
};

class SnapshotReader : public Snapshot {
public:
    SnapshotReader() {}
    virtual ~SnapshotReader() {}

    // Load meta from 
    virtual int load_meta(SnapshotMeta* meta) = 0;

    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy() = 0;
};

class SnapshotStorage {
public:
    virtual ~SnapshotStorage() {}

    virtual int set_filter_before_copy_remote() 
    {
        
        TLOGERROR_RAFT("doesn't support filter before copy remote"<<endl);
        return -1;
    }

    virtual int set_file_system_adaptor(FileSystemAdaptor* fs) 
    {
        (void)fs;
        
        TLOGERROR_RAFT("doesn't support file system adaptor"<<endl);
        return -1;
    }

    // virtual int set_snapshot_throttle(SnapshotThrottle* st) {
    //     (void)st;
        
    //                   TLOGERROR_RAFT( " doesn't support snapshot throttle"<<endl);
    //     return -1;
    // }

    // Initialize
    virtual int init() = 0;

    // create new snapshot writer
    virtual SnapshotWriter* create() = 0;

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) = 0;

    // get lastest snapshot reader
    virtual SnapshotReader* open() = 0;

    // close snapshot reader
    virtual int close(SnapshotReader* reader) = 0;

    // Copy snapshot from uri and open it as a SnapshotReader
    virtual SnapshotReader* copy_from(const std::string& uri)  = 0;


    // Create an instance of this kind of SnapshotStorage with the parameters encoded 
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.

    static SnapshotStorage* create(const std::string& uri);
    

    static int destroy(const std::string& uri);
};


}


#endif
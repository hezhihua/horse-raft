

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

#ifndef  _RAFT_LOG_MANAGER_H
#define  _RAFT_LOG_MANAGER_H


#include <deque>                            
#include "raft/Raft.h"                          
#include "raft/RaftDB.h"   
#include "raft/LogEntryContext.h"                     
#include "raft/Configuration.h"        
#include "raft/Storage.h"   
namespace horsedb {


class FSMCaller;

struct LogManagerOptions {
    LogManagerOptions();
    LogStorage* log_storage;
    ConfigurationManager* configuration_manager;
    FSMCaller* fsm_caller;  
};

struct LogManagerStatus {
    LogManagerStatus()
        : first_index(1), last_index(0), disk_index(0), known_applied_index(0)
    {}
    int64_t first_index;
    int64_t last_index;
    int64_t disk_index;
    int64_t known_applied_index;
};


class  LogManager {
public:
    typedef int64_t WaitId;


    LogManager();
     ~LogManager();
    int init(const LogManagerOptions& options);

    void shutdown();


    void append_entries( std::vector<LogEntry> &entries);

    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
     void set_snapshot(const SnapshotMeta* meta);

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
     void clear_bufferred_logs();

    // Get the log at |index|
    // Returns:
    //  success return ptr, fail return null
    bool get_entry(const int64_t index,LogEntry &entry);

    // Get the log term at |index|
    // Returns:
    //  success return term > 0, fail return 0
    int64_t get_term(const int64_t index);

    // Get the first log index of log
    // Returns:
    //  success return first log index, empty return 0
    int64_t first_log_index();

    // Get the last log index of log
    // Returns:
    //  success return last memory and logstorage index, empty return 0
    int64_t last_log_index(bool is_flush = false);
    
    // Return the id the last log.
    LogId last_log_id(bool is_flush = false);

    void get_configuration(int64_t index, ConfigurationEntry* conf);

    // Check if |current| should be updated to the latest configuration
    // Returns true and |current| is assigned to the lastest configuration, returns
    // false otherweise
    bool check_and_set_configuration(ConfigurationEntry* current);

    // Wait until there are more logs since |last_log_index| and |on_new_log| 
    // would be called after there are new logs or error occurs
    WaitId wait(int64_t expected_last_log_index,
                int (*on_new_log)(void *arg, int error_code), void *arg);

    // Remove a waiter
    // Returns:
    //  - 0: success
    //  - -1: id is Invalid
    int remove_waiter(WaitId id);
    
    
    // Set the applied id, indicating that the log before applied_id (inclded)
    // can be droped from memory logs
    void set_applied_id(const LogId& applied_id);

    // Check the consistency between log and snapshot, which must satisfy ANY
    // one of the following condition
    //   - Log starts from 1. OR
    //   - Log starts from a positive position and there must be a snapshot
    //     of which the last_included_id is in the range 
    //     [first_log_index-1, last_log_index]
    // Returns butil::Status::OK if valid, a specific error otherwise
    RaftState check_consistency();

    void describe(std::ostream& os, bool use_html);

    // Get the internal status of LogManager.
    void get_status(LogManagerStatus* status);


private:


    //class AppendBatcher; todo 批量刷入磁盘
    void append_to_storage(std::vector<LogEntry*>* to_append, LogId* last_id);

    static int disk_thread(void* meta);
    
    // delete logs from storage's head, [1, first_index_kept) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_prefix(const int64_t first_index_kept, std::unique_lock<std::mutex>& lck);
    
    int reset(const int64_t next_log_index,std::unique_lock<std::mutex>& lck);

    // Must be called in the disk thread, otherwise the
    // behavior is undefined
    void set_disk_id(const LogId& disk_id);

    bool get_entry_from_memory(const int64_t index,LogEntry &tLogEntry);

    WaitId notify_on_new_log(int64_t expected_last_log_index);

    int check_and_resolve_conflict(std::vector<LogEntry>& entries);

    void unsafe_truncate_suffix(const int64_t last_index_kept);

    // Clear the logs in memory whose id <= the given |id|
    void clear_memory_logs(const LogId& id);

    int64_t unsafe_get_term(const int64_t index);

    // Start a independent thread to append log to LogStorage
    int start_disk_thread();
    int stop_disk_thread();

    void wakeup_all_waiter(std::unique_lock<std::mutex>& lck);
    static void *run_on_new_log(void* arg);



    
    LogStorage* _log_storage;

    ConfigurationManager* _config_manager;
    FSMCaller* _fsm_caller;

    std::mutex _mutex;

    bool _stopped;
    std::atomic<bool> _has_error;
    WaitId _next_wait_id;

    LogId _disk_id;
    LogId _applied_id;//应用到状态机的最后的index

    std::deque<LogEntry> _logs_in_memory;
    int64_t _first_log_index;
    int64_t _last_log_index;
    // the last snapshot's log_id
    LogId _last_snapshot_id;
    // the virtual first log, for finding next_index of replicator, which 
    // can avoid install_snapshot too often in extreme case where a follower's
    // install_snapshot is slower than leader's save_snapshot
    // [NOTICE] there should not be hole between this log_id and _last_snapshot_id,
    // or may cause some unexpect cases
    LogId _virtual_first_log_id;

};

}  

#endif  

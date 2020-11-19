
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


    static  LogStorage* create(const string& type);

    
};




}


#endif
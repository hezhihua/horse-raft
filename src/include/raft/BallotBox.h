
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


#ifndef  _RAFT_BALLOT_BOX_H
#define  _RAFT_BALLOT_BOX_H

#include <stdint.h>                             // int64_t
#include <set>                                  // std::set
#include <deque>

#include "raft/Raft.h"
#include "raft/Ballot.h"
#include "raft/LogEntryContext.h"
namespace horsedb {

class FSMCaller;

struct BallotBoxOptions {
    BallotBoxOptions() : _waiter(NULL) {}
    FSMCaller* _waiter;
    deque<ClientContext>  _vContext;
};

struct BallotBoxStatus {
    BallotBoxStatus()
        : _committed_index(0), _pending_index(0), _pending_queue_size(0)
    {}
    int64_t _committed_index;
    int64_t _pending_index;
    int64_t _pending_queue_size;
};

class BallotBox {
public:
    BallotBox();
    ~BallotBox();

    int init(const BallotBoxOptions& options);

    // Called by leader, otherwise the behavior is undefined
    // Set logs in [first_log_index, last_log_index] are stable at |peer|.
    int commit_at(int64_t first_log_index, int64_t last_log_index,const PeerId& peer);

    // Called when the leader steps down, otherwise the behavior is undefined
    // When a leader steps down, the uncommitted user applications should 
    // fail immediately, which the new leader will deal whether to commit or
    // truncate.
    int clear_pending_tasks();
    
    // Called when a candidate becomes the new leader, otherwise the behavior is
    // undefined.
    // According the the raft algorithm, the logs from pervious terms can't be 
    // committed until a log at the new term becomes committed, so 
    // |new_pending_index| should be |last_log_index| + 1.
    int reset_pending_index(int64_t new_pending_index);

    // Called by leader, otherwise the behavior is undefined
    // Store application context before replication.
    int append_pending_task(const Configuration& conf, const Configuration* old_conf, ClientContext *ClientContext);

    // Called by follower, otherwise the behavior is undefined.
    // Set commited index received from leader
    int set_last_committed_index(int64_t last_committed_index);

    int64_t last_committed_index() 
    { 
        return _last_committed_index.load(std::memory_order_acquire); 
    }

    void describe(std::ostream& os, bool use_html);

    void get_status(BallotBoxStatus* ballot_box_status);

    int pop_until(int64_t index, std::vector<ClientContext*> &out, int64_t *out_first_index);

private:

    FSMCaller*                                      _waiter;
                      
    std::mutex                                      _mutex;
    std:: atomic<int64_t>                            _last_committed_index;
    int64_t                                         _pending_index;//待提交日志索引
    std::deque<Ballot>                              _pending_meta_queue;//每一次log的commit都需要过半数的同意,用队列记录待提交日志的投票/反馈结果

    deque<ClientContext *>  _vContext;

    int64_t  _first_index; //for _vContext

};

}  //  namespace horsedb

#endif  


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

#ifndef _RAFT_STATE_
#define _RAFT_STATE_
#include <string>
using namespace std;

#define    DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);                \
    TypeName& operator=(const TypeName&)

namespace horsedb {

    enum RaftError {
    EOK=0,
    // Start from 5000, to avoid HTTP error and RPC error (0 ~100, 1000 ~
    // 3000),

    // All Kinds of Timeout(Including Election_timeout, Timeout_now, Stepdown_timeout)
    ERAFTTIMEDOUT = 10001,

    ESTATEMACHINE = 10002,  // Bad User State Machine
    ECATCHUP = 10003, // Catchup Failed
    
    // Trigger step_down(Not All)
    ELEADERREMOVED = 10004,  // Configuration_change_done When 
                             //Leader Is Not In The New Configuration
    ESETPEER = 10005,  // Set_peer
    ENODESHUTDOWN = 10006,  // Shut_down
    EHIGHERTERMREQUEST = 10007,  // Receive Higher Term Requests
    EHIGHERTERMRESPONSE = 10008,  // Receive Higher Term Response
    EBADNODE = 10009,  // Node Is In Error
    EVOTEFORCANDIDATE = 10010,  // Node Votes For Some Candidate 
    ENEWLEADER = 10011,  // Follower(without leader) or Candidate Receives
                         // Append_entries/Install_snapshot Request from a new leader
    ELEADERCONFLICT = 10012,  // More Than One Leader In One Term
   
    // Trigger on_leader_stop 
    ETRANSFERLEADERSHIP = 10013,  // Leader Transfer Leadership To A Follower
    // The log at the given index is deleted
    ELOGDELETED = 10014,
    // No available user log to read
    ENOMOREUSERLOG = 10015,
    // Raft node in readonly mode
    EREADONLY = 10016,


    EUNKOWN= 99999
};


struct RaftState{

    RaftState():_state(EUNKOWN),_msg("unkown"){}
    RaftState(int raftError,const string &msg):_state(raftError),_msg(msg){}
    bool ok(){return _state==0;}
    const string &msg() const {return _msg;}
    const int &code() const {return _state;}
    

    int _state;
    string _msg;

};
inline std::ostream& operator<<(std::ostream& os, const RaftState& tRaftState) 
{
        os << "_state=" << tRaftState.code()
        << ", _msg=" << tRaftState.msg()
        << "}";
        return os;
 }


}


#endif
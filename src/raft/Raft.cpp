#include "raft/Raft.h"
#include "raft/RaftState.h"

namespace horsedb{

    StateMachine::~StateMachine()
    {

    }

    void StateMachine::on_shutdown() {}
    void StateMachine::on_snapshot_save(){}
    void StateMachine::on_snapshot_save(const SnapshotMeta &meta){}
    int StateMachine::on_snapshot_load(){}
    void StateMachine::on_leader_start(int64_t term){}
    void StateMachine::on_leader_stop(){}
    void StateMachine::on_error(){}
     void StateMachine::on_configuration_committed(const Configuration& conf){}
     void StateMachine::on_configuration_committed(const Configuration& conf, int64_t index){}
     void StateMachine::on_stop_following(const LeaderChangeContext& ctx){}
     void StateMachine::on_start_following(const LeaderChangeContext& ctx){}


    std::ostream& operator<<(std::ostream& os, const RaftState& tRaftState) 
    {
          return  os << "{_state=" << tRaftState.code()
            << ", _msg=" << tRaftState.msg()
            << "}";
            
    }
}
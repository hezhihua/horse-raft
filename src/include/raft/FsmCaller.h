
// 
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



#ifndef  __RAFT_FSM_CALLER_H
#define  __RAFT_FSM_CALLER_H


#include "raft/BallotBox.h"
#include "raft/Configuration.h"

#include "raft/LogEntryContext.h"
#include "raft/RaftState.h"
#include "raft/Replicator.h"



namespace horsedb {

class NodeImpl;
class LogManager;
class StateMachine;
//class SnapshotMeta;

struct LogEntry;

//class LeaderChangeContext;
// This class encapsulates the parameter of on_start_following and on_stop_following interfaces.
class LeaderChangeContext {
    DISALLOW_COPY_AND_ASSIGN(LeaderChangeContext);
public:
    LeaderChangeContext(const PeerId& leader_id, int64_t term, const RaftState& status)
        : _leader_id(leader_id)
        , _term(term) 
        , _st(status)
    {};
    // for on_start_following, the leader_id and term are of the new leader;
    // for on_stop_following, the leader_id and term are of the old leader.
    const PeerId& leader_id() const { return _leader_id; }
    int64_t term() const { return _term; }
    // return the information about why on_start_following or on_stop_following is called.
    const RaftState& status() const { return _st; }
        
private:
    PeerId _leader_id;
    int64_t _term;
    RaftState _st;
};

inline std::ostream& operator<<(std::ostream& os, const LeaderChangeContext& ctx) {
    os << "{ leader_id=" << ctx.leader_id()
       << ", term=" << ctx.term()
       << ", status=" << ctx.status()
       << "}";
    return os;
}

// Backing implementation of Iterator
class IteratorImpl {
    DISALLOW_COPY_AND_ASSIGN(IteratorImpl);
public:
    // Move to the next
    void next();
    const LogEntry& entry() const { return _cur_entry; }
    bool is_good() const { return _cur_index <= _committed_index ; }
    const ClientContext* done() const;

    int64_t index() const { return _cur_index; }

private:
    IteratorImpl(StateMachine* sm, LogManager* lm, 
                 std::vector<ClientContext> &vContext,
                 int64_t first_closure_index,
                 int64_t last_applied_index,
                 int64_t committed_index,
                 std::atomic<int64_t>* applying_index);
    ~IteratorImpl() {}
friend class FSMCaller;
    StateMachine* _sm;
    LogManager* _lm;
    std::vector<ClientContext> _vClientContext;
    int64_t _first_closure_index;
    int64_t _cur_index;
    int64_t _committed_index;
    LogEntry _cur_entry;
    std::atomic<int64_t>* _applying_index;

};

struct FSMCallerOptions {
    FSMCallerOptions() 
        : log_manager(NULL)
        , fsm(NULL)
        , node(NULL)
        , usercode_in_pthread(false)
        , bootstrap_id()
    {}
    LogManager *log_manager;
    StateMachine *fsm;

    NodeImpl* node;
    bool usercode_in_pthread;
    LogId bootstrap_id;
};


/**
 * 处理已经 commit 的log,应用到状态机.
 */
class  FSMCaller : public TC_Thread, public TC_ThreadLock{
public:
    FSMCaller(size_t iQueueCap);
     ~FSMCaller();

     enum TaskType {
        IDLE,
        COMMITTED,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        LEADER_STOP,
        LEADER_START,
        START_FOLLOWING,
        STOP_FOLLOWING,
        ERROR,
    };

    struct LeaderStartContext 
    {
        LeaderStartContext(int64_t term, int64_t lease_epoch) : _term(term), _lease_epoch(lease_epoch){}

        int64_t _term;
        int64_t _lease_epoch;
    };

    struct ApplyTask 
    {
        TaskType type;
        union {
            // For applying log entry (including configuration change)
            int64_t committed_index;
            
            // For on_leader_start
            LeaderStartContext* leader_start_context;
            
            // For on_leader_stop
            //butil::Status* status;    

            // For on_start_following and on_stop_following
            LeaderChangeContext* leader_change_context;

            
        };
    };


         /**
     * 结束处理线程
     */
    void terminate();

    
    /**
     * 插入
     */
    void push_back(ApplyTask &msg);

    /**
     * 从队列中取消息后执行回调逻辑
     */
    void run();


    int init(const FSMCallerOptions& options);
    int shutdown();
     int on_committed(int64_t committed_index);
     int on_snapshot_load();
     int on_snapshot_save();

    int on_leader_stop(const RaftState& status);
    int on_leader_start(int64_t term, int64_t lease_epoch);
    int on_start_following(const LeaderChangeContext& start_following_context);
    int on_stop_following(const LeaderChangeContext& stop_following_context);
     int on_error(const Error& e);
    int64_t last_applied_index() const 
    {
        return _last_applied_index.load(std::memory_order_relaxed);
    }
    int64_t applying_index() const;
    void describe(std::ostream& os, bool use_html);
    void join();
private:



/**
     * 是否需要退出
     */
    bool            _terminate;

    /**
     * 任务队列
     */
    TC_CasQueue<ApplyTask> *_ApplyTaskQueue;

    /**
     * 队列流量控制
     */
    size_t _iQueueCap;

    

    

    static double get_cumulated_cpu_time(void* arg);
    static int run(void* meta, ApplyTask& iter);
    void do_shutdown(); //Closure* done);
    void do_committed(int64_t committed_index);
    void do_cleared(int64_t log_index,  int error_code);
    void do_snapshot_save();
    void do_snapshot_load();
    void do_on_error();
    void do_leader_stop();
    void do_leader_start(const LeaderStartContext& leader_start_context);
    void do_start_following(const LeaderChangeContext& start_following_context);
    void do_stop_following(const LeaderChangeContext& stop_following_context);
    //void set_error(const Error& e);
    void batchProcess();
    void time2process();



    LogManager *_log_manager;
    StateMachine *_fsm;

    BallotBox* _ballot_box;

    std::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;

    NodeImpl* _node;
    TaskType _cur_task;
    std::atomic<int64_t> _applying_index;
    //Error _error;
    bool _queue_started;

    int64_t  _last_time_processed;
};

};

#endif  //BRAFT_FSM_CALLER_H

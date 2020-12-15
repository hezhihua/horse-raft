
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


#ifndef _RAFT_NODE_H
#define _RAFT_NODE_H

#include <set>

#include "raft/Raft.h"
#include "raft/RaftDB.h"
#include "raft/LogManager.h"
#include "raft/BallotBox.h"

#include "raft/FsmCaller.h"
#include "raft/Replicator.h"
#include "raft/SnapshotExecutor.h"

#include "raft/Configuration.h"
#include "client/util/tc_timer.h"
#include "raft/Lease.h"
#include "raft/AsyncLogThread.h"

#define raft_step_down_when_vote_timedout true
#define raft_election_heartbeat_factor 10


namespace horsedb {

 class LogStorage;

// class SnapshotStorage;
 class SnapshotExecutor;
// class StopTransferArg;




class  NodeImpl{


friend class VoteBallotCtx;
public:
    NodeImpl(const GroupId& group_id, const PeerId& peer_id);
    NodeImpl();

    NodeId node_id() const 
    {
        return NodeId(_group_id, _server_id);
    }

    PeerId leader_id() 
    {
        std::lock_guard<std::mutex> lck(_mutex);
        return _leader_id;
    }

    bool is_leader() 
    {
        std::lock_guard<std::mutex> lck(_mutex);
        return _state == STATE_LEADER;
    }

    // public user api
    //
    // init node
    int init(const NodeOptions& options);

    // shutdown local replica
    // done is user defined function, maybe response to client or clean some resource
    void shutdown();

    // Block the thread until the node is successfully stopped.
    void join();

    // apply task to the replicated-state-machine
    //
    // About the ownership:
    // |task.data|: for the performance consideration, we will take way the 
    //              content. If you want keep the content, copy it before call
    //              this function
    // |task.done|: If the data is successfully committed to the raft group. We
    //              will pass the ownership to StateMachine::on_apply.
    //              Otherwise we will specifit the error and call it.
    //

    void apply(const Task& task);

    int list_peers(std::vector<PeerId>* peers);

    // @Node configuration change
    void add_peer(const PeerId& peer);
    void remove_peer(const PeerId& peer);
    void change_peers(const Configuration& new_peers);
    int reset_peers(const Configuration& new_peers);

    // trigger snapshot
    void snapshot();

    // trigger vote
    int vote(int election_timeout);

    // reset the election_timeout for the very node
    int reset_election_timeout_ms(int election_timeout_ms);
    void reset_election_timeout_ms(int election_timeout_ms, int max_clock_drift_ms);

    // rpc request proc func
    //
    // handle received PreVote
    int handle_pre_vote_request(const RequestVoteReq& request, RequestVoteRes&  response);
    // handle received RequestVote
    int handle_request_vote_request(const RequestVoteReq&  request, RequestVoteRes&  response);

    // handle received AppendEntries
    int handle_append_entries_request(const AppendEntriesReq&  request, AppendEntriesRes&  response);

    // handle received InstallSnapshot
    int handle_install_snapshot_request(const InstallSnapshotReq&  request, InstallSnapshotRes&  response);

    int handle_timeout_now_request(const TimeoutNowReq&  request,TimeoutNowRes&  response);
    // timer func
    //follower心跳随机超时,检测leader是否还有心跳过来 
    static void handle_election_timeout(NodeImpl *);
    //候选人选自己为主的超时时间,
    static void handle_vote_timeout(NodeImpl *);

    void handle_stepdown_timeout();
    void handle_snapshot_timeout();
    void handle_transfer_timeout(int64_t term, const PeerId& peer);

    // Closure call func
    //
    void handle_pre_vote_response(const PeerId& peer_id, const int64_t term,
                                  const int64_t ctx_version,
                                  const RequestVoteRes& response);
    void handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                      const int64_t ctx_version,
                                      const RequestVoteRes& response);
    void on_caughtup(const PeerId& peer, int64_t term, 
                     int64_t version, int st);
    // other func
    //
    // called when leader change configuration done, ref with FSMCaller
    void on_configuration_change_done(int64_t term);

    // Called when leader lease is safe to start.
    void leader_lease_start(int64_t lease_epoch);

    // called when leader recv greater term in AppendEntriesResponse, ref with Replicator
    int increase_term_to(int64_t new_term, const RaftState& status);

    // Temporary solution
    void update_configuration_after_installing_snapshot();

    void describe(std::ostream& os, bool use_html);
 
    // Get the internal status of this node, the information is mostly the same as we
    // see from the website, which is generated by |describe| actually.
    void get_status(NodeStatus* status);

    // Readonly mode func
    void enter_readonly_mode();
    void leave_readonly_mode();
    bool readonly();
    int change_readonly_config(int64_t term, const PeerId& peer_id, bool readonly);
    void check_majority_nodes_readonly();
    void check_majority_nodes_readonly(const Configuration& conf);

    // Lease func
    bool is_leader_lease_valid();
    void get_leader_lease_status(LeaderLeaseStatus* status);

    // Call on_error when some error happens, after this is called.
    // After this point:
    //  - This node is to step down immediately if it was the leader.
    //  - Any futuer operation except shutdown would fail, including any RPC
    //    request.
    void on_error(int e);

    int transfer_leadership_to(const PeerId& peer);
    
    int read_committed_user_log(const int64_t index);

    int bootstrap(const BootstrapOptions& options);

    bool disable_cli() const { return _options.disable_cli; }

private:


    virtual ~NodeImpl();
    // internal init func
    int init_snapshot_storage();
    int init_log_storage();
    int init_meta();
    int init_fsm_caller(const LogId& bootstrap_index);
    void unsafe_register_conf_change(const Configuration& old_conf,
                                     const Configuration& new_conf);
    void stop_replicator(const std::set<PeerId>& keep,
                         const std::set<PeerId>& drop);

    // become leader
    void become_leader();

    // step down to follower, status give the reason
    void step_down(const int64_t term, bool wakeup_a_candidate,const RaftState& status);

    // reset leader_id. 
    // When new_leader_id is NULL, it means this node just stop following a leader; 
    // otherwise, it means setting this node's leader_id to new_leader_id.
    // status gives the situation under which this method is called.
    void reset_leader_id(const PeerId& new_leader_id, const RaftState& status);

    // check weather to step_down when receiving append_entries/install_snapshot
    // requests.
    void check_step_down(const int64_t term, const PeerId& server_id);

    // pre vote before elect_self
    void pre_vote(std::unique_lock<std::mutex> &lck);

    // elect self to candidate
    void elect_self();

    // grant self a vote
    class VoteBallotCtx;
    void grant_self(VoteBallotCtx* vote_ctx);
    static void on_grant_self_timedout(void* arg);
    static void* handle_grant_self_timedout(void* arg);

    // leader async apply configuration
    void unsafe_apply_configuration(const Configuration& new_conf,
                                    const Configuration* old_conf,
                                    bool leader_start);

    void do_snapshot();

    void after_shutdown();
    static void after_shutdown(NodeImpl* node);

    void do_apply();

    void initProxy();


    static void check_dead_nodes(NodeImpl* node);

    // bool handle_out_of_order_append_entries(brpc::Controller* cntl,
    //                                         const AppendEntriesReq* request,
    //                                         AppendEntriesResponse* response,
    //                                         google::protobuf::Closure* done,
    //                                         int64_t local_last_index);
    void check_append_entries_cache(int64_t local_last_index);
    void clear_append_entries_cache();
    static void* handle_append_entries_from_cache(void* arg);
    static void on_append_entries_cache_timedout(void* arg);
    static void* handle_append_entries_cache_timedout(void* arg);

    int64_t last_leader_active_timestamp();
    int64_t last_leader_active_timestamp(const Configuration& conf);
    void unsafe_reset_election_timeout_ms(int election_timeout_ms,
                                          int max_clock_drift_ms);

    void on_transfer_timeout(void* arg) ;



private:

    class ConfigurationCtx {
    DISALLOW_COPY_AND_ASSIGN(ConfigurationCtx);
    public:
        enum Stage {
            // Don't change the order if you are not sure about the usage
            STAGE_NONE = 0,
            STAGE_CATCHING_UP = 1,
            STAGE_JOINT = 2,
            STAGE_STABLE = 3,
        };
        ConfigurationCtx(NodeImpl* node) : _node(node), _stage(STAGE_NONE), _version(0) {}
        void list_new_peers(std::vector<PeerId>* new_peers) const 
        {
            new_peers->clear();
            std::set<PeerId>::iterator it;
            for (it = _new_peers.begin(); it != _new_peers.end(); ++it) 
            {
                new_peers->push_back(*it);
            }
        }
        void list_old_peers(std::vector<PeerId>* old_peers) const 
        {
            old_peers->clear();
            std::set<PeerId>::iterator it;
            for (it = _old_peers.begin(); it != _old_peers.end(); ++it) 
            {
                old_peers->push_back(*it);
            }
        }
        const char* stage_str() 
        {
            const char* str[] = {"STAGE_NONE", "STAGE_CATCHING_UP", 
                                 "STAGE_JOINT", "STAGE_STABLE", };
            if (_stage <= STAGE_STABLE) 
            {
                return str[(int)_stage];
            } 
            else 
            {
                return "UNKNOWN";
            }
        }
        int32_t stage() const { return _stage; }
        void reset(){};
        bool is_busy() const { return _stage != STAGE_NONE; }
        // Start change configuration.
        void start(const Configuration& old_conf,  const Configuration& new_conf );
        // Invoked when this node becomes the leader, write a configuration
        // change log as the first log
        void flush(const Configuration& conf, const Configuration& old_conf);
        void next_stage(){};
        void on_caughtup(int64_t version, const PeerId& peer_id, bool succ);
    private:
        NodeImpl* _node;
        Stage _stage;
        int _nchanges;
        int64_t _version;
        std::set<PeerId> _new_peers;
        std::set<PeerId> _old_peers;
        std::set<PeerId> _adding_peers;
       
    };

    // A versioned ballot for vote and prevote
    struct GrantSelfArg;
    class VoteBallotCtx {
    public:
        VoteBallotCtx() :  _version(0), _grant_self_arg(NULL) { }
        void init(NodeImpl* node);
        void grant(const PeerId& peer) 
        {
            _ballot.grant(peer);
        }
        bool granted() 
        {
            return _ballot.granted();
        }
        int64_t version() 
        {
            return _version;
        }
        void start_grant_self_timer( NodeImpl* node);
        void stop_grant_self_timer(NodeImpl* node);
        void reset(NodeImpl* node);
    private:
        TC_Timer _timer;
        Ballot _ballot;
        // Each time the vote ctx restarted, increase the version to avoid
        // ABA problem.
        int64_t _version;
        GrantSelfArg* _grant_self_arg;
    };

    struct GrantSelfArg {
        NodeImpl* node;
        int64_t vote_ctx_version;
        VoteBallotCtx* vote_ctx;
    };
    class StopTransferArg {
    DISALLOW_COPY_AND_ASSIGN(StopTransferArg);
    public:
        StopTransferArg(NodeImpl* n, int64_t t, const PeerId& p)
            : node(n), term(t), peer(p) {
            
        }
        ~StopTransferArg() {
            
        }
        NodeImpl* node;
        int64_t term;
        PeerId peer;
    };
public:
    State _state;
    int64_t _current_term;
    bool _leader_start;
    PeerId _leader_id;
    PeerId _voted_id;
    VoteBallotCtx _vote_ctx; // candidate vote ctx
    VoteBallotCtx _pre_vote_ctx; // prevote ctx
    ConfigurationEntry _conf;

    GroupId _group_id;
    VersionedGroupId _v_group_id;
    PeerId _server_id;
    NodeOptions _options;

    std::mutex _mutex;
    ConfigurationCtx _conf_ctx;

    ConfigurationManager* _config_manager;
    LogManager* _log_manager;
    FSMCaller* _fsm_caller;
    BallotBox* _ballot_box;

    ReplicatorGroup _replicator_group;


    TC_Timer _election_timer;//检测leader心跳是否超时 线程
    TC_Timer _vote_timer;//检测选举是否超时
    TC_Timer _stepdown_timer;
    TC_Timer _snapshot_timer;

    TC_Timer _transfer_timer;//禅让,管理命令用 

   SnapshotExecutor* _snapshot_executor;


    StopTransferArg* _stop_transfer_arg;
    bool _vote_triggered;
    //ReplicatorId _waking_candidate;

    int64_t _append_entries_cache_version;

    // for readonly mode
    bool _node_readonly;
    bool _majority_nodes_readonly;

    LeaderLease _leader_lease;
    FollowerLease _follower_lease;

    map<string,RaftDBPrx> _mPrx;

    AsyncLogThread  *_pAsyncLogThread;
    FSMCaller  *_pFsmCaller;
    LogStorage* _log_storage;

    int64_t _waking_candidate;
};

}

#endif //

#include "raft/Node.h"
#include "raft/NodeManager.h"
#include "raft/LogManager.h"
#include "raft/RaftState.h"
#include "raft/RaftDBCallback.h"
#include "logger/logger.h"
#include "cfg/config.h"

namespace horsedb {


    void NodeImpl::VoteBallotCtx::reset(NodeImpl* node) 
    {
        stop_grant_self_timer(node);
        ++_version;
    }
   

    void NodeImpl::VoteBallotCtx::start_grant_self_timer(NodeImpl* node) 
    {
        _timer.startTimer();

        GrantSelfArg* timer_arg = new GrantSelfArg;
        timer_arg->node = node;
        timer_arg->vote_ctx_version = _version;
        timer_arg->vote_ctx = this;
        _timer.postRepeated(1000,1000,NodeImpl::handle_grant_self_timedout,(void *)timer_arg);
    }
    void NodeImpl::VoteBallotCtx::stop_grant_self_timer(NodeImpl* node) 
    {
        _timer.stopTimer();
    }

     void* NodeImpl::handle_grant_self_timedout(void* arg) 
    {
        GrantSelfArg*  grant_arg = (GrantSelfArg*)arg;
        NodeImpl*      node = grant_arg->node;
        VoteBallotCtx* vote_ctx = grant_arg->vote_ctx;
        int64_t        vote_ctx_version = grant_arg->vote_ctx_version;

        delete grant_arg;

        std::unique_lock<std::mutex> lck(node->_mutex);
        if (!is_active_state(node->_state) ||vote_ctx->version() != vote_ctx_version) 
        {
            
            return NULL;
        }
        node->grant_self(vote_ctx);
        
        return NULL;
    }


    void NodeImpl::on_transfer_timeout(void* arg) 
    {
        StopTransferArg* a = (StopTransferArg*)arg;
        a->node->handle_transfer_timeout(a->term, a->peer);
        delete a;
    }

    void NodeImpl::handle_transfer_timeout(int64_t term, const PeerId& peer) 
    {
        TLOGINFO_RAFT( "node " << node_id()  << " failed to transfer leadership to peer="
                << peer << " : reached timeout"<<endl);
        std::unique_lock<std::mutex> lck(_mutex);
        if (term == _current_term) 
        {
            _replicator_group.stop_transfer_leadership(peer);
            if (_state == STATE_TRANSFERRING) 
            {
                _leader_lease.on_leader_start(term);
                _fsm_caller->on_leader_start(term, _leader_lease.lease_epoch());
                _state = STATE_LEADER;
                _stop_transfer_arg = NULL;
            }
        }
    }


    //进入follwer角色,并开始检测leader心跳是否超时,超时则触发选举
    void NodeImpl::step_down(const int64_t term, bool wakeup_a_candidate,  const RaftState& status) 
    {
        TLOGINFO_RAFT(   "node " << _group_id << ":" << _server_id
                << " term " << _current_term 
                << " stepdown from " << state2str(_state)
                << " new_term " << term
                << " wakeup_a_candidate=" << wakeup_a_candidate<<endl);

        if (!is_active_state(_state)) 
        {
            return;
        }
        // delete timer and something else
        if (_state == STATE_CANDIDATE) 
        {
            _vote_timer.stopTimer();
            _vote_ctx.reset(this);
        } 
        else if (_state == STATE_FOLLOWER) 
        {
            _pre_vote_ctx.reset(this);
        } 
        else if (_state <= STATE_TRANSFERRING) 
        {
            _stepdown_timer.stopTimer();
            _ballot_box->clear_pending_tasks();

            // signal fsm leader stop immediately
            if (_state == STATE_LEADER) 
            {
                _leader_lease.on_leader_stop();
                _fsm_caller->on_leader_stop(status);
            }
        }

        // reset leader_id 
        PeerId empty_id;
        reset_leader_id(empty_id, status);

        // soft state in memory
        _state = STATE_FOLLOWER;
        // _conf_ctx.reset() will stop replicators of catching up nodes
        _conf_ctx.reset();
        _majority_nodes_readonly = false;

        //clear_append_entries_cache();

        if (_snapshot_executor) 
        {
            _snapshot_executor->interrupt_downloading_snapshot(term);
        }

        // meta state
        if (term > _current_term) 
        {
            _current_term = term;
            _voted_id.reset();

            if (_log_storage->set_term_and_votedfor(_current_term, _voted_id, _v_group_id)!=0)
            {
                TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                    << " fail to set_term_and_votedfor when step_down, "<<endl)
                    
            }
        }

        // stop stagging new node
        if (wakeup_a_candidate) 
        {
            _replicator_group.stop_all_and_find_the_next_candidate((Replicator*)&_waking_candidate, _conf);
            // FIXME: We issue the RPC in the critical section, which is fine now
            // since the Node is going to quit when reaching the branch
            Replicator::send_timeout_now_and_stop((Replicator*)&_waking_candidate);
        } 
        else 
        {
            _replicator_group.stop_all();//todo , only leader can do this?
        }

        if (_stop_transfer_arg != NULL) //中断让位
        {
            
            _transfer_timer.stopTimer();
            
                // Get the right to delete _stop_transfer_arg.
                delete _stop_transfer_arg;
              // else on_transfer_timeout will delete _stop_transfer_arg

            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark _stop_transfer_arg to NULL
            _stop_transfer_arg = NULL;
        }
        _election_timer.startTimer();
        _election_timer.postRepeated(1000,1000,NodeImpl::handle_election_timeout,this);

    }

    void NodeImpl::handle_vote_timeout(NodeImpl* node) 
    {
        std::unique_lock<std::mutex> lck(node->_mutex);

        // check state
        if (node->_state != STATE_CANDIDATE) 
        {
            return;
        }
        if (raft_step_down_when_vote_timedout) 
        {
            // step down to follower
            TLOGWARN_RAFT( "node " << node->node_id()<< " term " << node->_current_term  << " steps down when reaching vote timeout: fail to get quorum vote-granted");
            RaftState status(ERAFTTIMEDOUT, "Fail to get quorum vote-granted");
            node->step_down(node->_current_term, false, status);
            node->pre_vote();
        } 
        else 
        {
            // retry vote
            TLOGWARN_RAFT( "node " << node->_group_id << ":" << node->_server_id<< " term " << node->_current_term << " retry elect");
            node->elect_self();
        }
    }


    int NodeImpl::init_fsm_caller(const LogId& bootstrap_id) 
    {
        
        FSMCallerOptions fsm_caller_options;
        fsm_caller_options.usercode_in_pthread = _options.usercode_in_pthread;
        
        
        fsm_caller_options.log_manager = _log_manager;
        fsm_caller_options.fsm = _options.fsm;

        fsm_caller_options.node = this;
        fsm_caller_options.bootstrap_id = bootstrap_id;
        int ret = _fsm_caller->init(fsm_caller_options);
        if (ret != 0) 
        {
            TLOGWARN_RAFT(  "_fsm_caller->init ERROR " <<endl);
        }
        return ret;
    }

    int NodeImpl::init_snapshot_storage()
    {
        return 0;
    }

    static inline int heartbeat_timeout(int election_timeout) 
    {
        return std::max(election_timeout / raft_election_heartbeat_factor, 10);
    }


    int NodeImpl::init(const NodeOptions& options) 
    {
        _options = options;

        // check _server_id
        if ( _server_id._addr.isEmpty()) 
        {
            TLOGWARN_RAFT(  "Group " << _group_id << " Node can't started from IP_ANY" <<endl);
            return -1;
        }

        if (!NodeManager::getInstance()->server_exists(_server_id._addr)) 
        {
            TLOGERROR_RAFT( "Group " << _group_id
                    << " No RPC Server attached to " << _server_id._addr
                    << ", did you forget to call braft::add_service()?"<<endl );
            return -1;
        }

        

        _config_manager = new ConfigurationManager();


        _fsm_caller = new FSMCaller(10000);

        _leader_lease.init(options.election_timeout_ms);
        _follower_lease.init(options.election_timeout_ms, options.max_clock_drift_ms);

        if (init_log_storage() !=0)
        {
            TLOGERROR_RAFT( "init_log_storage failed" <<endl);
            return -1;
        }
        if (init_meta()!=0)
        {
            TLOGERROR_RAFT( "init_meta failed" <<endl);
            return -1;
        }

        if (init_fsm_caller(LogId(0, 0)) != 0) 
        {
            TLOGERROR_RAFT("node " << _group_id << ":" << _server_id
                    << " init_fsm_caller failed" <<endl);
            return -1;
        }

        // commitment manager init
        _ballot_box = new BallotBox();
        BallotBoxOptions ballot_box_options;
        ballot_box_options._waiter = _fsm_caller;
        if (_ballot_box->init(ballot_box_options) != 0) 
        {
            TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                            << " init _ballot_box failed"<<endl);
            return -1;
        }

        if (init_snapshot_storage() != 0) 
        {
            TLOGERROR_RAFT(  "node " << _group_id << ":" << _server_id
                    << " init_snapshot_storage failed" <<endl);
            return -1;
        }

        RaftState st = _log_manager->check_consistency();
        if (!st.ok()) 
        {
            TLOGERROR_RAFT(  "node " << _group_id << ":" << _server_id
                    << " is initialized with inconsitency log: "+  st.msg()<<endl);
            return -1;
        }

        _conf.id = LogId();
        // if have log using conf in log, else using conf in options
        if (_log_manager->last_log_index() > 0) 
        {
            _log_manager->check_and_set_configuration(&_conf);
        } 
        else 
        {
            _conf.conf = _options.initial_conf;
        }

        // init replicator
        ReplicatorGroupOptions rg_options;
        rg_options.heartbeat_timeout_ms = heartbeat_timeout(_options.election_timeout_ms);
        rg_options.election_timeout_ms = _options.election_timeout_ms;
        rg_options.log_manager = _log_manager;
        rg_options.ballot_box = _ballot_box;
        rg_options.node = this;
        //rg_options.snapshot_throttle = _options.snapshot_throttle? _options.snapshot_throttle->get(): NULL;
        //rg_options.snapshot_storage = _snapshot_executor? _snapshot_executor->snapshot_storage(): NULL;
        _replicator_group.init(NodeId(_group_id, _server_id), rg_options);

        // set state to follower
        _state = STATE_FOLLOWER;

        TLOGINFO_RAFT(  "node " << _group_id << ":" << _server_id << " init,"
                << " term: " << _current_term
                << " last_log_id: " << _log_manager->last_log_id()
                << " conf: " << _conf.conf
                << " old_conf: " << _conf.old_conf <<endl);

        // start snapshot timer
        // if (_snapshot_executor && _options.snapshot_interval_s > 0) {
        //     TLOGINFO_RAFT(  "node " << _group_id << ":" << _server_id
        //                << " term " << _current_term << " start snapshot_timer"<<endl);
        //     _snapshot_timer.start();
        // }



       if (!_conf.empty()) 
       {
           //开始选举
            step_down(_current_term, false, RaftState(EOK,"ok"));
        }

        NodeManager::getInstance()->add(this);

        // Now the raft node is started , have to acquire the lock to avoid race
        // conditions
        std::unique_lock<std::mutex> lck(_mutex);
        if (_conf.stable() && _conf.conf.size() == 1u && _conf.conf.contains(_server_id)) 
        {
            // The group contains only this server which must be the LEADER, trigger
            // the timer immediately.
            elect_self();
        }
        return 0;

    }

    void NodeImpl::reset_leader_id(const PeerId& new_leader_id,  const RaftState& status) 
    {
        if (new_leader_id.is_empty()) 
        {
            if (!_leader_id.is_empty() && _state > STATE_TRANSFERRING) 
            {
                LeaderChangeContext stop_following_context(_leader_id,  _current_term, status);
                _fsm_caller->on_stop_following(stop_following_context);
            }
            _leader_id.reset();
        }
        else 
        {
            if (_leader_id.is_empty()) 
            {
                _pre_vote_ctx.reset(this);
                LeaderChangeContext start_following_context(new_leader_id,  _current_term, status);
                _fsm_caller->on_start_following(start_following_context);
            }
            _leader_id = new_leader_id;
        }
    }
    
    int NodeImpl::init_log_storage() 
    {
        
        if (_options.log_storage) 
        {
            _log_storage = _options.log_storage;
        } 
        else 
        {
            _log_storage = LogStorage::create(G_LogStorageType);
        }
        if (!_log_storage) 
        {
            TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                            << " find log storage failed, G_LogStorageType: " << G_LogStorageType <<endl);
            return -1;
        }
        _log_manager = new LogManager();
        LogManagerOptions log_manager_options;
        log_manager_options.log_storage = _log_storage;
        log_manager_options.configuration_manager = _config_manager;
        log_manager_options.fsm_caller = _fsm_caller;
        return _log_manager->init(log_manager_options);
    }
    int NodeImpl::init_meta() 
    {
        // get term and votedfor
        int iRet = _log_storage->get_term_and_votedfor(&_current_term, &_voted_id, _v_group_id);
        if (iRet!=0) 
        {
            TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                        << " failed to get term and voted_id when init meta storage,"<<endl)
            return -1;
        }

        return 0;
    }


    int NodeImpl::bootstrap(const BootstrapOptions& options) 
    {
        if (options.last_log_index > 0) 
        {
            if (options.group_conf.empty() || options.fsm == NULL) 
            {
                TLOGERROR_RAFT(  "Invalid arguments : " 
                            << "group_conf=" << options.group_conf
                            << " fsm=" << options.fsm
                            << " while last_log_index="<< options.last_log_index <<endl);
                return -1;
            }
        }

        if (options.group_conf.empty()) 
        {
            TLOGERROR_RAFT( "bootstraping an empty node makes no sense" <<endl);
            return -1;
        }

        // Term is not an option since changing it is very dangerous
        const int64_t boostrap_log_term = options.last_log_index ? 1 : 0;
        const LogId boostrap_id(options.last_log_index, boostrap_log_term);

        _options.fsm = options.fsm;
        _options.node_owns_fsm = options.node_owns_fsm;
        _options.usercode_in_pthread = options.usercode_in_pthread;
        _options.log_uri = options.log_uri;
        _options.raft_meta_uri = options.raft_meta_uri;
        _options.snapshot_uri = options.snapshot_uri;
        _config_manager = new ConfigurationManager();

        // Create _fsm_caller first as log_manager needs it to report error
        _fsm_caller = new FSMCaller(10000);

        if (init_log_storage() !=0)
        {
            TLOGERROR_RAFT( "init_log_storage failed" <<endl);
            return -1;
        }
        if (init_meta()!=0)
        {
            TLOGERROR_RAFT( "init_meta failed" <<endl);
            return -1;
        }

        if (_current_term == 0) 
        {
            _current_term = 1;
            if(_log_storage->set_term_and_votedfor(1, PeerId(), _v_group_id)!=0)
            {
                TLOGERROR_RAFT( "Fail to set term and votedfor when bootstrap " <<endl);
                return -1;
            }
            return -1;
        }

        if (options.fsm && init_fsm_caller(boostrap_id) != 0) 
        {
            TLOGERROR_RAFT( "Fail to init fsm_caller"<<endl);
            return -1;
        }

        return 0;

    }




    void NodeImpl::elect_self() 
    {
        TLOGINFO_RAFT("node " << _group_id << ":" << _server_id << " term " << _current_term << " start vote and grant vote self"<<endl);
        if (!_conf.contains(_server_id)) 
        {
            TLOGWARN_RAFT("node " << _group_id << ':' << _server_id<< " can't do elect_self as it is not in " << _conf.conf<<endl);
            return;
        }
        // cancel follower election timer
        if (_state == STATE_FOLLOWER) 
        {
           TLOGINFO_RAFT( "node " << _group_id << ":" << _server_id  << " term " << _current_term << " stop election_timer"<<endl);
            _election_timer.stopTimer();
        }
        // reset leader_id before vote
        PeerId empty_id;
        RaftState status(ERAFTTIMEDOUT, "A follower's leader_id is reset to NULL ,as it begins to request_vote.");

        reset_leader_id(empty_id, status);

        _state = STATE_CANDIDATE;//状态变为候选人
        _current_term++;//任期加1
        _voted_id = _server_id; //选自己

        TLOGINFO_RAFT( "node " << _group_id << ":" << _server_id<< " term " << _current_term << " start vote_timer"<<endl);

        _vote_timer.startTimer();//起一个定时线程如果时间到了但未选到自己，继续执行elect_self
        _vote_timer.postRepeated(1000,1000,NodeImpl::handle_vote_timeout,this);

        _pre_vote_ctx.reset(this);//stop_grant_self_timer
        _vote_ctx.init(this);

        int64_t old_term = _current_term;

        LogId last_log_id;
        
        {
            // get last_log_id outof node mutex
            //todo lock?
            last_log_id  = _log_manager->last_log_id(true);

        }

        // vote need defense ABA after unlock&lock
        if (old_term != _current_term) 
        {
            // term changed cause by step_down
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id<< " raise term " << _current_term << " when get last_log_id"<<endl);
            return;
        }
        std::set<PeerId> peers;
        _conf.list_peers(&peers);//peers也有可能为一个节点

        for (auto iter = peers.begin(); iter != peers.end(); ++iter) 
        {
            if (*iter == _server_id) 
            {
                continue;
            }

            if (_mPrx.find(iter->to_string())==_mPrx.end())
            {
                TLOGWARN_RAFT("can't find proxy:"+iter->to_string()<<endl );
                continue;
            }
            
            auto &proxy=_mPrx[iter->to_string()];

            horsedb::RequestVoteReq tRequestVoteReq;
            RaftDBPrxCallbackPtr callback(new RaftDBCallback());

            tRequestVoteReq.groupID=_group_id;
            tRequestVoteReq.serverID=_server_id.to_string();
            tRequestVoteReq.peerID=iter->to_string();
            tRequestVoteReq.term=_current_term;
            tRequestVoteReq.lastLogIndex=last_log_id.index;
            tRequestVoteReq.lastLogTerm=last_log_id.term;

            proxy->async_requestVote(callback,tRequestVoteReq);
            
        }

        //TODO: outof lock
        //选举信息落地,重启的时候需要读取

        if (_log_storage->set_term_and_votedfor(_current_term, _server_id, _v_group_id)!=0)
        {
            TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                   << " fail to set_term_and_votedfor itself when elect_self"<<endl)
                   _voted_id.reset(); 
        }
        
        


        // if (!status.ok()) 
        // {
        //     TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id<< " fail to set_term_and_votedfor itself when elect_self, error: " << status;
        //     // reset _voted_id to avoid inconsistent cases
        //     // return immediately without granting _vote_ctx
        //     _voted_id.reset(); 
        // }
        grant_self(&_vote_ctx);

    }

    // in lock
    void NodeImpl::become_leader() 
    {
        if (_state != STATE_CANDIDATE) 
        {
            TLOGERROR_RAFT("_state != STATE_CANDIDATE" );
            return;
        }
        TLOGINFO_RAFT("node " << _group_id << ":" << _server_id << " term " << _current_term<< " become leader of group " << _conf.conf<< " " << _conf.old_conf);
        // cancel candidate vote timer
        _vote_timer.stopTimer();
        _vote_ctx.reset(this);

        _state = STATE_LEADER;
        _leader_id = _server_id;

        _replicator_group.reset_term(_current_term);
        _follower_lease.reset();
        _leader_lease.on_leader_start(_current_term);

        std::set<PeerId> peers;
        _conf.list_peers(&peers);
        for (auto iter = peers.begin(); iter != peers.end(); ++iter) 
        {
            if (*iter == _server_id) 
            {
                continue;
            }

            if (_mPrx.find(iter->to_string())==_mPrx.end())
            {
                TLOGWARN_RAFT("can't find proxy:"+iter->to_string() );
                continue;
            }

            TLOGINFO_RAFT("node " << _group_id << ":" << _server_id << " term " << _current_term  << " add replicator " << *iter);
            //TODO: check return code
            _replicator_group.start_replicator(*iter);
        }

        // init commit manager
        _ballot_box->reset_pending_index(_log_manager->last_log_index() + 1);

        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.

        if (!_conf_ctx.is_busy())
        {
            
        }

        _conf_ctx.flush(_conf.conf, _conf.old_conf);
        _stepdown_timer.startTimer();
        _stepdown_timer.postRepeated(1000,1000,NodeImpl::check_dead_nodes,this);

        _pAsyncLogThread =new AsyncLogThread(10000);
        _pFsmCaller=new FSMCaller(10000);

        
    }

    void NodeImpl::check_dead_nodes(NodeImpl *node) 
    {
        std::vector<PeerId> peers;
        node->_conf.conf.list_peers(&peers);
        size_t alive_count = 0;
        Configuration dead_nodes;  // for easily print
        for (size_t i = 0; i < peers.size(); i++) 
        {
            if (peers[i] == node->_server_id) 
            {
                ++alive_count;
                continue;
            }

            if (TNOWMS - node->_replicator_group.last_rpc_send_timestamp(peers[i]) <= node->_options.election_timeout_ms) 
            {
                ++alive_count;
                continue;
            }
            dead_nodes.add_peer(peers[i]);
        }
        if (alive_count >= peers.size() / 2 + 1) 
        {
            return;
        }
        TLOGINFO_RAFT( "node " << node->node_id()
                    << " term " << node->_current_term
                    << " steps down when alive nodes don't satisfy quorum"
                        " dead_nodes: " << dead_nodes
                    << " conf: " << node->_conf.conf<<endl);
        RaftState status(ERAFTTIMEDOUT, "Majority of the group dies");
        
        node->step_down(node->_current_term, false, status);

        if (!node->_conf.old_conf.empty())
        {
            std::vector<PeerId> peers;
            node->_conf.old_conf.list_peers(&peers);
            size_t alive_count = 0;
            Configuration dead_nodes;  // for easily print
            for (size_t i = 0; i < peers.size(); i++) 
            {
                if (peers[i] == node->_server_id) 
                {
                    ++alive_count;
                    continue;
                }

                if (TNOWMS - node->_replicator_group.last_rpc_send_timestamp(peers[i]) <= node->_options.election_timeout_ms) 
                {
                    ++alive_count;
                    continue;
                }
                dead_nodes.add_peer(peers[i]);
            }
            if (alive_count >= peers.size() / 2 + 1) 
            {
                return;
            }
            TLOGINFO_RAFT( "node " << node->node_id()
                        << " term " << node->_current_term
                        << " steps down when alive nodes don't satisfy quorum"
                            " dead_nodes: " << dead_nodes
                        << " conf: " << node->_conf.old_conf<<endl);
            RaftState status(ERAFTTIMEDOUT, "Majority of the group dies");
            
            node->step_down(node->_current_term, false, status);
        }
        
    }

    void NodeImpl::grant_self(VoteBallotCtx* vote_ctx) 
    {
        // If follower lease expired, we can safely grant self. Otherwise, we wait util:
        // 1. last active leader vote the node, and we grant two votes together;
        // 2. follower lease expire.
        int64_t wait_ms = _follower_lease.votable_time_from_now();
        if (wait_ms == 0) 
        {
            vote_ctx->grant(_server_id);
            if (!vote_ctx->granted()) 
            {
                return;
            }
            //已经拿到过半数投票
            if (vote_ctx == &_pre_vote_ctx) 
            {
                //上一步为prevote，需要再vote
                elect_self();
            } 
            else 
            {
                //上一步为vote，自己可以变为主了
                become_leader();
            }
            return;
        }
        //follower lease 还未过期，起一个定时线程继续执行grant_self
        vote_ctx->start_grant_self_timer(this);
    }


    void NodeImpl::pre_vote() 
    {
        TLOGINFO_RAFT( "node " << _group_id << ":" << _server_id << " term " << _current_term << " start pre_vote"<<endl);
        if (_snapshot_executor && _snapshot_executor->is_installing_snapshot()) 
        {
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id<< " term " << _current_term
                        << " doesn't do pre_vote when installing snapshot as the   configuration is possibly out of date"<<endl);
            return;
        }

        if (!_conf.contains(_server_id)) 
        {
            TLOGWARN_RAFT( "node " << _group_id << ':' << _server_id
                        << " can't do pre_vote as it is not in " << _conf.conf<<endl);
            return;
        }

        int64_t old_term = _current_term;
        LogId last_log_id;

        {
            //todo lock?
            std::unique_lock<std::mutex> lck(_mutex);
            last_log_id = _log_manager->last_log_id(true);

        }

        if (old_term != _current_term) 
        {
            TLOGWARN_RAFT("node " << _group_id << ":" << _server_id << " raise term " << _current_term << " when get last_log_id"<<endl) ;
            return;
        }

        _pre_vote_ctx.init(this);
        std::set<PeerId> peers;
        _conf.list_peers(&peers);

        for (auto iter = peers.begin(); iter != peers.end(); ++iter) 
        {
            if (*iter == _server_id) 
            {
                continue;
            }

            if (_mPrx.find(iter->to_string())==_mPrx.end())
            {
                continue;
            }
            
            auto &proxy=_mPrx[iter->to_string()];

            horsedb::RequestVoteReq tRequestVoteReq;
            RaftDBPrxCallbackPtr callback(new RaftDBCallback());

            tRequestVoteReq.groupID=_group_id;
            tRequestVoteReq.serverID=_server_id.to_string();
            tRequestVoteReq.peerID=iter->to_string();
            tRequestVoteReq.term=_current_term + 1;
            tRequestVoteReq.lastLogIndex=last_log_id.index;
            tRequestVoteReq.lastLogTerm=last_log_id.term;

            proxy->async_preVote(callback,tRequestVoteReq);
            
        }

        grant_self(&_pre_vote_ctx);


    }

    void NodeImpl::VoteBallotCtx::init(NodeImpl* node)
     {
        ++_version;
        _ballot.init(node->_conf.conf, node->_conf.stable() ? NULL : &(node->_conf.old_conf));
        //stop_grant_self_timer(node);
    }

    void NodeImpl::handle_election_timeout( NodeImpl* node)
    {
        std::unique_lock<std::mutex> lck(node->_mutex);

        // check state
        if (node->_state != STATE_FOLLOWER) 
        {
            return;
        }

        // Trigger vote manually, or wait until follower lease expire.
        //follwer 判断 Leader心跳是否超时
        if (!node->_vote_triggered && !node->_follower_lease.expired()) 
        {

            return;
        }
        node->_vote_triggered = false;

        // Reset leader as the leader is uncerntain on election timeout.
        PeerId empty_id;
        RaftState status(ERAFTTIMEDOUT, "Lost connection from leader "+node->_leader_id.to_string());
        node->reset_leader_id(empty_id, status);

        //已经超时了,重新发起选举
         node->pre_vote();

         return;

    }

    int NodeImpl::handle_pre_vote_request(const RequestVoteReq& request, RequestVoteRes& response) 
    {
        std::unique_lock<std::mutex> lck(_mutex);

        if (!is_active_state(_state)) 
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            TLOGWARN_RAFT(  "node " << _group_id << ":" << _server_id 
                        << " is not in active state " << "current_term " 
                        << saved_current_term
                        << " state " << state2str(saved_state)<<endl );
            return -1;
        }

        PeerId candidate_id;
        if (0 != candidate_id.parse(request.serverID )) 
        {
            TLOGWARN_RAFT("node " << _group_id << ":" << _server_id
                        << " received PreVote from " << request.serverID
                        << " server_id bad format"<<endl);
            return -1;
        }

        bool granted = false;
        do {
            int64_t votable_time = _follower_lease.votable_time_from_now();
            if (request.term < _current_term || votable_time > 0) 
            {
                // ignore older term
                TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " ignore PreVote from " << request.serverID
                        << " in term " << request.term
                        << " current_term " << _current_term
                        << " votable_time_from_now " << votable_time<<endl);
                break;
            }

            // get last_log_id outof node mutex
            lck.unlock();
            LogId last_log_id = _log_manager->last_log_id(true);
            lck.lock();
            // pre_vote not need ABA check after unlock&lock

            granted = LogId(request.lastLogIndex, request.lastLogTerm)>= last_log_id?true:false;

            TLOGWARN_RAFT(  "node " << _group_id << ":" << _server_id
                    << " received PreVote from " << request.serverID
                    << " in term " << request.term
                    << " current_term " << _current_term
                    << " granted " << granted<<endl);

        } while (0);

        response.term= _current_term;
        response.isVoteGranted=granted;
        return 0;
    }

    int NodeImpl::handle_request_vote_request(const RequestVoteReq& request, RequestVoteRes& response) 
    {
        std::unique_lock<std::mutex> lck(_mutex);

        if (!is_active_state(_state)) 
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            lck.unlock();
            TLOGWARN_RAFT(  "node " << _group_id << ":" << _server_id 
                        << " is not in active state " << "current_term " 
                        << saved_current_term
                        << " state " << state2str(saved_state) );
            return -1;
        }

        PeerId candidate_id;
        if (0 != candidate_id.parse(request.serverID )) 
        {
            TLOGWARN_RAFT("node " << _group_id << ":" << _server_id
                        << " received PreVote from " << request.serverID
                        << " server_id bad format"<<endl);
            return -1;
        }


        do {
            int64_t votable_time = _follower_lease.votable_time_from_now();
            if (request.term > _current_term && votable_time == 0) 
            {
                TLOGINFO_RAFT( "node " << _group_id << ":" << _server_id
                      << " received RequestVote from " << request.serverID
                      << " in term " << request.term
                      << " current_term " << _current_term<<endl);
                // incress current term, change state to follower
                if (request.term > _current_term) 
                {
                    RaftState status(EHIGHERTERMREQUEST, "Raft node receives higher term request_vote_request.");
                    step_down(request.term, false, status);//如果为候选人则降为follower，并更新自己的任期
                }
            }
            else
            {
                // ignore older term
                TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " ignore PreVote from " << request.serverID
                        << " in term " << request.term
                        << " current_term " << _current_term
                        << " votable_time_from_now " << votable_time<<endl);
                break;
            }
            

            // get last_log_id outof node mutex
            lck.unlock();
            LogId last_log_id = _log_manager->last_log_id(true);
            lck.lock();
            // vote need ABA check after unlock&lock
            if (request.term != _current_term) 
            {
                TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id << " raise term " << _current_term << " when get last_log_id");
                break;
            }

            bool log_is_ok = LogId(request.lastLogIndex, request.term)>= last_log_id?true : false;
            // save
            if (log_is_ok && _voted_id.is_empty()) 
            {
                RaftState status(EVOTEFORCANDIDATE, "Raft node votes for some candidate, step down to restart election_timer.");
                step_down(request.term, false, status);
                _voted_id = candidate_id;
                

                if (_log_storage->set_term_and_votedfor(_current_term, candidate_id, _v_group_id)!=0)
                {
                    TLOGERROR_RAFT( "node " << _group_id << ":" << _server_id
                        << " fail to set_term_and_votedfor ,_current_term="<<_current_term<<endl)
                        _voted_id.reset(); 
                }
            }

        } while (0);

        bool granted= (request.term== _current_term && _voted_id == candidate_id) ?true : false;

        response.term= _current_term;
        response.isVoteGranted=granted;
        return 0;
    }


    void NodeImpl::check_step_down(const int64_t request_term, const PeerId& server_id) 
    {
        if (request_term > _current_term) 
        {
            RaftState status(ENEWLEADER, "Raft node receives message from "
                    "new leader with higher term."); 
            step_down(request_term, false, status);
        } 
        else if (_state != STATE_FOLLOWER)
        { 
            RaftState status(ENEWLEADER, "Candidate receives message "
                    "from new leader with the same term.");
            step_down(request_term, false, status);
        }
        else if (_leader_id.is_empty()) 
        {
            RaftState status(ENEWLEADER, "Follower receives message "
                    "from new leader with the same term.");
            step_down(request_term, false, status); 
        }
        // save current leader
        if (_leader_id.is_empty()) 
        { 
            reset_leader_id(server_id, RaftState());
        }
    }

    int NodeImpl::increase_term_to(int64_t new_term, const RaftState& status) 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (new_term <= _current_term) 
        {
            return EINVAL;
        }
        step_down(new_term, false, status);
        return 0;
    }

    //follower 收到 leader 的 AppendEntriesReq
    int NodeImpl::handle_append_entries_request(const AppendEntriesReq& request, AppendEntriesRes& response) 
    {
        std::unique_lock<std::mutex> lck(_mutex);

        response.term=_current_term;

        if (!is_active_state(_state)) 
        {
            const int64_t saved_current_term = _current_term;
            const State saved_state = _state;
            
            TLOGWARN_RAFT(  "node " << _group_id << ":" << _server_id 
                        << " is not in active state " << "current_term " << saved_current_term 
                        << " state " << state2str(saved_state) <<endl);
            
            return -1;
        }

        PeerId server_id;
        if (0 != server_id.parse(request.serverID)) 
        {
            
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " received AppendEntries from " << request.serverID
                        << " server_id bad format"<<endl);
            
            return -1;
        }

        // check stale term
        if (request.term < _current_term) 
        {
            const int64_t saved_current_term = _current_term;
            
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " ignore stale AppendEntries from " << request.serverID
                        << " in term " << request.term
                        << " current_term " << saved_current_term);
            response.isSuccess=false;
            response.term= saved_current_term;
            return -1;
        }

        //request.term >= _current_term
        //正常情况下,选举结束后,大家任期一样,如果request.term > 本地的任期，需要更新本地任期
        check_step_down(request.term, server_id);   
     
        if (server_id != _leader_id) 
        {
            TLOGWARN_RAFT(  "Another peer " << _group_id << ":" << server_id
                    << " declares that it is the leader at term=" << _current_term 
                    << " which was occupied by leader=" << _leader_id<<endl);
            // Increase the term by 1 and make both leaders step down to minimize the
            // loss of split brain
            RaftState status(ELEADERCONFLICT, "More than one leader in the same term."); 
            step_down(request.term + 1, false, status);
            response.isSuccess=false;
            response.term= request.term + 1;//让leader 降为follwer ,大家进入下一次选举
            return -1;
        }

        _follower_lease.renew(_leader_id);//follower 更新 leader 心跳时间 

        if (request.logEntries.size() > 0 &&(_snapshot_executor&& _snapshot_executor->is_installing_snapshot())) 
        {
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " received append entries while installing snapshot"<<endl);
            
            return -1;
        }

        const int64_t prev_log_index = request.prevLogIndex;
        const int64_t prev_log_term = request.prevLogTerm;
        const int64_t local_prev_log_term = _log_manager->get_term(prev_log_index);
        if (local_prev_log_term != prev_log_term) 
        {
            int64_t last_index = _log_manager->last_log_index();
            //int64_t saved_term = request.term;
            //int     saved_entries_size = request.logEntries.size();
            
            
            response.isSuccess=false ;
            response.term= _current_term;
            response.lastLogIndex =last_index;
            
            TLOGWARN_RAFT( "node " << _group_id << ":" << _server_id
                        << " reject term_unmatched AppendEntries from " 
                        << request.serverID
                        << " in term " << request.term
                        << " prev_log_index " << request.prevLogIndex
                        << " prev_log_term " << request.prevLogTerm
                        << " local_prev_log_term " << local_prev_log_term
                        << " last_log_index " << last_index
                        << " entries_size " << request.logEntries.size()<<endl)
            return -1;
        }

        if (request.logEntries.size() == 0) 
        {
            response.isSuccess=true;
            response.term=_current_term;
            response.lastLogIndex=_log_manager->last_log_index();
            //response->set_readonly(_node_readonly);

            // see the comments at FollowerStableClosure::run()
            _ballot_box->set_last_committed_index( std::min((int64_t)request.commitIndex,  prev_log_index));
            //request.commitIndex 发送方(leader)投票箱最后一个过半数的index
            //request.prevLogIndex 发送方已经落地的last_log_index
            return 0;
        }


        auto logEntries =request.logEntries;
        _log_manager->append_entries(logEntries);

        // update configuration after _log_manager updated its memory status
        _log_manager->check_and_set_configuration(&_conf);

        response.isSuccess=true;
        response.term= _current_term;

        const int64_t committed_index =
                std::min((int64_t)request.commitIndex  ,//leader 最后过半数 的 index
                         // ^^^ committed_index is likely less than the
                         // last_log_index
                         (int64_t)(request.prevLogIndex + request.logEntries.size())
                         // ^^^ The logs after the appended entries are
                         // untrustable so we can't commit them even if their
                         // indexes are less than request->committed_index()
                        );
        //_ballot_box is thread safe and tolerates disorder.
        _ballot_box->set_last_committed_index(committed_index);

        return 0;


    }


}
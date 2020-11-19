#include "raft/Replicator.h"
#include "raft/RaftDB.h"
#include "logger/logger.h"
#include "util/tc_timeprovider.h"
#include "raft/LogManager.h"
#include "raft/RaftDBCallback.h"
#include "raft/Node.h"
namespace horsedb{


    ReplicatorGroup::ReplicatorGroup() 
        : _dynamic_timeout_ms(-1)
        , _election_timeout_ms(-1)
    {
        _common_options.dynamic_heartbeat_timeout_ms = _dynamic_timeout_ms;
        _common_options.election_timeout_ms = _election_timeout_ms;
    }

    ReplicatorGroup::~ReplicatorGroup() 
    {
        stop_all();
    }

    int ReplicatorGroup::stop_all()
    {
        for (auto &item :_rMap)
        {
            item.second->terminate();
            delete item.second;
        }

        return 0;
    }

    int ReplicatorGroup::init(const NodeId& node_id, const ReplicatorGroupOptions& options) 
    {
        _dynamic_timeout_ms = options.heartbeat_timeout_ms;
        _election_timeout_ms = options.election_timeout_ms;
        _common_options.log_manager = options.log_manager;
        _common_options.ballot_box = options.ballot_box;
        _common_options.node = options.node;
        _common_options.term = 0;
        _common_options.group_id = node_id._group_id;
        _common_options.server_id = node_id._peer_id;
        //_common_options.snapshot_storage = options.snapshot_storage;
        //_common_options.snapshot_throttle = options.snapshot_throttle;
        return 0;
    }

    int ReplicatorGroup::start_replicator(const PeerId& peer) 
    {
        TLOGWARN_RAFT( "_common_options.term " <<_common_options.term<<endl );
        if (0!= _common_options.term)
        {
            TLOGWARN_RAFT( "0!= _common_options.term "<<endl );
        }
        
        if (_rMap.find(peer) != _rMap.end()) 
        {
            return 0;
        }
        ReplicatorOptions options = _common_options;
        options.peer_id = peer;

        Replicator *replicator= new Replicator(10000);
        
        replicator->init(options);

        _rMap[peer] = replicator;

        replicator->_update_last_rpc_send_timestamp(TNOWMS);

        replicator->sendEmptyEntries(false);

        return 0;
    }

    Replicator* ReplicatorGroup::get_replicator(const PeerId& peer) 
    {

        if (_rMap.find(peer) == _rMap.end()) 
        {
            return NULL;
        }
        

        return _rMap[peer];
    }

    Replicator::Replicator(size_t iQueueCap)
    : _terminate(false), _iQueueCap(iQueueCap)
    {
        _replicLogTaskQueue = new TC_CasQueue<ReplicLogTask>();

        
        start();
        
    }

    void Replicator::terminate()
    {
        
            TC_ThreadLock::Lock lock(*this);

            _terminate = true;

            notifyAll();
        
    }

    Replicator::~Replicator()
    {
        terminate();

        if(_replicLogTaskQueue)
        {
            delete _replicLogTaskQueue;
            _replicLogTaskQueue = NULL;
        }
    }



    void Replicator::init(const ReplicatorOptions &options)
    {
        _options = options;
        _next_index = _options.log_manager->last_log_index() + 1;

        _last_rpc_send_timestamp = TNOWMS;

    }

    void Replicator::push_back(ReplicLogTask &task)
    {
        if(_replicLogTaskQueue->size() >= _iQueueCap)
        {
            TLOGERROR_RAFT("[Replicator::push_back] async_queue full:" << _replicLogTaskQueue->size() << ">=" << _iQueueCap << endl);
            //delete msg;
        }
        else
        {
            _replicLogTaskQueue->push_back(task);

            TC_ThreadLock::Lock lock(*this);
            
            notify();
        }

    }

    void Replicator::run()
    {
        //客户端请求来了,收到AsyncLogThread的通知马上发送日志
        //远程节点响应回来了,收到AsyncProcThread的通知马上发送日志
        //定时检测  log_manager._last_log_index 看是否有新的日志要发送,有则继续发送日志,没有则等待超时发送心跳
        while (!_terminate)
        {
            ReplicLogTask task;

            if (_replicLogTaskQueue->pop_front(task))
            {
                process(task);
            }
            else
            {
                
                TC_ThreadLock::Lock lock(*this);
                timedWait(200);//todo 心跳时间间隔		
                sendEmptyEntries(true);
            }
        }
        

    }

    void Replicator::process(ReplicLogTask &task)
    {
        if (task._ReplicType==REPLIC_DATA)
        {
            sendEntries();
        }
        else if (task._ReplicType==REPLIC_EMPTY)
        {
            sendEmptyEntries(false);
        }
        else if (task._ReplicType==REPLIC_REQDATA)
        {
            sendEntries(task._batchLog);
        }
        
 
    }



    void Replicator::sendEmptyEntries(bool is_heartbeat)
    {
        try
        {
            int64_t prev_log_index=_next_index-1;
            const int64_t prev_log_term = _options.log_manager->get_term(prev_log_index);
            if (prev_log_term == 0 &&  prev_log_index!= 0) 
            {
                if (!is_heartbeat) 
                {
                    if (prev_log_index < _options.log_manager->first_log_index())
                    {
                        TLOGWARN_RAFT(  "Group " << _options.group_id<< " prev_log_index=" << prev_log_index <<",first_log_index= " <<_options.log_manager->first_log_index()<<endl);
                    }
                    
                    TLOGWARN_RAFT(  "Group " << _options.group_id<< " log_index=" << prev_log_index << " was compacted"<<endl);

                    //todo snapshot
                    return _install_snapshot() ;
                } 
                else 
                {
                    // The log at prev_log_index has been compacted, which indicates 
                    // we is or is going to install snapshot to the follower. So we let 
                    // both prev_log_index and prev_log_term be 0 in the heartbeat 
                    // request so that follower would do nothing besides updating its 
                    // leader timestamp.
                    prev_log_index = 0;
                }
            }

            AppendEntriesReq tReq;
            tReq.term=_options.term;
            tReq.groupID=_options.group_id;
            tReq.serverID=_options.server_id.to_string();
            tReq.peerID=_options.peer_id.to_string();//发给谁

            tReq.prevLogIndex=prev_log_index;
            tReq.prevLogTerm=prev_log_term;
            tReq.commitIndex=_options.ballot_box->last_committed_index();

            if (is_heartbeat) 
            {
                _heartbeat_counter++;

            }
            else
            {
                _st.type = APPENDING_ENTRIES;
                _st.first_log_index = _next_index;
                _st.last_log_index = _next_index - 1;//?
                TLOGINFO_RAFT(  "_append_entries_in_fly.size()=" << _append_entries_in_fly.size() <<endl);
                TLOGINFO_RAFT(  "_flying_append_entries_size=" << _flying_append_entries_size <<endl);

                _append_entries_in_fly.push_back(FlyingAppendEntriesRpc(_next_index, 0));
                _append_entries_counter++;
            }

            TLOGINFO_RAFT(  "node " << _options.group_id << ":" << _options.server_id << " send HeartbeatRequest to " << _options.peer_id 
            << " term " << _options.term<< " prev_log_index " << tReq.prevLogIndex<< " last_committed_index " << tReq.commitIndex<<endl);

            auto &mPrx=ReplicatorGroup::getInstance()->getAllProxy();
            auto it=mPrx.find(tReq.peerID);
            if (it==mPrx.end())
            {
                TLOGERROR_RAFT("can't find proxy:"+tReq.peerID<<endl );
                return ;
            }
            
            auto &proxy=it->second;

            RaftDBPrxCallbackPtr callback(new RaftDBCallback());


            proxy->async_appendEntries(callback,tReq);
            return ;


           
        }
        catch(const std::exception& e)
        {
            
            TLOGERROR_RAFT("exception:"+string(e.what())<<endl );
        }

        
    }

    void Replicator::_cancel_append_entries_rpcs() 
    {
        _append_entries_in_fly.clear();
    }

    void Replicator::_reset_next_index() 
    {
        _next_index -= _flying_append_entries_size;
        _flying_append_entries_size = 0;
        _cancel_append_entries_rpcs();
        _is_waiter_canceled = true;
        // if (_wait_id != 0) {
        //     _options.log_manager->remove_waiter(_wait_id);
        //     _wait_id = 0;
        // }
    }

    void Replicator::_install_snapshot()
    {
        
    }

    void Replicator::sendEntries()
    {

        try
        {
            bool is_heartbeat=false;
            if (_flying_append_entries_size >= raft_max_entries_size 
            ||_append_entries_in_fly.size() >= (size_t)raft_max_parallel_append_entries_rpc_num 
            || _st.type == BLOCKING)
            {
                TLOGERROR_RAFT( "node " << _options.group_id << ":" << _options.server_id
                    << " skip sending AppendEntriesRequest to " << _options.peer_id
                    << ", too many requests in flying, or the replicator is in block,"
                    << " next_index " << _next_index << " flying_size " << _flying_append_entries_size
                    << " _append_entries_in_fly.size() " << _append_entries_in_fly.size()<<endl);
                
                return;
            }

            int64_t prev_log_index=_next_index-1;
            const int64_t prev_log_term = _options.log_manager->get_term(prev_log_index);
            if (prev_log_term == 0 &&  prev_log_index!= 0) 
            {
                if (!is_heartbeat) 
                {
                    if (prev_log_index < _options.log_manager->first_log_index())
                    {
                        TLOGWARN_RAFT(  "Group " << _options.group_id<< " prev_log_index=" << prev_log_index <<",first_log_index= " <<_options.log_manager->first_log_index()<<endl);
                    }
                    
                    TLOGWARN_RAFT(  "Group " << _options.group_id<< " log_index=" << prev_log_index << " was compacted"<<endl);

                    //todo snapshot
                    _reset_next_index();
                    return _install_snapshot() ;
                } 
                else 
                {
                    // The log at prev_log_index has been compacted, which indicates 
                    // we is or is going to install snapshot to the follower. So we let 
                    // both prev_log_index and prev_log_term be 0 in the heartbeat 
                    // request so that follower would do nothing besides updating its 
                    // leader timestamp.
                    prev_log_index = 0;
                }
            }

            AppendEntriesReq tReq;
            tReq.term=_options.term;
            tReq.groupID=_options.group_id;
            tReq.serverID=_options.server_id.to_string();
            tReq.peerID=_options.peer_id.to_string();//发给谁

            tReq.prevLogIndex=prev_log_index;//目前落地日志的最后一个日志的index
            tReq.prevLogTerm=prev_log_term;//目前落地日志的最后一个日志的term
            tReq.commitIndex=_options.ballot_box->last_committed_index();

            const int max_entries_size = raft_max_entries_size - _flying_append_entries_size;
            int prepare_entry_rc = 0;
            LogEntry tLogEntry;
            for (int i = 0; i < max_entries_size; ++i) 
            {
                prepare_entry_rc = _prepare_entry(i, tLogEntry);
                if (prepare_entry_rc != 0) 
                {
                    break;
                }
                tReq.logEntries.push_back(tLogEntry);
            }

            if (tReq.logEntries.size() == 0) 
            {
                if (_next_index < _options.log_manager->first_log_index()) 
                {
                    _reset_next_index();
                    return _install_snapshot();
                }
                // NOTICE: a follower's readonly mode does not prevent install_snapshot
                // as we need followers to commit conf log(like add_node) when 
                // leader reaches readonly as well 
                if (prepare_entry_rc == EREADONLY) 
                {
                    if (_flying_append_entries_size == 0) 
                    {
                        _st.type = IDLE;
                    }

                }
                return ;
            }

            _append_entries_in_fly.push_back(FlyingAppendEntriesRpc(_next_index,tReq.logEntries.size()));
            _append_entries_counter++;
            _next_index += tReq.logEntries.size();
            _flying_append_entries_size += tReq.logEntries.size();

            TLOGINFO_RAFT( "node " << _options.group_id << ":" << _options.server_id
            << " send AppendEntriesRequest to " << _options.peer_id << " term " << _options.term
            << " last_committed_index " << tReq.commitIndex
            << " prev_log_index " <<tReq.prevLogIndex
            << " prev_log_term " << tReq.prevLogTerm
            << " next_index " << _next_index << " count " << tReq.logEntries.size() <<endl);
            _st.type = APPENDING_ENTRIES;
            _st.first_log_index = _next_index - _flying_append_entries_size;
            _st.last_log_index = _next_index - 1;

            auto &mPrx=ReplicatorGroup::getInstance()->getAllProxy();
            auto it=mPrx.find(tReq.peerID);
            if (it==mPrx.end())
            {
                TLOGERROR_RAFT("can't find proxy:"+tReq.peerID<<endl );
                return ;
            }
            
            auto &proxy=it->second;

            RaftDBPrxCallbackPtr callback(new RaftDBCallback());


            proxy->async_appendEntries(callback,tReq);



        }
        catch(const std::exception& e)
        {
            TLOGERROR_RAFT("exception:"+string(e.what())<<endl );
        }
        

    }


    void Replicator::sendEntries(const vector<LogEntry> &batchLog)
    {

        try
        {
            bool is_heartbeat=false;
            if (_flying_append_entries_size >= raft_max_entries_size 
            ||_append_entries_in_fly.size() >= (size_t)raft_max_parallel_append_entries_rpc_num 
            || _st.type == BLOCKING)
            {
                TLOGERROR_RAFT( "node " << _options.group_id << ":" << _options.server_id
                    << " skip sending AppendEntriesRequest to " << _options.peer_id
                    << ", too many requests in flying, or the replicator is in block,"
                    << " next_index " << _next_index << " flying_size " << _flying_append_entries_size
                    << " _append_entries_in_fly.size() " << _append_entries_in_fly.size()<<endl);
                
                return;
            }

            int64_t prev_log_index=_next_index-1;
            const int64_t prev_log_term = _options.log_manager->get_term(prev_log_index);
            if (prev_log_term == 0 &&  prev_log_index!= 0) 
            {
                if (!is_heartbeat) 
                {
                    if (prev_log_index < _options.log_manager->first_log_index())
                    {
                        TLOGWARN_RAFT(  "Group " << _options.group_id<< " prev_log_index=" << prev_log_index <<",first_log_index= " <<_options.log_manager->first_log_index()<<endl);
                    }
                    
                    TLOGWARN_RAFT(  "Group " << _options.group_id<< " log_index=" << prev_log_index << " was compacted"<<endl);

                    //todo snapshot
                    _reset_next_index();
                    return _install_snapshot() ;
                } 
                else 
                {
                    // The log at prev_log_index has been compacted, which indicates 
                    // we is or is going to install snapshot to the follower. So we let 
                    // both prev_log_index and prev_log_term be 0 in the heartbeat 
                    // request so that follower would do nothing besides updating its 
                    // leader timestamp.
                    prev_log_index = 0;
                }
            }

            AppendEntriesReq tReq;
            tReq.term=_options.term;
            tReq.groupID=_options.group_id;
            tReq.serverID=_options.server_id.to_string();
            tReq.peerID=_options.peer_id.to_string();//发给谁

            tReq.prevLogIndex=prev_log_index;//目前落地日志的最后一个日志的index
            tReq.prevLogTerm=prev_log_term;//目前落地日志的最后一个日志的term
            tReq.commitIndex=_options.ballot_box->last_committed_index();

            const int max_entries_size = raft_max_entries_size - _flying_append_entries_size;
            //int prepare_entry_rc = 0;
            LogEntry tLogEntry;
            
            if(batchLog.size()>(uint32_t)max_entries_size)
            {
                TLOGERROR_RAFT("max_entries_size:" << max_entries_size << ",_batchLog.size()=" << batchLog.size() << endl);
                return ;
            }
            else
            {
                tReq.logEntries=std::move(batchLog) ;
            }
        

            

            _append_entries_in_fly.push_back(FlyingAppendEntriesRpc(_next_index,tReq.logEntries.size()));
            _append_entries_counter++;
            _next_index += tReq.logEntries.size();
            _flying_append_entries_size += tReq.logEntries.size();

            TLOGINFO_RAFT( "node " << _options.group_id << ":" << _options.server_id
            << " send AppendEntriesRequest to " << _options.peer_id << " term " << _options.term
            << " last_committed_index " << tReq.commitIndex
            << " prev_log_index " <<tReq.prevLogIndex
            << " prev_log_term " << tReq.prevLogTerm
            << " next_index " << _next_index << " count " << tReq.logEntries.size() <<endl);
            _st.type = APPENDING_ENTRIES;
            _st.first_log_index = _next_index - _flying_append_entries_size;
            _st.last_log_index = _next_index - 1;

            auto &mPrx=ReplicatorGroup::getInstance()->getAllProxy();
            auto it=mPrx.find(tReq.peerID);
            if (it==mPrx.end())
            {
                TLOGERROR_RAFT("can't find proxy:"+tReq.peerID<<endl );
                return ;
            }
            
            auto &proxy=it->second;

            RaftDBPrxCallbackPtr callback(new RaftDBCallback());


            proxy->async_appendEntries(callback,tReq);



        }
        catch(const std::exception& e)
        {
            TLOGERROR_RAFT("exception:"+string(e.what())<<endl );
        }
        

    }

    int Replicator::send_timeout_now_and_stop(Replicator* r) 
    {

        r->_send_timeout_now();
        return 0;
    }

    void Replicator::_send_timeout_now() 
    {
        TimeoutNowReq request;
        request.term=_options.term;
        request.groupID=_options.group_id;
        request.serverID=_options.server_id.to_string();
        request.peerID=_options.peer_id.to_string();


        _timeout_now_index = 0;

        auto &mPrx=ReplicatorGroup::getInstance()->getAllProxy();
        auto it=mPrx.find(request.peerID);
        if (it==mPrx.end())
        {
            TLOGERROR_RAFT("can't find proxy:"+request.peerID<<endl );
            return ;
        }
        
        auto &proxy=it->second;

        RaftDBPrxCallbackPtr callback(new RaftDBCallback());


        proxy->async_timeoutNow(callback,request);
    
}


    int Replicator::_prepare_entry(int offset, LogEntry &tLogEntry) 
    {
        try
        {
            const int64_t log_index = _next_index + offset;
            bool bFound = _options.log_manager->get_entry(log_index,tLogEntry);
            if (!bFound) 
            {
                return ENOENT;
            }

            // When leader become readonly, no new user logs can submit. On the other side,
            // if any user log are accepted after this replicator become readonly, the leader
            // still have enough followers to commit logs, we can safely stop waiting new logs
            // until the replicator leave readonly mode.
            if (_readonly_index != 0 && log_index >= _readonly_index) 
            {
                if (tLogEntry.cmdType != CM_Config) 
                {
                    return EREADONLY;
                }
                _readonly_index = log_index + 1;
            }

            return 0;
        }
        catch(const std::exception& e)
        {
            TLOGERROR_RAFT("exception:"+string(e.what())<<endl );
        }

        
        return -1;
            

    }
}
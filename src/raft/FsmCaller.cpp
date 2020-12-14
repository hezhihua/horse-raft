#include "raft/FsmCaller.h"
#include "logger/logger.h"
#include "raft/Node.h"
#include "raft/Snapshot.h"
namespace horsedb{

FSMCaller::FSMCaller(size_t iQueueCap)
: _terminate(false), _iQueueCap(iQueueCap)
{
	 _ApplyTaskQueue = new TC_CasQueue<ApplyTask>();
	start();   
}

FSMCaller::~FSMCaller()
{
    terminate();

    if(_ApplyTaskQueue)
    {
        delete _ApplyTaskQueue;
        _ApplyTaskQueue = NULL;
    }
}

void FSMCaller::terminate()
{
	
        TC_ThreadLock::Lock lock(*this);

        _terminate = true;

        notifyAll();
    
}
int FSMCaller::init(const FSMCallerOptions &options) 
{
    if (options.log_manager == NULL || options.fsm == NULL ) {
        return EINVAL;
    }
    _log_manager = options.log_manager;
    _fsm = options.fsm;

    _node = options.node;
    _last_applied_index.store(options.bootstrap_id.index,std::memory_order_relaxed);
    _last_applied_term = options.bootstrap_id.term;
    
    return 0;
}
void FSMCaller::push_back(ApplyTask &task)
{
	
		if(_ApplyTaskQueue->size() >= _iQueueCap)
		{
			TLOGERROR_RAFT("[FsmCaller::push_back] async_queue full:" << _ApplyTaskQueue->size() << ">=" << _iQueueCap << endl);
		}
		else
		{
			_ApplyTaskQueue->push_back(task);

			TC_ThreadLock::Lock lock(*this);
            _last_time_processed=TNOWMS;
			notify();
		}
	
}


void FSMCaller::run()
{
    while (!_terminate)
    {

        if(_ApplyTaskQueue->size()==20)
        {
            batchProcess();
        }
		else
		{
			time2process();
			TC_ThreadLock::Lock lock(*this);
	        timedWait(7000);	//300	
		}

    }
}


void FSMCaller::batchProcess()
{
    deque<ApplyTask> batchTask;
    _ApplyTaskQueue->swap(batchTask);
    int64_t max_committed_index = -1; 
    for(auto &task: batchTask)
    {
        if (task.type==COMMITTED)
        {
            if (task.committed_index > max_committed_index) 
            {
                max_committed_index = task.committed_index;
            }
        }
        else
        {
            if (max_committed_index >= 0)
            {
                _cur_task = COMMITTED;
                do_committed(max_committed_index);
                max_committed_index = -1;
            }
            
            switch (task.type){

                case LEADER_START:               
                break;

                case   LEADER_STOP:

                break;

                case   START_FOLLOWING:

                break;
                case   SNAPSHOT_SAVE:
                do_snapshot_save();

                break;

            }

        }
        
    }
    if (max_committed_index >= 0)
    {
        _cur_task = COMMITTED;
        do_committed(max_committed_index);
        max_committed_index = -1;
    }

    _cur_task = IDLE;


}
void FSMCaller::time2process()
{
	TLOGINFO_RAFT("[FsmCaller::time2process] time2process." << endl);

	try
	{
        //比如收到批量20个包消耗的时间为50毫秒,这个数值要小于50毫秒
		if (TNOWMS -_last_time_processed > 30)
		{
			batchProcess();
		}
		
	}
	catch (exception& e)
	{
		TLOGERROR_RAFT("[FsmCaller exception]:" << e.what() << endl);
	}
	catch (...)
	{
		TLOGERROR_RAFT("[FsmCaller exception.]" << endl);
	}
}

//得到大多数投票后执行
int FSMCaller::on_committed(int64_t committed_index)
{
    ApplyTask task;
    task.type=COMMITTED;
    task.committed_index=committed_index;

    push_back(task);

    return 0;

}
int FSMCaller::on_leader_start(int64_t term, int64_t lease_epoch)
{
    ApplyTask task;
    task.type=LEADER_START;
    task.leader_start_context = new LeaderStartContext(term, lease_epoch) ;

    push_back(task);

    return 0;
}

int FSMCaller::on_leader_stop(const RaftState& status) 
{
    ApplyTask task;
    task.type = LEADER_STOP;
    push_back(task);
    return 0;
}

int FSMCaller::on_start_following(const LeaderChangeContext& start_following_context) 
{
    ApplyTask task;
    task.type = START_FOLLOWING;
    LeaderChangeContext* context  = new LeaderChangeContext(start_following_context.leader_id(), start_following_context.term(), start_following_context.status());
    task.leader_change_context = context;
    push_back(task);
 
    return 0;
}

int FSMCaller::on_stop_following(const LeaderChangeContext& stop_following_context) 
{
    ApplyTask task;
    task.type = STOP_FOLLOWING;
    LeaderChangeContext* context = new LeaderChangeContext(stop_following_context.leader_id(), 
            stop_following_context.term(), stop_following_context.status());
    task.leader_change_context = context;

    push_back(task);
   
    return 0;
}


//leader和follwer都可能会执行此函数
void FSMCaller::do_committed(int64_t committed_index)
{
    int64_t last_applied_index = _last_applied_index.load(std::memory_order_relaxed);

    // We can tolerate the disorder of committed_index
    if (last_applied_index >= committed_index) 
    {
        return;
    }

    //批量应用到状态机,即进行业务处理,批量响应客户端
    std::vector<ClientContext> out;
    int64_t out_first_index;
    _ballot_box->pop_until(committed_index, out, &out_first_index);

    IteratorImpl iter_impl(_fsm, _log_manager, out, out_first_index,last_applied_index, committed_index, &_applying_index);
    for (; iter_impl.is_good();) 
    {
        if (iter_impl.entry().cmdType == CM_Config) 
        {
            
            if (iter_impl.entry().vOldPeer.empty() ) 
            {
                // Joint stage is not supposed to be noticeable by end users.
                std::vector<PeerId> peers;
                for (size_t i = 0; i < iter_impl.entry().vPeer.size(); i++)
                {
                    peers.push_back(iter_impl.entry().vPeer[i]);
                }
                
                _fsm->on_configuration_committed( Configuration(peers),iter_impl.entry().index);
            }
            
            // For other entries, we have nothing to do besides flush the
            // pending tasks and run this closure to notify the caller that the
            // entries before this one were successfully committed and applied.
            if (iter_impl.done()) 
            {
                
                    _node->on_configuration_change_done(_node->_current_term);
                    if (_node->_leader_start) //todo if leader start
                    {
                        _node->leader_lease_start(_node->_leader_lease.lease_epoch());
                        _node->_options.fsm->on_leader_start(_node->_current_term);
                    }
                
            }
            iter_impl.next();
            continue;
        }
        Iterator iter(&iter_impl);
        _fsm->on_apply(iter);
        
        // Try move to next in case that we pass the same log twice.
        iter.next();
    }


}


int FSMCaller::on_snapshot_save() 
{
    ApplyTask task;
    task.type = SNAPSHOT_SAVE;

    push_back(task);

    return 0;
}


void FSMCaller::do_snapshot_save() 
{

    int64_t last_applied_index = _last_applied_index.load(std::memory_order_relaxed);

    SnapshotMeta meta;
    meta.lastIncludedIndex=last_applied_index;
    meta.lastIncludedTerm=_last_applied_term;
    ConfigurationEntry conf_entry;
    _log_manager->get_configuration(last_applied_index, &conf_entry);
    for (Configuration::const_iterator iter = conf_entry.conf.begin(); iter != conf_entry.conf.end(); ++iter) 
    { 
        meta.peers.push_back(iter->to_string()) ;
    }
    for (Configuration::const_iterator iter = conf_entry.old_conf.begin(); iter != conf_entry.old_conf.end(); ++iter) 
    { 
        meta.oldPeers.push_back(iter->to_string())  ;
    }

    // SnapshotWriter* writer = done->start(meta);
    // if (!writer) {
    //     done->status().set_error(EINVAL, "snapshot_storage create SnapshotWriter failed");
    //     done->Run();
    //     return;
    // }

    //业务处理,保存到文件
    _fsm->on_snapshot_save(meta);
    return;
}



int FSMCaller::on_snapshot_load() 
{
    ApplyTask task;
    task.type = SNAPSHOT_LOAD;

    push_back(task);

    return 0;
}


void FSMCaller::do_snapshot_load() 
{

    SnapshotReader* reader = new LocalSnapshotReader();

    SnapshotMeta meta;
    int ret = reader->load_meta(&meta);
    if (0 != ret) 
    {
        TLOGERROR_RAFT("SnapshotReader load_meta failed."<<endl);
        
        return;
    }

    LogId last_applied_id;
    last_applied_id.index = _last_applied_index.load(std::memory_order_relaxed);
    last_applied_id.term = _last_applied_term;
    LogId snapshot_id;
    snapshot_id.index = meta.lastIncludedIndex;
    snapshot_id.term = meta.lastIncludedTerm;
    if (last_applied_id > snapshot_id) //已经应用到状态机的id比快照的大,不再需要从快照load数据
    {
         TLOGINFO_RAFT("Loading a stale snapshot last_applied_index="<< last_applied_id.index <<" last_applied_term="<<last_applied_id.term
                       <<" snapshot_index="<<snapshot_id.index<<" snapshot_term="<<snapshot_id.term<<endl)
                                 
        return ;
    }

    //从文件load数据到db
    ret = _fsm->on_snapshot_load();
    if (ret != 0) 
    {
        TLOGERROR_RAFT("StateMachine on_snapshot_load failed"<<endl);
        
        return;
    }

    if (meta.oldPeers.size() == 0) 
    {
        // Joint stage is not supposed to be noticeable by end users.
        Configuration conf;
        for (int i = 0; i < meta.peers.size(); ++i) 
        {
            conf.add_peer(meta.peers[i]);
        }
        _fsm->on_configuration_committed(conf, meta.lastIncludedIndex);
    }

    //更新最后应用的id
    _last_applied_index.store(meta.lastIncludedIndex, std::memory_order_release);
    _last_applied_term = meta.lastIncludedTerm;
    
}




IteratorImpl::IteratorImpl(StateMachine* sm, LogManager* lm,
                          std::vector<ClientContext> &vContext, 
                          int64_t first_closure_index,
                          int64_t last_applied_index, 
                          int64_t committed_index,
                          std::atomic<int64_t>* applying_index)
        : _sm(sm)
        , _lm(lm)
        , _vClientContext(vContext)
        , _first_closure_index(first_closure_index)
        , _cur_index(last_applied_index)
        , _committed_index(committed_index)
        , _applying_index(applying_index)
{ next(); }

void IteratorImpl::next() 
{
    if (_cur_index <= _committed_index) 
    {
        ++_cur_index;
        if (_cur_index <= _committed_index) 
        {
            if (!_lm->get_entry(_cur_index,_cur_entry))
            {
                TLOGERROR_RAFT("Fail to get entry at index="<< _cur_index<<
                        " while committed_index="<< _committed_index<<endl);
            }
            _applying_index->store(_cur_index, std::memory_order_relaxed);
        }
    }
}

void Iterator::next() 
{
    if (valid()) 
    {
        _impl->next();
    }
}

bool Iterator::valid() const 
{
    return _impl->is_good() && _impl->entry().cmdType != 0   && _impl->entry().cmdType != CM_Config;
}

const LogEntry& Iterator::entry() const 
{ 
    return _impl->entry(); 
}
const ClientContext* Iterator::done() const
{
    return _impl->done();
}

const ClientContext* IteratorImpl::done() const 
{
    if (_cur_index < _first_closure_index)
    {
        return NULL;
    }
    return &_vClientContext[_cur_index - _first_closure_index];
}




}
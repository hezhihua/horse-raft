#include "raft/FsmCaller.h"
#include "logger/logger.h"
#include "raft/Node.h"
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
	        timedWait(300);		
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

const ClientContext* IteratorImpl::done() const 
{
    if (_cur_index < _first_closure_index)
    {
        return NULL;
    }
    return &_vClientContext[_cur_index - _first_closure_index];
}




}
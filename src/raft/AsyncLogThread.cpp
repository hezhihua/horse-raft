/**
 * Tencent is pleased to support the open source community by making Tars available.
 *
 * Copyright (C) 2016THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except 
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the 
 * specific language governing permissions and limitations under the License.
 */

#include "raft/AsyncLogThread.h"
#include "logger/logger.h"
#include "raft/Replicator.h"
#include "raft/Node.h"
namespace horsedb
{

AsyncLogThread::AsyncLogThread(size_t iQueueCap)
: _terminate(false), _iQueueCap(iQueueCap)
{
	 _logEntryContextQueue = new TC_CasQueue<LogEntryContext>();
	 
	start();
     
}
AsyncLogThread::AsyncLogThread(size_t iQueueCap,NodeImpl *node): _terminate(false), _iQueueCap(iQueueCap),_node(node)
{
	_logEntryContextQueue = new TC_CasQueue<LogEntryContext>();
		
	start();
}

AsyncLogThread::~AsyncLogThread()
{
    terminate();

    if(_logEntryContextQueue)
    {
        delete _logEntryContextQueue;
        _logEntryContextQueue = NULL;
    }
}

void AsyncLogThread::terminate()
{
	
        TC_ThreadLock::Lock lock(*this);

        _terminate = true;

        notifyAll();
    
}




//外在线程调用
void AsyncLogThread::push_back(LogEntryContext &msg)
{
	
	if(_logEntryContextQueue->size() >= _iQueueCap)
	{
		TLOGERROR_RAFT("[AsyncProcThread::push_back] async_queue full:" << _logEntryContextQueue->size() << ">=" << _iQueueCap << endl);
		//delete msg;
	}
	else
	{
		_logEntryContextQueue->push_back(msg);

		//lock构造时加锁,析构时解锁并通知AsyncLogThread线程
		TC_ThreadLock::Lock lock(*this);
		
		notify();
	}

}


void AsyncLogThread::run()
{
    while (!_terminate)
    {
        LogEntryContext  log;

        if (_logEntryContextQueue->pop_front(log))
        {
	        process(log);
        }
		else
		{
			time2process();
			TC_ThreadLock::Lock lock(*this);
			//一个请求包被处理要等待的最大时间
	        timedWait(100);//200		
		}
    }
}

//只在本线程调用,不加锁
bool AsyncLogThread::batchWrite()
{
	if (_batchLog.size()==0)
	{
		return true;
	}

	//通知所有replicator线程 给远程节点发送日志
	auto &rmap=ReplicatorGroup::getInstance()->getAllReplicator();
	for (auto &item : rmap)
	{
		ReplicLogTask tReplicLogTask;
		tReplicLogTask._ReplicType=REPLIC_REQDATA;
		tReplicLogTask._batchLog=_batchLog;
		if (!item.second->isTerminnate())
		{
			item.second->push_back(tReplicLogTask);
		}
	}
	
	//落地
	_node->_log_manager->append_entries(_batchLog);
	
	
	//投票给自己
	_node->_ballot_box->commit_at(_batchLog[0].index,_batchLog[0].index+_batchLog.size()-1,_node->_server_id);

	vector<LogEntry>().swap(_batchLog);  


}

void AsyncLogThread::process(LogEntryContext &log)
{
	TLOGINFO_RAFT("[AsyncLogThread::process] get one msg." << endl);

	try
	{
		_last_time_processed=TNOWMS;
	
		_batchLog.push_back(log._LogEntry);
		//一次请求需要一次投票
		_node->_ballot_box->append_pending_task(_node->_conf.conf,_node->_conf.old_conf.empty() ? NULL : &_node->_conf.old_conf,log._ClientContext);

		if (_batchLog.size()==20||TNOWMS-_last_time_processed>100)
		{
			batchWrite();
		}

	}
	catch (exception& e)
	{
		TLOGERROR_RAFT("[AsyncLogThread exception]:" << e.what() << endl);
	}
	catch (...)
	{
		TLOGERROR_RAFT("[AsyncLogThread exception.]" << endl);
	}
}

void AsyncLogThread::time2process()
{
	//TLOGINFO_RAFT("[AsyncLogThread] time2process." << endl);

	try
	{

		//比如收到批量20个包消耗的时间为50毫秒,这个数值要小于50毫秒
		if (TNOWMS -_last_time_processed > 30)
		{
			batchWrite();
		}

	}
	catch (exception& e)
	{
		TLOGERROR_RAFT("[AsyncLogThread exception]:" << e.what() << endl);
	}
	catch (...)
	{
		TLOGERROR_RAFT("[AsyncLogThread exception.]" << endl);
	}
}

/////////////////////////////////////////////////////////////////////////
}

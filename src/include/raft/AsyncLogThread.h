/**
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

#ifndef __ASYNC_LOG_THREAD_H_
#define __ASYNC_LOG_THREAD_H_


#include "client/util/tc_cas_queue.h"
#include "util/tc_thread.h"
#include "raft/LogEntryContext.h"
#include "kv/DBBase.h"
#include "raft/BallotBox.h"
namespace horsedb
{

//////////////////////////////////////////////////////////
/**
 * 客户端请求数据异步队列处理,只有leader才能start此线程
 * 批量数据落地
 * 给自己投票
 * 通知replicator线程同步到远程节点
 */
class AsyncLogThread : public TC_Thread, public TC_ThreadLock
{
public:
    /**
     * 构造函数
     */
    AsyncLogThread(size_t iQueueCap);

    /**
     * 析构函数
     */
    virtual ~AsyncLogThread();

    void init();

    /**
     * 结束处理线程
     */
    void terminate();

    /**
     * 插入
     */
    void push_back(LogEntryContext &msg);
    bool batchWrite();

    /**
     * 从队列中取消息后执行回调逻辑
     */
    void run();

    /**
     * 获取异步队列的大小
     */
    size_t getSize()
    {
        return _logEntryContextQueue->size();
    }
    string getLogIndex();

    void time2process();

protected:
	void process(LogEntryContext &msg);

private:
    /**
     * 是否需要退出
     */
    bool            _terminate;

    /**
     * 请求队列
     */
    TC_CasQueue<LogEntryContext> *_logEntryContextQueue;

    /**
     * 队列流量控制
     */
    size_t _iQueueCap;

    vector<RaftDBPrx> _vRaftDBPrx;

    
    vector<LogEntry> _batchLog;


    std::shared_ptr<DBBase>  _db;

    int64_t  _last_time_processed;

    string _sGroupID;

    string _server_id;


    NodeImpl* _node;
    


};
///////////////////////////////////////////////////////
}
#endif

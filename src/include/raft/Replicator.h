
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

#ifndef __REPLICATOR_H_
#define __REPLICATOR_H_

#include <vector>
#include "client/util/tc_cas_queue.h"
#include "util/tc_thread.h"
#include "util/tc_singleton.h"
#include "raft/Configuration.h"
#include "raft/RaftDB.h"
#include "raft/BallotBox.h"

#define raft_max_entries_size 1024
#define raft_max_parallel_append_entries_rpc_num 1


using namespace std;

namespace horsedb
{


enum ReplicType {
    REPLIC_REQDATA,
    REPLIC_DATA,
    REPLIC_EMPTY,
    REPLIC_HEART
};

struct ReplicLogTask {
    ReplicType _ReplicType;
    vector<LogEntry> _batchLog;
};

class LogManager;
class NodeImpl;
struct ReplicatorOptions {
    
    int dynamic_heartbeat_timeout_ms;
    int election_timeout_ms;
    GroupId group_id;
    PeerId server_id;
    PeerId peer_id;
    LogManager* log_manager;
    BallotBox* ballot_box;
    NodeImpl *node;
    int64_t term;

};

//////////////////////////////////////////////////////////
/**
 * leader发现follower日志不同步,异步append_entry日志复制和定时心跳,leader才能启动此线程
 */
class Replicator : public TC_Thread, public TC_ThreadLock
{
public:
    /**
     * 构造函数
     */
    Replicator(size_t iQueueCap);

    /**
     * 析构函数
     */
    virtual ~Replicator();


    void init(const ReplicatorOptions &options);
    void _cancel_append_entries_rpcs() ;
    


    void sendEmptyEntries(bool is_heartbeat);
    void sendEntries();
    void sendEntries(const vector<LogEntry> &batchLog);
    void _reset_next_index() ;
    void _install_snapshot();

        // Send TimeoutNowRequest to the very follower to make it become
    // CANDIDATE. And the replicator would stop automatically after the RPC
    // finishes no matter it succes or fails.
    static int send_timeout_now_and_stop( Replicator*);


    /**
     * 结束处理线程
     */
    void terminate();

    
    /**
     * 插入
     */
    void push_back(ReplicLogTask &task);

    /**
     * 从队列中取消息后执行回调逻辑
     */
    void run();

    /**
     * 获取异步队列的大小
     */
    size_t getSize()
    {
        return _replicLogTaskQueue->size();
    }

    void _update_last_rpc_send_timestamp(int64_t last_rpc_send_timestamp)
    {
        _last_rpc_send_timestamp=last_rpc_send_timestamp;
    }

    int64_t _min_flying_index() 
    {
        return _next_index - _flying_append_entries_size;
    }

     void _send_timeout_now() ;


    TC_CasQueue<ReplicLogTask> *_replicLogTaskQueue;

protected:
	void process(ReplicLogTask &task);
    
    int _prepare_entry(int offset, LogEntry &tLogEntry) ;
private:
    enum StType {
        IDLE,
        BLOCKING,
        APPENDING_ENTRIES,
        INSTALLING_SNAPSHOT,
    };
    struct StatInfo {
        StType type;
        union {
            int64_t first_log_index;
            int64_t last_log_included;
        };
        union {
            int64_t last_log_index;
            int64_t last_term_included;
        };
    };

    struct FlyingAppendEntriesRpc {
        int64_t log_index;
        int entries_size;
        FlyingAppendEntriesRpc(int64_t index, int size): log_index(index), entries_size(size) {}
    };

    /**
     * 是否需要退出
     */
    bool            _terminate;


    /**
     * 队列流量控制
     */
    size_t _iQueueCap;

   

    RaftDBPrx _raftDBPrx;

    int64_t _next_index;
    bool _is_waiter_canceled;

    ReplicatorOptions _options;

    int64_t _flying_append_entries_size;
    int _consecutive_error_times;
    bool _has_succeeded;
    int64_t _timeout_now_index;
    // the sending time of last successful RPC
    int64_t _last_rpc_send_timestamp;
    int64_t _heartbeat_counter;
    int64_t _append_entries_counter;
    int64_t _install_snapshot_counter;
    int64_t _readonly_index;
    StatInfo _st;
    std::deque<FlyingAppendEntriesRpc> _append_entries_in_fly;


};
struct ReplicatorGroupOptions {
    
    int heartbeat_timeout_ms;
    int election_timeout_ms;
    LogManager* log_manager;
    BallotBox* ballot_box;
    NodeImpl* node;
    //SnapshotStorage* snapshot_storage;
    //SnapshotThrottle* snapshot_throttle;
};
class ReplicatorGroup :public TC_Singleton<ReplicatorGroup>{
public:
    ReplicatorGroup();
    ~ReplicatorGroup();
    int init(const NodeId& node_id, const ReplicatorGroupOptions& options);
    
    // start a replicator attached with |peer|
    // will be a notification when the replicator catches up according to the
    // arguments.
    // NOTE: when calling this function, the replicatos starts to work
    // immediately, annd might call node->step_down which might have race with
    // the caller, you should deal with this situation.
    int start_replicator(const PeerId &peer);

    Replicator * get_replicator(const PeerId& peer) ;

    

    int64_t last_rpc_send_timestamp(const PeerId& peer);

    // Stop all the replicators
    int stop_all();

    int stop_replicator(const PeerId &peer);

    // Reset the term of all to-add replicators.
    // This method is supposed to be called when the very candidate becomes the
    // leader, so we suppose that there are no running replicators.
    // Return 0 on success, -1 otherwise
    int reset_term(int64_t new_term);

    // Reset the interval of heartbeat
    // This method is supposed to be called when the very candidate becomes the
    // leader, use new heartbeat_interval, maybe call vote() reset election_timeout
    // Return 0 on success, -1 otherwise
    int reset_heartbeat_interval(int new_interval_ms);
    
    // Reset the interval of election_timeout for replicator, 
    // used in rpc's set_timeout_ms
    int reset_election_timeout_interval(int new_interval_ms);

    // Returns true if the there's a replicator attached to the given |peer|
    bool contains(const PeerId& peer) const;

    // Transfer leadership to the given |peer|
    int transfer_leadership_to(const PeerId& peer, int64_t log_index);

    // Stop transferring leadership to the given |peer|
    int stop_transfer_leadership(const PeerId& peer);

    // Stop all the replicators except for the one that we think can be the
    // candidate of the next leader, which has the largest `last_log_id' among
    // peers in |current_conf|. 
    // |candidate| would be assigned to a valid ReplicatorId if we found one and
    // the caller is responsible for stopping it, or an invalid value if we
    // found none.
    // Returns 0 on success and -1 otherwise.
    int stop_all_and_find_the_next_candidate(Replicator* candidate, const ConfigurationEntry& conf);
    
    // Find the follower with the most log entries in this group, which is
    // likely becomes the leader according to the election algorithm of raft.
    // Returns 0 on success and |peer_id| is assigned with the very peer.
    // -1 otherwise.
    int find_the_next_candidate(PeerId* peer_id, const ConfigurationEntry& conf);

    // List all the existing replicators
    void list_replicators(std::vector<Replicator*>& out) const;

    // List all the existing replicators with PeerId
    void list_replicators(std::vector<std::pair<PeerId, Replicator*> >& out) const;

    // Change the readonly config for a peer
    int change_readonly_config(const PeerId& peer, bool readonly);

    // Check if a replicator is in readonly
    bool readonly(const PeerId& peer) const;

    const std::map<PeerId, Replicator*>& getAllReplicator(){return _rMap;}
    const map<string,RaftDBPrx>& getAllProxy(){return _mPrx;}

private:

    std::map<PeerId, Replicator*> _rMap;//一个节点leader开一个线程同步
    map<string,RaftDBPrx> _mPrx;

    ReplicatorOptions _common_options;
    int _dynamic_timeout_ms;
    int _election_timeout_ms;
};

///////////////////////////////////////////////////////
}
#endif

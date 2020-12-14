

#include "raft/RaftDBCallback.h"
#include "raft/Replicator.h"
#include "raft/RaftState.h"
#include "logger/logger.h"
#include "raft/Node.h"
namespace horsedb {


    //leader receive AppendEntriesRes
    void RaftDBCallback::callback_appendEntries(horsedb::Int32 ret,  const horsedb::AppendEntriesReq &tReq, const horsedb::AppendEntriesRes& tRes)
    {
        try
        {
            do
            {
                Replicator *replicator=ReplicatorGroup::getInstance()->get_replicator(PeerId(tReq.peerID))    ;
                if (replicator==NULL)
                {
                    break;
                }

               TLOGWARN_RAFT(  "node " << replicator->_options.group_id << ":" << replicator->_options.server_id 
                << " received AppendEntriesResponse from "
                << replicator->_options.peer_id << " prev_log_index " << tReq.prevLogIndex
                << " prev_log_term " << tReq.prevLogTerm << " count " << tReq.logEntries.size() <<endl) ;

                //bool valid_rpc = false;
                int64_t rpc_first_index = tReq.prevLogIndex + 1;
                int64_t min_flying_index = replicator->_min_flying_index();
                if (min_flying_index<=0)
                {
                    TLOGWARN_RAFT(  "min_flying_index<=0 "  <<endl) ;
                }

                replicator->_update_last_rpc_send_timestamp(TNOWMS);

                for (auto rpc_it = replicator->_append_entries_in_fly.begin(); rpc_it != replicator->_append_entries_in_fly.end(); ++rpc_it) 
                {
                    if (rpc_it->log_index > rpc_first_index) 
                    {
                        break;
                    }
                   
                }

                //todo timeout
                if (true)
                {
                    /* timeout do in callback_AppendEntries_exception*/
                }

                if (!tRes.isSuccess) 
                {
                    if (tRes.term > replicator->_options.term/*follwer 的任期比leader的要大*/) 
                    {
                        TLOGWARN_RAFT( " fail, greater term " << tRes.term
                                << " expect term " << replicator->_options.term<<endl);
                        replicator->_reset_next_index();

                        NodeImpl *node_impl = replicator->_options.node;
                        
                        //r->_notify_on_caught_up(EPERM, true);
                        RaftState status(EHIGHERTERMRESPONSE, "Leader receives higher term  from peer:"+ replicator->_options.peer_id.to_string());
                        //replicator->_destroy();
                        node_impl->increase_term_to(tRes.term, status);//降为follwer
                        
                        return;
                    }

                    //
                    TLOGWARN_RAFT( " fail, find next_index remote last_log_index " << tRes.lastLogIndex
                                << " local next_index " << replicator->_next_index 
                                << " rpc prev_log_index " << tReq.prevLogIndex<<endl );
                    
                    
                    // prev_log_index and prev_log_term doesn't match
                    replicator->_reset_next_index();
                    if (tRes.lastLogIndex + 1 < replicator->_next_index) 
                    {
                        TLOGWARN_RAFT(  "Group " << replicator->_options.group_id
                                << " last_log_index at peer=" << replicator->_options.peer_id 
                                << " is " << tRes.lastLogIndex <<endl);
                        // The peer contains less logs than leader
                        //replicator->_next_index 初始化的时候初始为leader落地日志最后一个Index+1
                        replicator->_next_index = tRes.lastLogIndex + 1;
                    } 
                    else 
                    {  
                        // The peer contains logs from old term which should be truncated,
                        // decrease _last_log_at_peer by one to test the right index to keep
                        if (replicator->_next_index > 1) 
                        {
                            TLOGWARN_RAFT(  "Group " << replicator->_options.group_id 
                                    << " log_index=" << replicator->_next_index << " mismatch" <<endl);
                            --replicator->_next_index;
                        } 
                        else 
                        {
                            TLOGERROR_RAFT( "Group " << replicator->_options.group_id 
                                    << " peer=" << replicator->_options.peer_id
                                    << " declares that log at index=0 doesn't match,"
                                        " which is not supposed to happen" <<endl);
                        }
                    }

                    //todo 通知replicator,马上再发送请求检测是否对得上follower的index
                    //对得上,follwer返回true
                    ReplicLogTask tReplicLogTask;
                    tReplicLogTask._ReplicType=REPLIC_EMPTY;
                    replicator->push_back(tReplicLogTask);

                    //replicator->_send_empty_entries(false);


                    return;
            }

            TLOGINFO_RAFT( " success"<<endl);

            
            
            if (tRes.term != replicator->_options.term) 
            {
                TLOGINFO_RAFT(  "Group " << replicator->_options.group_id<< " fail, response term " << tRes.term
                                << " mismatch, expect term " << replicator->_options.term <<endl);
                replicator->_reset_next_index();
                
                return;
            }

            const int entries_size = tReq.logEntries.size();
            const int64_t rpc_last_log_index = tReq.prevLogIndex + entries_size;
            if (entries_size > 0)
            {
                //这一段已经发送成功
                TLOGINFO_RAFT("Group " << replicator->_options.group_id<< " replicated logs in [" 
                                            << min_flying_index << ", " 
                                            << rpc_last_log_index
                                            << "] to peer " << replicator->_options.peer_id<<endl);
            }

            if (entries_size > 0) 
            {
                replicator->_options.ballot_box->commit_at(min_flying_index, rpc_last_log_index,replicator->_options.peer_id);
                
            }

            // A rpc is marked as success, means all request before it are success,
            // erase them sequentially.
            while (!replicator->_append_entries_in_fly.empty() &&replicator->_append_entries_in_fly.front().log_index <= rpc_first_index) 
            {
                replicator->_flying_append_entries_size -= replicator->_append_entries_in_fly.front().entries_size;
                replicator->_append_entries_in_fly.pop_front();
            }
            replicator->_has_succeeded = true;
            replicator->_notify_on_caught_up(0, false);
            
            if (replicator->_timeout_now_index > 0 && replicator->_timeout_now_index < replicator->_min_flying_index()/*leader收到禅让管理命令*/) 
            {
                replicator->_send_timeout_now();
            }

            //todo 通知 replicator线程继续发送数据
            ReplicLogTask tReplicLogTask;
            tReplicLogTask._ReplicType=REPLIC_DATA;
            replicator->push_back(tReplicLogTask);
            //replicator->_send_entries(); 
            

            } while (0);
            
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }

        return ;
        
        

    }

    void RaftDBCallback::callback_appendEntries_exception(horsedb::Int32 ret)
    {
        //may be timeout

    }

    void RaftDBCallback::callback_appendEntries_exception(horsedb::Int32 ret,const horsedb::AppendEntriesReq &tReq)
    {

    }

    void RaftDBCallback::callback_requestVote(horsedb::Int32 ret,  const horsedb::RequestVoteRes& tRes)
    {

    }
    void RaftDBCallback::callback_requestVote_exception(horsedb::Int32 ret)
    {

    }

    void RaftDBCallback::callback_preVote(horsedb::Int32 ret,  const horsedb::RequestVoteRes& tRes)
    {

    }
    void RaftDBCallback::callback_preVote_exception(horsedb::Int32 ret)
    {

    }

    void RaftDBCallback::callback_installSnapshot(horsedb::Int32 ret,  const horsedb::InstallSnapshotRes& tRes)
    {

    }
    void RaftDBCallback::callback_installSnapshot_exception(horsedb::Int32 ret)
    {

    }

    void RaftDBCallback::callback_timeoutNow(horsedb::Int32 ret,  const horsedb::TimeoutNowRes& tRes)
    {

    }
    void RaftDBCallback::callback_timeoutNow_exception(horsedb::Int32 ret)
    {

    }




}
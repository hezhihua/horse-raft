
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

#ifndef _RAFT_DB_CALLBACK_
#define  _RAFT_DB_CALLBACK_

#include "raft/RaftDB.h"

namespace horsedb{

class RaftDBCallback:public RaftDBPrxCallback
{
    public:
    virtual ~RaftDBCallback(){}
    virtual void callback_appendEntries(horsedb::Int32 ret,  const horsedb::AppendEntriesRes& tRes){}
    virtual void callback_appendEntries(horsedb::Int32 ret, const horsedb::AppendEntriesReq &tReq, const horsedb::AppendEntriesRes& tRes);    
    virtual void callback_appendEntries_exception(horsedb::Int32 ret,const horsedb::AppendEntriesReq &tReq);
    virtual void callback_appendEntries_exception(horsedb::Int32 ret);

    virtual void callback_requestVote(horsedb::Int32 ret,  const horsedb::RequestVoteRes& tRes);   
    virtual void callback_requestVote_exception(horsedb::Int32 ret);

    virtual void callback_preVote(horsedb::Int32 ret,  const horsedb::RequestVoteRes& tRes);
    virtual void callback_preVote_exception(horsedb::Int32 ret);

    virtual void callback_installSnapshot(horsedb::Int32 ret,  const horsedb::InstallSnapshotRes& tRes);
    virtual void callback_installSnapshot_exception(horsedb::Int32 ret);

    virtual void callback_timeoutNow(horsedb::Int32 ret,  const horsedb::TimeoutNowRes& tRes);
    virtual void callback_timeoutNow_exception(horsedb::Int32 ret);


};
typedef shared_ptr<RaftDBCallback> RaftDBCallbackPtr;

}


#endif
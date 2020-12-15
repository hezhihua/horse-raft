#include "raft/RaftDB.h"
#include <iostream>
#include "raft/Node.h"
#include "raft/Configuration.h"
#include "raft/NodeManager.h"
using namespace std;

namespace horsedb {

 horsedb::Int32 RaftDB::appendEntries(const horsedb::AppendEntriesReq & tReq,horsedb::AppendEntriesRes &tRes,horsedb::TarsCurrentPtr current) 
 {
    int iRet=-1;
      cout<<"appendEntries,tReq.serverID="<<tReq.serverID<< ",request.peerID="<<tReq.peerID<<endl;
      try
      {
         do
         {
            PeerId peer_id;
            if (0 != peer_id.parse(tReq.peerID))
            {
               //log
               break;
            }

            NodeImpl *node=NodeManager::getInstance()->get(tReq.groupID,peer_id);
            if (node==NULL)
            {
               //log
               break;
            }

            iRet= node->handle_append_entries_request(tReq, tRes);

         } while (0);
         
      }
      catch(const std::exception& e)
      {
         std::cerr << e.what() << '\n';
      }

      return iRet;
   

 }

  horsedb::Int32 RaftDB::requestVote(const horsedb::RequestVoteReq & tReq,horsedb::RequestVoteRes &tRes,horsedb::TarsCurrentPtr current) 
  {
      int iRet=-1;
      try
      {
         do
         {
            PeerId peer_id;
            if (0 != peer_id.parse(tReq.peerID) )
            {
               //log
               break;
            }

            NodeImpl *node=NodeManager::getInstance()->get(tReq.groupID,peer_id);
            if (node==NULL)
            {
               //log
               break;
            }

            iRet= node->handle_request_vote_request(tReq, tRes);

         } while (0);
         
      }
      catch(const std::exception& e)
      {
         std::cerr << e.what() << '\n';
      }
   
    return iRet;
  }


horsedb::Int32  RaftDB::installSnapshot(const horsedb::InstallSnapshotReq &tReq, horsedb::InstallSnapshotRes& tRes, std::shared_ptr<horsedb::Current>)
{
   return 0;

}



 horsedb::Int32   RaftDB::preVote(const horsedb::RequestVoteReq &tReq, horsedb::RequestVoteRes& tRes, std::shared_ptr<horsedb::Current>)
 {

      int iRet=-1;
      try
      {
         do
         {
            PeerId peer_id;
            if (0 != peer_id.parse(tReq.peerID) )
            {
               //log
               break;
            }

            NodeImpl *node=NodeManager::getInstance()->get(tReq.groupID,peer_id);
            if (node==NULL)
            {
               //log
               break;
            }

            iRet= node->handle_pre_vote_request(tReq, tRes);

         } while (0);
      
      }
      catch(const std::exception& e)
      {
         std::cerr << e.what() << '\n';
      }
   
    return iRet;

 }


horsedb::Int32  RaftDB::timeoutNow(const horsedb::TimeoutNowReq &tReq, horsedb::TimeoutNowRes& tRes, std::shared_ptr<horsedb::Current>)
{
   return 0;

}


// int RaftDBPrxCallback::onDispatch(horsedb::ReqMessagePtr msg)
// {
//    static ::std::string __RaftDB_all[]=
//    {
//          "appendEntries",
//          "installSnapshot",
//          "preVote",
//          "requestVote",
//          "timeoutNow"
//    };
//    pair<string*, string*> r = equal_range(__RaftDB_all, __RaftDB_all+5, string(msg->request.sFuncName));
//    if(r.first == r.second) return horsedb::TARSSERVERNOFUNCERR;
//    switch(r.first - __RaftDB_all)
//    {
//          case 0:
//          {
//             if (msg->response->iRet != horsedb::TARSSERVERSUCCESS)
//             {
//                callback_appendEntries_exception(msg->response->iRet);

//                return msg->response->iRet;
//             }
//             horsedb::TarsInputStream<horsedb::BufferReader> _isReq;
//             horsedb::AppendEntriesReq tReq;
//             _isReq.setBuffer(msg->request.sBuffer);
//             _isReq.read(tReq, 1, true);
//             cout<<"tReq.groupID="<<tReq.groupID <<endl;


//             horsedb::TarsInputStream<horsedb::BufferReader> _is;

//             _is.setBuffer(msg->response->sBuffer);
//             horsedb::Int32 _ret;
//             _is.read(_ret, 0, true);

            
//             horsedb::AppendEntriesRes tRes;
            
//             _is.read(tRes, 2, true);
//             CallbackThreadData * pCbtd = CallbackThreadData::getData();
//             assert(pCbtd != NULL);

//             pCbtd->setResponseContext(msg->response->context);

//             callback_appendEntries(_ret, tRes);

//             pCbtd->delResponseContext();

//             return horsedb::TARSSERVERSUCCESS;

//          }
//          case 1:
//          {
//             if (msg->response->iRet != horsedb::TARSSERVERSUCCESS)
//             {
//                callback_installSnapshot_exception(msg->response->iRet);

//                return msg->response->iRet;
//             }
//             horsedb::TarsInputStream<horsedb::BufferReader> _is;

//             _is.setBuffer(msg->response->sBuffer);
//             horsedb::Int32 _ret;
//             _is.read(_ret, 0, true);

//             horsedb::InstallSnapshotRes tRes;
//             _is.read(tRes, 2, true);
//             CallbackThreadData * pCbtd = CallbackThreadData::getData();
//             assert(pCbtd != NULL);

//             pCbtd->setResponseContext(msg->response->context);

//             callback_installSnapshot(_ret, tRes);

//             pCbtd->delResponseContext();

//             return horsedb::TARSSERVERSUCCESS;

//          }
//          case 2:
//          {
//             if (msg->response->iRet != horsedb::TARSSERVERSUCCESS)
//             {
//                callback_preVote_exception(msg->response->iRet);

//                return msg->response->iRet;
//             }
//             horsedb::TarsInputStream<horsedb::BufferReader> _is;

//             _is.setBuffer(msg->response->sBuffer);
//             horsedb::Int32 _ret;
//             _is.read(_ret, 0, true);

//             horsedb::RequestVoteRes tRes;
//             _is.read(tRes, 2, true);
//             CallbackThreadData * pCbtd = CallbackThreadData::getData();
//             assert(pCbtd != NULL);

//             pCbtd->setResponseContext(msg->response->context);

//             callback_preVote(_ret, tRes);

//             pCbtd->delResponseContext();

//             return horsedb::TARSSERVERSUCCESS;

//          }
//          case 3:
//          {
//             if (msg->response->iRet != horsedb::TARSSERVERSUCCESS)
//             {
//                callback_requestVote_exception(msg->response->iRet);

//                return msg->response->iRet;
//             }
//             horsedb::TarsInputStream<horsedb::BufferReader> _is;

//             _is.setBuffer(msg->response->sBuffer);
//             horsedb::Int32 _ret;
//             _is.read(_ret, 0, true);

//             horsedb::RequestVoteRes tRes;
//             _is.read(tRes, 2, true);
//             CallbackThreadData * pCbtd = CallbackThreadData::getData();
//             assert(pCbtd != NULL);

//             pCbtd->setResponseContext(msg->response->context);

//             callback_requestVote(_ret, tRes);

//             pCbtd->delResponseContext();

//             return horsedb::TARSSERVERSUCCESS;

//          }
//          case 4:
//          {
//             if (msg->response->iRet != horsedb::TARSSERVERSUCCESS)
//             {
//                callback_timeoutNow_exception(msg->response->iRet);

//                return msg->response->iRet;
//             }
//             horsedb::TarsInputStream<horsedb::BufferReader> _is;

//             _is.setBuffer(msg->response->sBuffer);
//             horsedb::Int32 _ret;
//             _is.read(_ret, 0, true);

//             horsedb::TimeoutNowRes tRes;
//             _is.read(tRes, 2, true);
//             CallbackThreadData * pCbtd = CallbackThreadData::getData();
//             assert(pCbtd != NULL);

//             pCbtd->setResponseContext(msg->response->context);

//             callback_timeoutNow(_ret, tRes);

//             pCbtd->delResponseContext();

//             return horsedb::TARSSERVERSUCCESS;

//          }
//    }
//    return horsedb::TARSSERVERNOFUNCERR;
// }

    

}
#include "raft/RocksDBStorage.h"
#include "raft/RaftDB.h"
#include "logger/logger.h"
namespace horsedb {



    RocksDBStorage::RocksDBStorage(std::shared_ptr<DBBase> &db,const string &sGroupID):_dbbase(db),_sGroupID(sGroupID)
    {

    }

    int RocksDBStorage::init(ConfigurationManager* configuration_manager) 
    {
         _config_manager=configuration_manager;
        //0x02 + 组id(4字节大端序) + 0x01 + LogIndex（8字节大端序）
        
        _sPreKey.append(1,0x02);
        string  sBigGroupID;
        TC_Common::encodeID32(TC_Common::strto<int32_t>(_sGroupID),sBigGroupID);
        _sPreKey.append(sBigGroupID);
        _sPreKey.append(1,0x01);

        _sDBName="logentry";
        _dbbase->Create(_sDBName);

        //0x02 + 组id(4字节大端序) + 0x04
        _sVoteMetaKey.append(1,0x02);

        _sVoteMetaKey.append(sBigGroupID);
        _sVoteMetaKey.append(1,0x04);

        //todo 设置选举信息

        if (first_log_index_from_db()<0)
        {
            _first_log_index.store(1);
            _last_log_index.store(0);
        }
        

        return 0;

    }


    int64_t RocksDBStorage::first_log_index()
    {
        return _first_log_index.load(std::memory_order_acquire);;
    }
    int64_t RocksDBStorage::last_log_index()
    {
        return _last_log_index.load(std::memory_order_acquire);;
    }


    int64_t RocksDBStorage::first_log_index_from_db()
    {
        try
        {
            string key,value;
            if (_dbbase->GetFirstKV(key,value,_sDBName))
            {
                LogEntry tLogEntry;
                vector<char> valueBuff;
                valueBuff.assign(value.begin(),value.end());

                horsedb::TarsInputStream<horsedb::BufferReader> _is;
                _is.setBuffer(valueBuff);
                tLogEntry.readFrom(_is);

                _first_log_index.store(tLogEntry.index);

                return tLogEntry.index;
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << "first_log_index_from_db:"<<e.what() << '\n';
        }
        
        
        
        return -1;

    }

    int64_t RocksDBStorage::last_log_index_from_db()
    {
        try
        {
            string key,value;
            if (_dbbase->GetLastKV(key,value,_sDBName))
            {
                LogEntry tLogEntry;
                vector<char> valueBuff;
                valueBuff.assign(value.begin(),value.end());

                horsedb::TarsInputStream<horsedb::BufferReader> _is;
                _is.setBuffer(valueBuff);
                tLogEntry.readFrom(_is);
                _last_log_index.store(tLogEntry.index);

                return tLogEntry.index;
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
        return -1;

    }

    LogEntry* RocksDBStorage::get_entry(const int64_t index) 
    {
        return nullptr;

    }

    bool RocksDBStorage::get_entry(const int64_t index,LogEntry &tLogEntry)
    {
        string key=getLogIndexKey(tLogEntry.index);

        string value;

        try
        {
            if (_dbbase->Get(key, value,_sDBName))
            {
                vector<char> valueBuff;
                valueBuff.assign(value.begin(),value.end());

                horsedb::TarsInputStream<horsedb::BufferReader> _is;
                _is.setBuffer(valueBuff);
                tLogEntry.readFrom(_is);

                return true;
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
        return false;

    }

    int64_t RocksDBStorage::get_term(const int64_t index)
    {
        LogEntry tLogEntry;
        if (get_entry( index,tLogEntry))
        {
            return tLogEntry.term;
        }
        
        return -1;
        
    }


    int RocksDBStorage::append_entry(const LogEntry& entry) 
    {
        std::vector<LogEntry> entries;
        entries.push_back(entry);
        _last_log_index.fetch_add(1, std::memory_order_release);
        return append_entries(entries);

    }

    string RocksDBStorage::getLogIndexKey2(const string &sGroupID,int64_t lLogIndex)
    {

        //0x02 + 组id(4字节大端序) + 0x01 + LogIndex（8字节大端序）
        string sLogKey;
        sLogKey.append(1,0x02);
        string  sBigGroupID;
        TC_Common::encodeID32(TC_Common::strto<int32_t>(sGroupID),sBigGroupID);
        sLogKey.append(sBigGroupID);
        sLogKey.append(1,0x01);
	

        string sBigLogIndex;
        TC_Common::encodeID64(lLogIndex,sBigLogIndex);
        return  sLogKey+sBigLogIndex;
        
    }

    string RocksDBStorage::getLogIndexKey(int64_t lLogIndex)
    {

        string sBigLogIndex;
        TC_Common::encodeID64(lLogIndex,sBigLogIndex);
        return  _sPreKey+sBigLogIndex;
        
    }




    int RocksDBStorage::append_entries(const std::vector<LogEntry>& entries) 
    {
        rocksdb::WriteBatch updates;

        for (auto &logEntry :entries)
        {
            string key=getLogIndexKey(logEntry.index);
        

            horsedb::TarsOutputStream<horsedb::BufferWriterVector> _os;
            logEntry.writeTo(_os);
            string value(_os.getBuffer(), _os.getLength());  

            _dbbase->Put2WriteBatch(updates,key, value,_sDBName);

            _last_log_index.fetch_add(1, std::memory_order_release);
        }


        if (!_dbbase->WriteBatch(updates,_sDBName))
        {
            return -1;
        }
        return 0;

    }


    int RocksDBStorage::truncate_prefix(const int64_t first_index_kept) 
    {
        string key,value;
        if (_dbbase->GetFirstKV(key,value,_sDBName))
        {
            string beginkey(key);
            string sLogIndex=getLogIndexKey(first_index_kept);
            string endkey(sLogIndex);
            return _dbbase->DeleteRange(beginkey,endkey,_sDBName) ? 0 :-1;

        }

        return -1;

    }


    int RocksDBStorage::truncate_suffix(const int64_t last_index_kept) 
    {
        string key,value;
        if (_dbbase->GetLastKV(key,value,_sDBName))
        {
            string endkey(key);
            string sLogIndex=getLogIndexKey(last_index_kept);
            string beginkey(sLogIndex);
            return _dbbase->DeleteRange(beginkey,endkey,_sDBName) ? 0:-1;

        }
        return -1;

    }



    int RocksDBStorage::reset(const int64_t next_log_index) 
    {
        return 0;

    }

    int RocksDBStorage::set_term_and_votedfor(const int64_t term, const PeerId& peer_id,const VersionedGroupId& group) 
    {
        int iRet=-1;
        try
        {
            do
            {

                VoteInfoMeta tVoteInfoMeta;
                tVoteInfoMeta.term = term;
                tVoteInfoMeta.votedfor = peer_id.to_string();
                horsedb::TarsOutputStream<horsedb::BufferWriterVector> _os;
                tVoteInfoMeta.writeTo(_os);
                string value(_os.getBuffer(), _os.getLength());  

                if (!_dbbase->Put(_sVoteMetaKey,value,_sDBName))
                {
                    break;
                }
                
                iRet=0;

            }while(0);
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }

        return iRet;
          
    }

    int RocksDBStorage::get_term_and_votedfor(int64_t* term, PeerId* peer_id, const VersionedGroupId& group) 
    {
        int iRet=-1;
        try
        {
            do
            {
                long lStartTime=TNOWMS;                
                std::string value;
                rocksdb::Slice key(_sVoteMetaKey);
                auto st = _dbbase->GetS( _sVoteMetaKey, value,_sDBName);
                if (st.IsNotFound()) 
                {
                    // Not exist in db, set initial term 1 and votedfor 0.0.0.0:0:0 ,tcp -h 0.0.0.0 -p 0
                    *term = 1;
                    *peer_id = PeerId("tcp -h 0.0.0.0 -p 0");
                    
                    set_term_and_votedfor(*term, *peer_id, group);
                    
                    TLOGWARN_RAFT( "group= " << group<< " succeed to set initial term and votedfor when first time init"<< endl)
                    iRet=0;
                    break;
                } 
                else if (!st.ok()) 
                {
                    TLOGERROR_RAFT( "group " << group
                            << " failed to get value from db, key " << key.ToString()
                            << ", error " << st.ToString()<<endl );
                    
                    break;
                }
            
                VoteInfoMeta tVoteInfoMeta;
                vector<char> valueBuff;
                valueBuff.assign(value.begin(),value.end());

                horsedb::TarsInputStream<horsedb::BufferReader> _is;
                _is.setBuffer(valueBuff);
                tVoteInfoMeta.readFrom(_is);

                *term=tVoteInfoMeta.term;
                *peer_id= PeerId(tVoteInfoMeta.votedfor);


                
                TLOGINFO_RAFT( "Loaded vote meta,  " 
                        << " group " << group
                        << " term " << tVoteInfoMeta.term
                        << " votedfor " << tVoteInfoMeta.votedfor
                        << " time: " << TNOWMS-lStartTime <<endl);
                iRet=0;
            
                
            } while (0);
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }

        return iRet;
        

    }
    
    



}
#include "raft/Storage.h"
#include "raft/RocksDBStorage.h"
#include "kv/DBBase.h"
#include "cfg/config.h"

namespace horsedb {

    LogStorage* LogStorage::create(const string& type, shared_ptr<DBBase> &pDB)
    {
        if (type=="rocksdb")
        {
            return new RocksDBStorage(pDB,G_GroupID);
        }
        else 
        {
            return nullptr;
        }
        
    }


}

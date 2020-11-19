#include "raft/Storage.h"
#include "raft/RocksDBStorage.h"
#include "kv/DBBase.h"
#include "cfg/config.h"

namespace horsedb {

    LogStorage* LogStorage::create(const string& type)
    {
        if (type=="rocksdb")
        {
            std::shared_ptr<DBBase> db(DBBase::getInstance());
            return new RocksDBStorage(db,G_GroupID);
        }
        else 
        {
            return nullptr;
        }
        
    }


}

#ifndef __DB_BASE__
#define __DB_BASE__
#include <string>
#include <vector>
#include <map>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/c.h"
#include "util/tc_singleton.h"


using namespace std;
using namespace rocksdb;

namespace horsedb{
    class  DBBase :public TC_Singleton<DBBase>
    {
    private:
        /* data */
    public:

        //DBBase(const string &dbPath,const vector<string> &vColumnFamilyName);
        void init(const string &dbPath,const vector<string> &vColumnFamilyName);
        ~ DBBase();

        bool Create(const string&dbname);
        bool DBExist(const string& dbname);
        bool Put(const string&key,const string &value,const string& dbname);

        bool WriteBatch(rocksdb::WriteBatch &updates,const string& db);
        bool Put2WriteBatch(rocksdb::WriteBatch &updates,const string&key,const string &value,const string& db);

        bool Get(const string&key, string &value,const string& dbname);
        rocksdb::Status GetS(const string&key, string &value,const string& db);

        bool KeyMayExist(const string&key, string &value,const string& dbname="sys");
        bool RangeGet(const string&beginkey, string &endkey,vector<string> &vValue,const string& dbname);
        bool PreKeyGet(const string&prekey, vector<string> &vValue,const string& dbname, vector<string> &vKey );
        bool ShowDB(vector<string> &vDBName );

        bool GetFirstKV( string&key, string &value,const string& dbname);
        bool GetLastKV( string&key, string &value,const string& dbname);
        bool DeleteRange(const string& begin_key, const string& end_key,const string& dbname);
        bool Delete(const string& key, const string& dbname);

        DB*  db(){return _db;}

        std::vector<ColumnFamilyHandle*> _handles;
        std::map<string,ColumnFamilyHandle*> _mhandles;

        DB* _db;
        string _dbPath;
        ReadOptions _readOptions;
        WriteOptions _WriteOptions;
        Options _options;

    };

}



#endif
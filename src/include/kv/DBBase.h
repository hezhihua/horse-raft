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
    class  DBBase 
    {
    private:
        /* data */
    public:

        //DBBase(const string &dbPath,const vector<string> &vColumnFamilyName);
        virtual void init(const string &dbPath,const vector<string> &vColumnFamilyName);
        virtual ~ DBBase();

        virtual bool Create(const string&dbname,const map<string,string> &msession=map<string,string>());
        virtual bool DBExist(const string& dbname);
        virtual bool Put(const string&key,const string &value,const string& dbname,const map<string,string> &msession=map<string,string>());

        virtual bool WriteBatch(rocksdb::WriteBatch &updates,const string& db,const map<string,string> &msession=map<string,string>());
        virtual bool Put2WriteBatch(rocksdb::WriteBatch &updates,const string&key,const string &value,const string& db,const map<string,string> &msession=map<string,string>());

        virtual bool Get(const string&key, string &value,const string& dbname);
        virtual rocksdb::Status GetS(const string&key, string &value,const string& db);

        virtual bool KeyMayExist(const string&key, string &value,const string& dbname="sys");
        bool RangeGet(const string&beginkey, string &endkey,vector<string> &vValue,const string& dbname);
        virtual bool PreKeyGet(const string&prekey, vector<string> &vValue,const string& dbname, vector<string> &vKey );
        virtual bool ShowDB(vector<string> &vDBName );

        virtual bool GetFirstKV( string&key, string &value,const string& dbname);
        virtual bool GetLastKV( string&key, string &value,const string& dbname);
        virtual bool DeleteRange(const string& begin_key, const string& end_key,const string& dbname,const map<string,string> &msession=map<string,string>());
        virtual bool Delete(const string& key, const string& dbname,const map<string,string> &msession=map<string,string>());

        virtual bool PreKeyGetFirst(const string&prekey, string &firstKey, string &firstValue,const string& dbname);

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
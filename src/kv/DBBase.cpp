#include "kv/DBBase.h"
#include <iostream>
using namespace std;

namespace horsedb {
    
    void DBBase:: init(const string &dbPath,const vector<string> &vColumnFamilyName)
    {
        
        _options.create_if_missing = true;
        _options.create_missing_column_families=true;
        _options.keep_log_file_num=15;
        _dbPath=dbPath;

        std::vector<ColumnFamilyDescriptor> column_families;
        std::vector<ColumnFamilyHandle*> handles;

        vector<string> vCFName;
        DB::ListColumnFamilies(_options,_dbPath,&vCFName);
        for (auto cf:vCFName)
        {
            cout<<"cfname="<<cf<<endl;
            column_families.push_back(ColumnFamilyDescriptor(cf, ColumnFamilyOptions(_options)));
        }
        if (column_families.empty())
        {
            column_families.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions(_options)));
        }
        
        
    
        Status s = DB::Open(_options, dbPath,column_families,&handles, &_db);
        cout<<"status:"<<s.ToString()<<endl;
        assert(s.ok());

        for (size_t i = 0; i < handles.size(); i++)
        {
            cout<<"handles name:"<<handles[i]->GetName()<<endl;
            _mhandles[handles[i]->GetName()]= handles[i]; 

        }

        for (auto newcf :vColumnFamilyName)
        {
            if(std::find(vCFName.begin(),vCFName.end(),newcf)==vCFName.end())
            {
                ColumnFamilyHandle* cf;
                auto s = _db->CreateColumnFamily(ColumnFamilyOptions(_options), newcf, &cf);
                cout<<"newcf:"<<newcf<<",status:"<<s.ToString()<<endl;
                assert(s.ok());   

                _mhandles[newcf]= cf; 

            }
        }
        

    }

    DBBase::~ DBBase()
    {
        // drop column family
    //   auto s = _db->DropColumnFamily(_handles[0]);
    //   assert(s.ok());

    //   // close db
    //   for (auto handle : _handles) {
    //    auto s = _db->DestroyColumnFamilyHandle(handle);
    //     assert(s.ok());
    //   }
    //   delete _db;
    }

    bool DBBase::ShowDB(vector<string> &vDBName )
    {
        for (auto &item :_mhandles)
        {
            vDBName.push_back(item.first);
        }

        return true;
        
    }

    bool DBBase::Create(const string&dbname,const map<string,string> &msession)
    {
        if (!DBExist(dbname))
        {
            ColumnFamilyHandle* cf;
            auto s = _db->CreateColumnFamily(ColumnFamilyOptions(), dbname, &cf);
            cout<<"CreateColumnFamily status:"<<s.ToString()<<endl;
            assert(s.ok());   

            _mhandles[dbname]= cf;
        }
        
         
        return true;

    }

    bool DBBase::DBExist(const string& dbname)
    {
        bool bDBExist= _mhandles.find(dbname)!=_mhandles.end();
        if (!bDBExist)
        {
            cout<<"DB not Exist,dbname="<<dbname<<endl;
        }
        
        return bDBExist;
    }


    bool DBBase::Put(const string&key,const string &value,const string& db,const map<string,string> &msession)
    {
        if (!DBExist(db))
        {
            return false;
        }
        auto s = _db->Put(_WriteOptions, _mhandles[db], Slice(key), Slice(value));
        cout<<"Put status:"<<s.ToString()<<endl;

        return s.ok();

    }
    bool DBBase::Put2WriteBatch(rocksdb::WriteBatch &updates,const string&key,const string &value,const string& db,const map<string,string> &msession)
    {
        if (!DBExist(db))
        {
            return false;
        }
        auto s = updates.Put(_mhandles[db], Slice(key), Slice(value));
        cout<<"Put status:"<<s.ToString()<<endl;

        return s.ok();

    }

    bool DBBase::WriteBatch(rocksdb::WriteBatch &updates,const string& db,const map<string,string> &msession)
    {
        if (!DBExist(db))
        {
            return false;
        }
        auto s = _db->Write(_WriteOptions,  &updates);
        cout<<"Write status:"<<s.ToString()<<endl;

        return s.ok();

    }



    bool DBBase::Get(const string&key, string &value,const string& db)
    {
        if (!DBExist(db))
        {
            return false;
        }
        auto s = _db->Get(_readOptions, _mhandles[db], Slice(key), &value);
        cout<<"Get status:"<<s.ToString()<<",key:" << key<<",value:"<<value<<endl;
         return s.ok();

    }

    rocksdb::Status DBBase::GetS(const string&key, string &value,const string& db)
    {
        Status tStatus;
        if (!DBExist(db))
        {
            return rocksdb::Status::NotFound("db not exist");
        }

        tStatus = _db->Get(_readOptions, _mhandles[db], Slice(key), &value);
        cout<<"Get status:"<<tStatus.ToString()<<",key:" << key<<",value:"<<value<<endl;
        return tStatus;

    }


    bool DBBase::KeyMayExist(const string&key, string &value,const string& db)
    {
        if (!DBExist(db))
        {
            return false;
        }
        bool isExist= _db->KeyMayExist(_readOptions, _mhandles[db], Slice(key), &value);
        cout<<"key:" << key<<",isExist:"<<isExist<<",value:"<<value<<endl;
        return isExist;

    }

    bool DBBase::GetFirstKV( string&key, string &value,const string& dbname)
    {
        if (!DBExist(dbname))
        {
            return false;
        }
         cout << " dbname:" << dbname<<endl;

        std::unique_ptr<Iterator> iter(_db->NewIterator(_readOptions,_mhandles[dbname]));
        Status st = iter->status();
        if (!st.ok()) 
        {
            cout<<"Iterator error." << st.ToString()<<endl;
            return false;
        }

        iter->SeekToFirst();
        if (iter->Valid())
        {
            key.assign(iter->key().data(),iter->key().size());
            value.assign(iter->value().data(),iter->value().size());
            return true;
        }


        return false;
        
    }
    bool DBBase::GetLastKV( string&key, string &value,const string& dbname)
    {
        if (!DBExist(dbname))
        {
            return false;
        }
         cout << " dbname:" << dbname<<endl;

        std::unique_ptr<Iterator> iter(_db->NewIterator(_readOptions,_mhandles[dbname]));
        Status st = iter->status();
        if (!st.ok()) 
        {
            cout<<"Iterator error." << st.ToString()<<endl;
            return false;
        }

        iter->SeekToLast();
        if (iter->Valid())
        {
            key.assign(iter->key().data(),iter->key().size());
            value.assign(iter->value().data(),iter->value().size());
            return true;
        }
        

        return false;
        
    }

    bool DBBase::DeleteRange(const string& begin_key, const string& end_key,const string& dbname,const map<string,string> &msession)
    {
        Status st = _db->DeleteRange(_WriteOptions,_mhandles[dbname],begin_key,end_key);
        return st.ok();
    }
    bool DBBase::Delete(const string& key, const string& dbname,const map<string,string> &msession)
    {
        Status st = _db->Delete(_WriteOptions,_mhandles[dbname],key);
        return st.ok();
    }


    bool DBBase::PreKeyGet(const string&prekey, vector<string> &vValue,const string& dbname,vector<string> &vKey)
    {
        if (!DBExist(dbname))
        {
            return false;
        }
         cout <<" prekey:" << prekey<< ", dbname:" << dbname<<endl;
         _readOptions.total_order_seek = true;
        //  uint64_t count=0;
        //  _db->GetIntProperty(_mhandles[dbname], "rocksdb.estimate-num-keys",&count) ;
        //  cout<<"count=" << count <<endl;

        std::unique_ptr<Iterator> iter(_db->NewIterator(_readOptions,_mhandles[dbname]));
        Status st = iter->status();
        if (!st.ok()) 
        {
            cout<<"Iterator error." << st.ToString()<<endl;
            return false;
        }
        // std::string stats;
        // bool pStats=false;
        // if (pStats && _db->GetProperty("rocksdb.stats", &stats)) {
        //     cout <<stats<<endl;
        // }

        for (iter->Seek(prekey); iter->Valid() && iter->key().starts_with(prekey) ; iter->Next()) 
        {   
            cout << iter->key().ToString()<<"," << iter->value().ToString() << endl;

            vValue.push_back(iter->value().ToString());
            vKey.push_back(iter->key().ToString());
            if (vValue.size()==10000)
            {
                break;
            }
            
        }

        return true;
        
    }

    bool DBBase::PreKeyGetFirst(const string&prekey, string &firstKey, string &firstValue,const string& dbname)
    {
        if (!DBExist(dbname))
        {
            return false;
        }
         cout <<" prekey:" << prekey<< ", dbname:" << dbname<<endl;
         _readOptions.total_order_seek = true;
        //  uint64_t count=0;
        //  _db->GetIntProperty(_mhandles[dbname], "rocksdb.estimate-num-keys",&count) ;
        //  cout<<"count=" << count <<endl;

        std::unique_ptr<Iterator> iter(_db->NewIterator(_readOptions,_mhandles[dbname]));
        Status st = iter->status();
        if (!st.ok()) 
        {
            cout<<"Iterator error." << st.ToString()<<endl;
            return false;
        }
        // std::string stats;
        // bool pStats=false;
        // if (pStats && _db->GetProperty("rocksdb.stats", &stats)) {
        //     cout <<stats<<endl;
        // }

        for (iter->Seek(prekey); iter->Valid() && iter->key().starts_with(prekey) ; iter->Next()) 
        {   
            cout << iter->key().ToString()<<"," << iter->value().ToString() << endl;

            firstValue=iter->value().ToString();
            firstKey=iter->key().ToString();
            return true;
            
        }

        return false;
        
    }

}

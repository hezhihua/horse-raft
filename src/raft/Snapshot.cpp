
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




#include "raft/Snapshot.h"
#include "raft/Storage.h"
#include "raft/Node.h"
#include "logger/logger.h"

//#define RAFT_SNAPSHOT_PATTERN "snapshot_%020ld"
#define RAFT_SNAPSHOT_PATTERN "snapshot_"        //真正的数据
#define RAFT_SNAPSHOT_META_FILE "__raft_snapshot_meta"     //meta数据 不放文件放db

namespace horsedb {

const char* LocalSnapshotStorage::_s_temp_path = "temp";

LocalSnapshotMetaTable::LocalSnapshotMetaTable() {}

LocalSnapshotMetaTable::~LocalSnapshotMetaTable() {}

int LocalSnapshotMetaTable::add_file(const std::string& filename, const LocalFileMeta& meta) 
{
    Map::value_type value(filename, meta);
    std::pair<Map::iterator, bool> ret = _file_map.insert(value);
    if (!ret.second)
    {
        TLOGINFO_RAFT( "file=" << filename << " already exists in snapshot" <<endl);
    }
            
    return ret.second ? 0 : -1;
}

int LocalSnapshotMetaTable::remove_file(const std::string& filename) {
    Map::iterator iter = _file_map.find(filename);
    if (iter == _file_map.end()) {
        return -1;
    }
    _file_map.erase(iter);
    return 0;
}

int LocalSnapshotMetaTable::save_to_file(FileSystemAdaptor* fs, const std::string& path) const {

    return 0;
    
}

int LocalSnapshotMetaTable::save_to_db() const 
{
    LocalSnapshotMeta lcMeta;
    if (_meta.lastIncludedTerm>0) 
    {
        lcMeta.meta = _meta;
    }
    for (Map::const_iterator iter = _file_map.begin(); iter != _file_map.end(); ++iter) 
    {
        lcMeta.vFiles.push_back(iter->second);
    }
    
    string sKey= getSnapshotMetaKey();
    horsedb::TarsOutputStream<horsedb::BufferWriterVector> os;
    lcMeta.writeTo(os);
    string value(os.getBuffer(), os.getLength());  

    if (!_dbbase->Put(sKey,value,"default"))
    {
        TLOGINFO_RAFT("Fail to save meta to db" <<endl);
        return -1;
    }

    return 0;
}

int LocalSnapshotMetaTable::load_from_db()  
{
    

    string value;
    string sKey=getSnapshotMetaKey();
    if (_dbbase->Get(sKey,value,"default"))
    {
        LocalSnapshotMeta lcMeta;
        vector<char> valueBuff;
        valueBuff.assign(value.begin(),value.end());

        horsedb::TarsInputStream<horsedb::BufferReader> _is;
        _is.setBuffer(valueBuff);
        lcMeta.readFrom(_is);

        _meta=lcMeta.meta;
        for (size_t i = 0; i < lcMeta.vFiles.size(); i++)
        {
           _file_map[lcMeta.vFiles[i].name]=lcMeta.vFiles[i];
        }
        
    }

    return 0;
}





int LocalSnapshotMetaTable::load_from_file(FileSystemAdaptor* fs, const std::string& path) 
{
    return 0;
}



void LocalSnapshotMetaTable::list_files(std::vector<std::string>* files) const 
{
    if (!files) 
    {
        return;
    }
    files->clear();
    files->reserve(_file_map.size());
    for (Map::const_iterator iter = _file_map.begin(); iter != _file_map.end(); ++iter) 
    {
        files->push_back(iter->first);
    }
}

int LocalSnapshotMetaTable::get_file_meta(const std::string& filename, LocalFileMeta* file_meta) const 
{
    Map::const_iterator iter = _file_map.find(filename);
    if (iter == _file_map.end()) 
    {
        return -1;
    }
    if (file_meta) 
    {
        *file_meta = iter->second;
    }
    return 0;
}



LocalSnapshotWriter::LocalSnapshotWriter(const std::string& path,FileSystemAdaptor* fs): _path(path), _fs(fs) 
{
}

LocalSnapshotWriter::~LocalSnapshotWriter() 
{
}

int LocalSnapshotWriter::init() 
{

    if (!_fs->create_directory(_path,NULL, false)) 
    {
        TLOGERROR_RAFT("CreateDirectory failed, path: "<< _path<<endl);
        return EIO;
    }

    if ( _meta_table.load_from_db() != 0) 
    {
        TLOGERROR_RAFT( "Fail to load metatable from db"<<endl);
        return EIO;
    }

    // remove file if meta_path not exist or it's not in _meta_table 
    // to avoid dirty data
    {
         std::vector<std::string> to_remove;
         DirReader* dir_reader = _fs->directory_reader(_path);
         if (!dir_reader->is_valid()) 
         {
             TLOGERROR_RAFT( "directory reader failed, maybe NOEXIST or PERMISSION,"
                 " path: " << _path<<endl);
             delete dir_reader;
             return EIO;
         }
         while (dir_reader->next()) 
         {
             std::string filename = dir_reader->name();
             if (filename != RAFT_SNAPSHOT_META_FILE) 
             {
                 if (get_file_meta(filename, NULL) != 0) 
                 {
                     to_remove.push_back(filename);
                 }
             }
         }
         delete dir_reader;
         for (size_t i = 0; i < to_remove.size(); ++i) 
         {
             std::string file_path = _path + "/" + to_remove[i];
             _fs->delete_file(file_path, false);
             TLOGERROR_RAFT( "Snapshot file exist but meta not found so delete it,"
                 " path: " << file_path<<endl);
         }
    }

    return 0;
}

int64_t LocalSnapshotWriter::snapshot_index() 
{
    return _meta_table.has_meta() ? _meta_table.meta().lastIncludedIndex : 0;
}

int LocalSnapshotWriter::remove_file(const std::string& filename) 
{
    return _meta_table.remove_file(filename);
}

int LocalSnapshotWriter::add_file( const std::string& filename,  const LocalFileMeta& file_meta) 
{
    
    return _meta_table.add_file(filename, file_meta);
}

void LocalSnapshotWriter::list_files(std::vector<std::string> *files) 
{
    return _meta_table.list_files(files);
}

int LocalSnapshotWriter::get_file_meta(const std::string& filename, LocalFileMeta* meta ) 
{

    return _meta_table.get_file_meta(filename, meta);
}

int LocalSnapshotWriter::save_meta(const SnapshotMeta& meta) 
{
    _meta_table.set_meta(meta);
    return 0;
}

int LocalSnapshotWriter::sync() 
{
    const int rc = _meta_table.save_to_db();
    if (rc != 0 ) 
    {
       TLOGERROR_RAFT("Fail to sync, path: " << endl);
        
    }
    return rc;
}

LocalSnapshotReader::LocalSnapshotReader(const std::string& path,
                                         horsedb::EndpointInfo server_addr,
                                         FileSystemAdaptor* fs)
    : _path(path)
    , _addr(server_addr)
    , _reader_id(0)
    , _fs(fs)
    
{}

LocalSnapshotReader::~LocalSnapshotReader() {
    destroy_reader_in_file_service();
}

int LocalSnapshotReader::init() 
{
    if (!_fs->directory_exists(_path)) 
    {
        TLOGERROR_RAFT("Not such _path : "+ _path<<endl);
        return ENOENT;
    }
    std::string meta_path = _path + string("/")+ RAFT_SNAPSHOT_META_FILE;
    if (_meta_table.load_from_file(_fs.get(), meta_path) != 0) //todo
    {
        TLOGERROR_RAFT("Fail to load meta"<<endl);
        return EIO;
    }
    return 0;
}

int LocalSnapshotReader::load_meta(SnapshotMeta* meta) 
{
    if (!_meta_table.has_meta()) 
    {
        return -1;
    }
    *meta = _meta_table.meta();
    return 0;
}

int64_t LocalSnapshotReader::snapshot_index() {
    
}

void LocalSnapshotReader::list_files(std::vector<std::string> *files) 
{
    return _meta_table.list_files(files);
}

int LocalSnapshotReader::get_file_meta(const std::string& filename,  LocalFileMeta* file_meta) 
{
    
    return _meta_table.get_file_meta(filename, file_meta);
}


std::string LocalSnapshotReader::generate_uri_for_copy() 
{
    return "";    
}

void LocalSnapshotReader::destroy_reader_in_file_service() {
    
}

LocalSnapshotStorage::LocalSnapshotStorage(const std::string& path)
    : _path(path)
    , _last_snapshot_index(0)
{}

LocalSnapshotStorage::~LocalSnapshotStorage() {
}

int LocalSnapshotStorage::init() 
{
   
    if (_fs == NULL) 
    {
        _fs=shared_ptr<FileSystemAdaptor>(default_file_system());
    }
    if (!_fs->create_directory(_path,NULL, true)) 
    {
        TLOGERROR_RAFT( "Fail to create " << _path << endl);
        return -1;
    }
    // delete temp snapshot
    if (!_filter_before_copy_remote) 
    {
        std::string temp_snapshot_path(_path);
        temp_snapshot_path.append("/");
        temp_snapshot_path.append(_s_temp_path);
        TLOGINFO_RAFT( "Deleting " << temp_snapshot_path<<endl);
        if (!_fs->delete_file(temp_snapshot_path, true)) 
        {
            TLOGWARN_RAFT( "delete temp snapshot path failed, path " << temp_snapshot_path<<endl);
            return EIO;
        }
    }

    // delete old snapshot
    DirReader* dir_reader = _fs->directory_reader(_path);
    if (!dir_reader->is_valid()) 
    {
        TLOGWARN_RAFT( "directory reader failed, maybe NOEXIST or PERMISSION. path: " << _path<<endl);
        delete dir_reader;
        return EIO;
    }
    std::set<int64_t> snapshots;
    while (dir_reader->next()) 
    {
        int64_t index = 0;
        int match = sscanf(dir_reader->name(), RAFT_SNAPSHOT_PATTERN, &index);
        if (match == 1) 
        {
            snapshots.insert(index);
        }
    }
    delete dir_reader;

    // TODO: add snapshot watcher

    // get last_snapshot_index
    if (snapshots.size() > 0) 
    {
        size_t snapshot_count = snapshots.size();
        for (size_t i = 0; i < snapshot_count - 1; i++) 
        {
            int64_t index = *snapshots.begin();
            snapshots.erase(index);

            std::string snapshot_path;
            snapshot_path=_path+ string("/" ) +RAFT_SNAPSHOT_PATTERN + TC_Common::tostr(index) ;
            TLOGINFO_RAFT(  "Deleting snapshot `" << snapshot_path << "'"<<endl);
            // TODO: Notify Watcher before delete directories.
            if (!_fs->delete_file(snapshot_path, true)) 
            {
                TLOGWARN_RAFT( "delete old snapshot path failed, path " << snapshot_path<<endl);
                return EIO;
            }
        }

        _last_snapshot_index = *snapshots.begin();
        
    }

    return 0;
}



int LocalSnapshotStorage::destroy_snapshot(const std::string& path) 
{
    TLOGINFO_RAFT( "Deleting "  << path<<endl);
    if (!_fs->delete_file(path, true)) 
    {
        TLOGWARN_RAFT(  "delete old snapshot path failed, path " << path<<endl);
        return -1;
    }
    return 0;
}



SnapshotWriter* LocalSnapshotStorage::create() 
{
    return create(true);
}

SnapshotWriter* LocalSnapshotStorage::create(bool from_empty)
 {
    LocalSnapshotWriter* writer = NULL;

    do {
        std::string snapshot_path(_path);
        snapshot_path.append("/");
        snapshot_path.append(_s_temp_path);

        // delete temp
        // TODO: Notify watcher before deleting
        if (_fs->path_exists(snapshot_path) && from_empty) 
        {
            if (destroy_snapshot(snapshot_path) != 0) 
            {
                break;
            }
        }

        writer = new LocalSnapshotWriter(snapshot_path, _fs.get());
        if (writer->init() != 0) 
        {
            TLOGERROR_RAFT(   "Fail to init writer, path: " << snapshot_path<<endl);
            delete writer;
            writer = NULL;
            break;
        }
        TLOGINFO_RAFT(   "Create writer success, path: " << snapshot_path<<endl);
    } while (0);

    return writer;
}


int LocalSnapshotStorage::close() {
    //delete copier;
    return 0;
}

SnapshotReader* LocalSnapshotStorage::copy_from(const std::string& uri) 
{

}

int LocalSnapshotStorage::close(SnapshotWriter* writer) 
{
    return close(writer, false);
}

int LocalSnapshotStorage::close(SnapshotWriter* writer_base,bool keep_data_on_error) 
{
    LocalSnapshotWriter* writer = dynamic_cast<LocalSnapshotWriter*>(writer_base);
    int ret = writer->code();
    do {

        if (0 != ret) 
        {
            break;
        }
        ret = writer->sync();
        if (ret != 0) 
        {
            break;
        }
        int64_t old_index = 0;
        {
            std::unique_lock<std::mutex> lck(_mutex);
            old_index = _last_snapshot_index;
        }
        int64_t new_index = writer->snapshot_index();
        if (new_index == old_index) 
        {
            ret = EEXIST;
            break;
        }

        // rename temp to new
        std::string temp_path(_path);
        temp_path.append("/");
        temp_path.append(_s_temp_path);
        std::string new_path(_path);
        new_path= _path + string("/")+ RAFT_SNAPSHOT_PATTERN+ TC_Common::tostr(new_index) ;

        TLOGINFO_RAFT(  "Deleting " << new_path<<endl);

        if (!_fs->delete_file(new_path, true)) //删除已经存在的
        {
            TLOGWARN_RAFT( "delete new snapshot path failed, path " << new_path);
            ret = EIO;
            break;
        }
        TLOGINFO_RAFT( "Renaming " << temp_path << " to " << new_path <<endl );
        if (!_fs->rename(temp_path, new_path)) 
        {
            TLOGWARN_RAFT ( "rename temp snapshot failed, from_path " << temp_path
                         << " to_path " << new_path <<endl);
            ret = EIO;
            break;
        }


        {
            std::unique_lock<std::mutex> lck(_mutex);
            _last_snapshot_index = new_index;
        }
        // unref old_index, ref new_index

    } while (0);

    if (ret != 0 && !keep_data_on_error) 
    {
        destroy_snapshot(writer->get_path());
    }
    delete writer;
    return ret != EIO ? 0 : -1;
}

SnapshotReader* LocalSnapshotStorage::open() 
{
    std::unique_lock<std::mutex> lck(_mutex);
    if (_last_snapshot_index != 0) 
    {
        const int64_t last_snapshot_index = _last_snapshot_index;

        lck.unlock();

        string  snapshot_path=_path+string("/") + RAFT_SNAPSHOT_PATTERN +  TC_Common::tostr(last_snapshot_index) ;
        LocalSnapshotReader* reader = new LocalSnapshotReader(snapshot_path, _addr, _fs.get());
        if (reader->init() != 0) 
        {

            delete reader;
            return NULL;
        }
        return reader;
    } 
    else
    {
        errno = ENODATA;
        return NULL;
    }
}

int LocalSnapshotStorage::close(SnapshotReader* reader_) 
{
    LocalSnapshotReader* reader = dynamic_cast<LocalSnapshotReader*>(reader_);

    delete reader;
    return 0;
}

int LocalSnapshotStorage::set_filter_before_copy_remote() 
{
    _filter_before_copy_remote = true;
    return 0;
}

int LocalSnapshotStorage::set_file_system_adaptor(FileSystemAdaptor* fs) 
{
    if (fs == NULL) 
    {
        TLOGERROR_RAFT ( "file system is NULL, path: " << _path<<endl);
        return -1;
    }
    _fs =shared_ptr<FileSystemAdaptor>(fs);
    return 0;
}





}  //  namespace raft

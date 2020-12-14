#include "raft/LogManager.h"
#include "logger/logger.h"

namespace horsedb{


LogManagerOptions::LogManagerOptions()
    : log_storage(NULL)
    , configuration_manager(NULL)
    , fsm_caller(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL)
    , _config_manager(NULL)
    , _stopped(false)
    , _has_error(false)
    , _next_wait_id(0)
    , _first_log_index(0)
    , _last_log_index(0)
{
    
}


int LogManager::init(const LogManagerOptions &options) 
{
    std::unique_lock<std::mutex> lck(_mutex);
    if (options.log_storage == NULL) 
    {
        return EINVAL;
    }
   
    _log_storage = options.log_storage;
    _config_manager = options.configuration_manager;
    int ret = _log_storage->init(_config_manager);
    if (ret != 0) {
        return ret;
    }
    _first_log_index = _log_storage->first_log_index();
    _last_log_index = _log_storage->last_log_index();
    _disk_id.index = _last_log_index;
    // Term will be 0 if the node has no logs, and we will correct the value
    // after snapshot load finish.
    _disk_id.term = _log_storage->get_term(_last_log_index);
    _fsm_caller = options.fsm_caller;
    return 0;
}

LogManager::~LogManager() 
{
    
    _logs_in_memory.clear();
}

bool LogManager::check_and_set_configuration(ConfigurationEntry* current) 
{
    if (current == NULL) 
    {
        TLOGERROR_RAFT("current should not be NULL"<<endl);
        return false;
    }
    std::unique_lock<std::mutex> lck(_mutex);

    const ConfigurationEntry& last_conf = _config_manager->last_configuration();
    if (current->id != last_conf.id) 
    {
        *current = last_conf;
        return true;
    }
    return false;
}
bool LogManager::get_entry_from_memory(const int64_t index,LogEntry &entry) 
{
    
    if (!_logs_in_memory.empty()) 
    {
        int64_t first_index = _logs_in_memory.front().index;
        int64_t last_index = _logs_in_memory.back().index;
 
        if (last_index - first_index + 1!= _logs_in_memory.size())
        {
            return false;
        }
        

        if (index >= first_index && index <= last_index) 
        {
            entry = _logs_in_memory[index - first_index];
            return true;
        }
    }
    return false;
}


int64_t LogManager::unsafe_get_term(const int64_t index) 
{
    if (index == 0) 
    {
        return 0;
    }
    
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) 
    {
        return 0;
    }

    LogEntry entry;
    bool bret = get_entry_from_memory(index,entry);
    if (bret) 
    {
        return entry.term;
    }
    
    return _log_storage->get_term(index);
}


int64_t LogManager::get_term(const int64_t index) 
{
    std::unique_lock<std::mutex> lck(_mutex);
    if (index == 0) 
    {
        return 0;
    }
    
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) 
    {
        return 0;
    }

    LogEntry entry;
    bool bret = get_entry_from_memory(index,entry);
    if (bret) 
    {
        return entry.term;
    }
    
    return _log_storage->get_term(index);
}

//清理内存小于等于 id的logentry
void LogManager::clear_memory_logs(const LogId& id) 
{

    do {
             
        while (!_logs_in_memory.empty() ) 
        {
            LogEntry& entry = _logs_in_memory.front();
            LogId tLogId(entry.index,entry.term);
            if (tLogId > id) 
            {
                break;
            }
            _logs_in_memory.pop_front();
        }
        
        
    } while (0);
}

void LogManager::set_disk_id(const LogId& disk_id) 
{
    std::unique_lock<std::mutex> lck(_mutex);
    if (disk_id < _disk_id) {
        return;
    }
    _disk_id = disk_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    
    return clear_memory_logs(clear_id);
}


    //从尾部开始truncate   比last_index_kept 大的 log
    void LogManager::unsafe_truncate_suffix(const int64_t last_index_kept) 
    {

        LogId last_id = _disk_id;

        if (last_index_kept < _applied_id.index) 
        {
            TLOGERROR_RAFT( "Can't truncate logs before _applied_id=" <<_applied_id.index
                    << ", last_log_kept=" << last_index_kept <<endl);
            return;
        }

        while (!_logs_in_memory.empty()) 
        {
            LogEntry& entry = _logs_in_memory.back();
            if (entry.index > last_index_kept) 
            {
                _logs_in_memory.pop_back();//内存
            } 
            else 
            {
                break;
            }
        }
        _last_log_index = last_index_kept;
        const int64_t last_term_kept = unsafe_get_term(last_index_kept);

        
        TLOGINFO_RAFT( "last_index_kept=" << last_index_kept
                    << ", last_term_kept=" << last_term_kept <<endl);

        _config_manager->truncate_suffix(last_index_kept);

        int ret = _log_storage->truncate_suffix(last_index_kept);//todo 异步处理
        if (ret == 0) 
        {
            // update last_id after truncate_suffix

            last_id.index = last_index_kept;
            last_id.term = last_term_kept;

            set_disk_id(last_id);
            
            TLOGINFO_RAFT( "last_id=" << last_id <<endl);
        }
}



    int LogManager::check_and_resolve_conflict(std::vector<LogEntry> &entries)
     {
    
        if (entries.front().index == 0) 
        {
            // Node is currently the leader and |entries| are from the user who 
            // don't know the correct indexes the logs should assign to. So we have
            // to assign indexes to the appending entries
            for (size_t i = 0; i < entries.size(); ++i) 
            {
                entries[i].index = ++_last_log_index;
            }
            return 0;
            
        } 
        else 
        {
            // Node is currently a follower and |entries| are from the leader. We 
            // should check and resolve the confliction between the local logs and
            // |entries|
            if (entries.front().index > _last_log_index + 1) 
            {
                TLOGERROR("There's gap between first_index=" + TC_Common::tostr(entries.front().index)  +" and last_log_index="+TC_Common::tostr(_last_log_index)<<endl);
                                        
                return -1;
            }
            const int64_t applied_index = _applied_id.index;
            if (entries.back().index <= applied_index) 
            {
                TLOGERROR( "Received entries of which the last_log="
                            << entries.back().index
                            << " is not greater than _applied_index=" << applied_index
                            << ", return immediately with nothing changed"<<endl );
                return 1;
            }

            if (entries.front().index == _last_log_index + 1) 
            {
                // Fast path
                _last_log_index = entries.back().index;
            } 
            else
            {
                //-------------|entries.front().index
                //------------------------|_last_log_index
                //  entries.front().index <= _last_log_index
                // Appending entries overlap the local ones. We should find if there
                // is a conflicting index from which we should truncate the local
                // ones.
                size_t conflicting_index = 0;
                for (; conflicting_index < entries.size(); ++conflicting_index) //检查follower本地的日志的任期是否与leader一致
                {
                    if (unsafe_get_term(entries[conflicting_index].index) != entries[conflicting_index].term) 
                    {
                        break;
                    }
                }
                if (conflicting_index != entries.size()) 
                {
                    if (entries[conflicting_index].index <= _last_log_index) //entries[conflicting_index].index 大于_last_log_index 表示是新的,本地并不存在
                    {
                        // Truncate all the conflicting entries to make local logs
                        // consensus with the leader.
                        //truncate 掉本地跟Leader不一致的log(大于entries[conflicting_index].index - 1) 的log
                        unsafe_truncate_suffix( entries[conflicting_index].index - 1);
                    }
                    _last_log_index = entries.back().index;
                }  // else this is a duplicated AppendEntriesRequest, we have 
                // nothing to do besides releasing all the entries
                
                // Release all the entries before the conflicting_index and the rest
                // would be append to _logs_in_memory and _log_storage after this
                // function returns
                
                entries.erase(entries.begin(), entries.begin() + conflicting_index);
            }
            
            return 0;
        }

        return -1;
}



    void LogManager::append_entries( std::vector<LogEntry> &entries) 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (!entries.empty() && check_and_resolve_conflict(entries) != 0) 
        {
            
            entries.clear();
            return;
        }

        for (size_t i = 0; i < entries.size(); ++i) 
        {
            
            if (entries[i].cmdType == CM_Config) 
            {
                ConfigurationEntry conf_entry(entries[i]);
                _config_manager->add(conf_entry);
            }
        }

        if (!entries.empty()) 
        {
            _logs_in_memory.insert(_logs_in_memory.end(), entries.begin(), entries.end());
        }

        //落地

        _log_storage->append_entries(entries);
        
    }

    RaftState LogManager::check_consistency() 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        
        TLOGINFO_RAFT( "_first_log_index="<<_first_log_index << ",_last_log_index="<<_last_log_index<<endl)


        if (_last_snapshot_id == LogId(0, 0)) 
        {
            if (_first_log_index == 1) 
            {
                return RaftState(EOK,"ok");
            }
            return RaftState(EIO, "Missing logs in (0, " +  TC_Common::tostr(_first_log_index)  +")");
        } 
        else 
        {
            if (_last_snapshot_id.index >= _first_log_index - 1 && _last_snapshot_id.index <= _last_log_index) 
            {
                return  RaftState(EOK,"ok");
            }

            string msg="There's a gap between snapshot={" + TC_Common::tostr(_last_snapshot_id.index) +","+TC_Common::tostr(_last_snapshot_id.term)+"}and log=["
                       +TC_Common::tostr(_first_log_index) +","+TC_Common::tostr(_last_log_index)+"]";

             return RaftState(EIO, msg);
        }

    }

    bool LogManager::get_entry(const int64_t index,LogEntry &entry) 
    {
        std::unique_lock<std::mutex> lck(_mutex);

        // out of range, direct return NULL
        if (index > _last_log_index || index < _first_log_index) 
        {
            return false;
        }

        bool bExist = get_entry_from_memory(index,entry);
        if (bExist) 
        {
            
            return true;
        }
        lck.unlock();

        bExist = _log_storage->get_entry(index,entry);
        if (!bExist) 
        {
            TLOGERROR_RAFT(EIO<<"Corrupted entry at index="<< index<<endl);
        }
        return bExist;
    }

    void LogManager::get_configuration(const int64_t index, ConfigurationEntry* conf) 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        return _config_manager->get(index, conf);
    }


    int64_t LogManager::last_log_index(bool is_flush) 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (!is_flush) 
        {
            return _last_log_index;
        } 
        else 
        {
            if (_last_log_index == _last_snapshot_id.index) {
                return _last_log_index;
            }
            
            return _disk_id.index;
        }
    }

    LogId LogManager::last_log_id(bool is_flush) 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        if (!is_flush) 
        {
            if (_last_log_index >= _first_log_index) 
            {
                return LogId(_last_log_index, unsafe_get_term(_last_log_index));
            }
            return _last_snapshot_id;
        } 
        else 
        {
            if (_last_log_index == _last_snapshot_id.index) {
                return _last_snapshot_id;
            }
            
            return _disk_id;
        }
    }

    int64_t LogManager::first_log_index() 
    {
        std::unique_lock<std::mutex> lck(_mutex);
        return _first_log_index;
    }
}
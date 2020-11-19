#ifndef _HORSEDB_CONFIG__
#define _HORSEDB_CONFIG__

#include <memory>
#include <string>

#include "yaml-cpp/yaml.h"
#include "util/tc_singleton.h"
using namespace std;

#define G_GroupID Config::getInstance()->groupID()
#define G_LogStorageType Config::getInstance()->logStorageType()

namespace horsedb{

class Config: public TC_Singleton<Config>{

    public:

    void init(const string& cfgPath)
    {
        _cfgPath=cfgPath;
        _config=YAML::LoadFile(cfgPath);

    }

    string groupID()
    {
        return _config["raft"]["groupid"].as<string>();
    }
    string logStorageType()
    {
        return _config["raft"]["storage_type"].as<string>();
    }

    const YAML::Node& getConfig() const { return _config;}


    
    YAML::Node _config ;

    string _cfgPath;


};    




}


#endif



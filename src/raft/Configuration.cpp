
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



#include "raft/Configuration.h"
#include "logger/logger.h"
namespace horsedb {

std::ostream& operator<<(std::ostream& os, const Configuration& a) 
{
    std::vector<PeerId> peers;
    a.list_peers(&peers);
    for (size_t i = 0; i < peers.size(); i++) 
    {
        os << peers[i];
        if (i < peers.size() - 1) 
        {
            os << ",";
        }
    }
    return os;
}

int Configuration::parse_from(const string& sEndpoints) 
{
    reset();
    vector<string>  vEndpoints    = TC_Common::sepstr<string>(sEndpoints, ":");

    for (size_t i = 0; i < vEndpoints.size(); ++i)
    {
        try
        {
            PeerId peer;
            peer.parse(vEndpoints[i]);
            add_peer(peer);
        }
        catch (...)
        {
            TLOGERROR_RAFT(" parse error: endpoint:" << vEndpoints[i] << "]" << endl);
            return -1;
        }
    }

    return 0;
}

int ConfigurationManager::add(const ConfigurationEntry& entry) {
    if (!_configurations.empty()) 
    {
        if (_configurations.back().id.index >= entry.id.index) 
        {
            TLOGERROR_RAFT( "Did you forget to call truncate_suffix before "
                            " the last log index goes back"<< endl);
            return -1;
        }
    }
    _configurations.push_back(entry);
    return 0;
}

void ConfigurationManager::truncate_prefix(const int64_t first_index_kept) 
{
    while (!_configurations.empty()&& _configurations.front().id.index < first_index_kept) 
    {
        _configurations.pop_front();
    }
}

void ConfigurationManager::truncate_suffix(const int64_t last_index_kept) 
{
    while (!_configurations.empty() && _configurations.back().id.index > last_index_kept) 
    {
        _configurations.pop_back();
    }
}

void ConfigurationManager::set_snapshot(const ConfigurationEntry& entry) 
{
    TLOGINFO_RAFT( "entry.id "<<entry.id<<" ,_snapshot.id "<<_snapshot.id<< endl);
    _snapshot = entry;
}

void ConfigurationManager::get(int64_t last_included_index,ConfigurationEntry* conf) 
{
    if (_configurations.empty()) 
    {
        TLOGINFO_RAFT( "last_included_index "<<last_included_index<<" ,_snapshot.id.index "<<_snapshot.id.index<< endl);
        *conf = _snapshot;
        return;
    }
    std::deque<ConfigurationEntry>::iterator it;
    for (it = _configurations.begin(); it != _configurations.end(); ++it) 
    {
        if (it->id.index > last_included_index) 
        {
            break;
        }
    }
    if (it == _configurations.begin()) {
        *conf = _snapshot;
        return;
    }
    --it;
    *conf = *it;
}

const ConfigurationEntry& ConfigurationManager::last_configuration() const 
{
    if (!_configurations.empty()) 
    {
        return _configurations.back();
    }
    return _snapshot;
}


}

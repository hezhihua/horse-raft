
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



#ifndef _RAFT_CONFIGURATION_H
#define _RAFT_CONFIGURATION_H

#include <string>
#include <ostream>
#include <vector>
#include <set>
#include <map>

#include "client/EndpointInfo.h"
#include "raft/LogEntryContext.h"

namespace horsedb {

typedef std::string GroupId;
// GroupId with version, format: {group_id}_{index}
typedef std::string VersionedGroupId;

// Represent a participant in a replicating group.
struct PeerId {
    EndpointInfo _addr; // ip port.eg:tcp -h 0.0.0.0 -p 8085
    int _idx; // idx in same addr, default 0

    PeerId() : _idx(0) {}
    explicit PeerId(const EndpointInfo &addr) : _addr(addr), _idx(0) {}
    PeerId(const EndpointInfo &addr, int idx) : _addr(addr), _idx(idx) {}
    PeerId(const std::string& str) 
    { 
        string sResultStr;
        vector<string> vIPInfo=TC_Common::sepstr<string> (str,":");//0.0.0.0 : 8085
        if (vIPInfo.size()==2)
        {
            sResultStr="tcp -h "+vIPInfo[0]+" -p "+vIPInfo[1];
        }
        else
        {
            sResultStr=str;
        }
        
        
        int ret= parse(sResultStr);
        assert(ret==0) ;
    }
    PeerId(const PeerId& id) : _addr(id._addr), _idx(id._idx) {}

    void reset() {
        _addr.getEndpoint().setHost("");
        _addr.getEndpoint().setPort(0);
        _idx = 0;
    }

    string desc(){return _addr.desc();}

    bool is_empty() const 
    {
        return (_addr.host().empty() && _addr.port() == 0 && _idx == 0);
    }

    int parse(const std::string& str) 
    {
        reset();
        try
        {
            TC_Endpoint ep(str);

            EndpointInfo epi(ep.getHost(), ep.getPort(), ep.getType(), ep.getGrid(), "", ep.getQos(), ep.getWeight(), ep.getWeightType(), ep.getAuthType());
            _addr=epi;
        }
        catch(...)
        {
            std::cerr << "parse error,str="<<str << endl;
            return -1;
        }
        
        
        return 0;
    }

    std::string to_string() const 
    {
        return _addr.cmpDesc();
    }
};

inline bool operator<(const PeerId& id1, const PeerId& id2) 
{
    if (id1._addr < id2._addr) 
    {
        return true;
    } 
    else 
    {
        return id1._addr == id2._addr && id1._idx < id2._idx;
    }
}

inline bool operator==(const PeerId& id1, const PeerId& id2) 
{
    return (id1._addr == id2._addr && id1._idx == id2._idx);
}

inline bool operator!=(const PeerId& id1, const PeerId& id2) 
{
    return (id1._addr != id2._addr || id1._idx != id2._idx);
}

inline std::ostream& operator << (std::ostream& os, const PeerId& id) 
{
    return os << id._addr << ':' << id._idx;
}

struct NodeId 
{
    GroupId _group_id;//组id，可能一个节点有多个组id
    PeerId _peer_id;

    NodeId(const GroupId& group_id, const PeerId& peer_id)
        : _group_id(group_id), _peer_id(peer_id) {}
    std::string to_string() const;
};

inline bool operator<(const NodeId& id1, const NodeId& id2) {
    const int rc = id1._group_id.compare(id2._group_id);
    if (rc < 0) {
        return true;
    } else {
        return rc == 0 && id1._peer_id < id2._peer_id;
    }
}

inline bool operator==(const NodeId& id1, const NodeId& id2) 
{
    return (id1._group_id == id2._group_id && id1._peer_id == id2._peer_id);
}

inline bool operator!=(const NodeId& id1, const NodeId& id2) 
{
    return (id1._group_id != id2._group_id || id1._peer_id != id2._peer_id);
}

inline std::ostream& operator << (std::ostream& os, const NodeId& id) 
{
    return os << id._group_id << ':' << id._peer_id;
}

inline std::string NodeId::to_string() const 
{
    std::ostringstream oss;
    oss << *this;
    return oss.str();
}

// A set of peers.
class Configuration {
public:
    typedef std::set<PeerId>::const_iterator const_iterator;
    // Construct an empty configuration.
    Configuration() {}

    // Construct from peers stored in std::vector.
    explicit Configuration(const std::vector<PeerId>& peers) 
    {
        for (size_t i = 0; i < peers.size(); i++) 
        {
            _peers.insert(peers[i]);
        }
    }

    // Construct from peers stored in std::set
    explicit Configuration(const std::set<PeerId>& peers) : _peers(peers) {}

    // Assign from peers stored in std::vector
    void operator=(const std::vector<PeerId>& peers) 
    {
        _peers.clear();
        for (size_t i = 0; i < peers.size(); i++)
        {
            _peers.insert(peers[i]);
        }
    }

    // Assign from peers stored in std::set
    void operator=(const std::set<PeerId>& peers) 
    {
        _peers = peers;
    }

    // Remove all peers.
    void reset() { _peers.clear(); }

    bool empty() const { return _peers.empty(); }
    size_t size() const { return _peers.size(); }

    const_iterator begin() const { return _peers.begin(); }
    const_iterator end() const { return _peers.end(); }

    // Clear the container and put peers in. 
    void list_peers(std::set<PeerId>* peers) const 
    {
        peers->clear();
        *peers = _peers;
    }
    void list_peers(std::vector<PeerId>* peers) const 
    {
        peers->clear();
        peers->reserve(_peers.size());
        peers->insert(peers->begin(),_peers.begin(),_peers.end());
        // std::set<PeerId>::iterator it;
        // for (it = _peers.begin(); it != _peers.end(); ++it) {
        //     peers->push_back(*it);
        // }
    }

    void append_peers(std::set<PeerId>* peers) 
    {
        peers->insert(_peers.begin(), _peers.end());
    }

    // Add a peer.
    // Returns true if the peer is newly added.
    bool add_peer(const PeerId& peer) 
    {
        return _peers.insert(peer).second;
    }

    // Remove a peer.
    // Returns true if the peer is removed.
    bool remove_peer(const PeerId& peer) 
    {
        return _peers.erase(peer);
    }

    // True if the peer exists.
    bool contains(const PeerId& peer_id) const 
    {
        return _peers.find(peer_id) != _peers.end();
    }

    // True if ALL peers exist.
    bool contains(const std::vector<PeerId>& peers) const 
    {
        for (size_t i = 0; i < peers.size(); i++) 
        {
            if (_peers.find(peers[i]) == _peers.end()) 
            {
                return false;
            }
        }
        return true;
    }

    // True if peers are same.
    bool equals(const std::vector<PeerId>& peers) const 
    {
        std::set<PeerId> peer_set;
        for (size_t i = 0; i < peers.size(); i++) 
        {
            if (_peers.find(peers[i]) == _peers.end()) 
            {
                return false;
            }
            peer_set.insert(peers[i]);
        }
        return peer_set.size() == _peers.size();
    }

    bool equals(const Configuration& rhs) const 
    {
        if (size() != rhs.size()) 
        {
            return false;
        }
        // The cost of the following routine is O(nlogn), which is not the best
        // approach.
        for (const_iterator iter = begin(); iter != end(); ++iter) 
        {
            if (!rhs.contains(*iter)) 
            {
                return false;
            }
        }
        return true;
    }
    
    // Get the difference between |*this| and |rhs|
    // |included| would be assigned to |*this| - |rhs|
    // |excluded| would be assigned to |rhs| - |*this|
    void diffs(const Configuration& rhs,Configuration* included,Configuration* excluded) const 
    {
        *included = *this;
        *excluded = rhs;
        for (std::set<PeerId>::const_iterator iter = _peers.begin(); iter != _peers.end(); ++iter) 
        {
            excluded->_peers.erase(*iter);
        }
        for (std::set<PeerId>::const_iterator  iter = rhs._peers.begin(); iter != rhs._peers.end(); ++iter) 
        {
            included->_peers.erase(*iter);
        }
    }

    // Parse Configuration from a string into |this|
    // Returns 0 on success, -1 otherwise
    int parse_from(const string& sEndpoints);
    
private:
    std::set<PeerId> _peers;

};

std::ostream& operator<<(std::ostream& os, const Configuration& a);


struct ConfigurationEntry 
{
    LogId id;
    Configuration conf;
    Configuration old_conf;

    ConfigurationEntry() {}
    ConfigurationEntry(const LogEntry& entry) 
    {
        vector<PeerId> peers,oldpeers;
        for(auto &peer:entry.vPeer)
        {
            peers.push_back(PeerId(peer));

        }
        conf = peers;
        for(auto &peer:entry.vOldPeer)
        {
            oldpeers.push_back(PeerId(peer));

        }
        
        old_conf = oldpeers;
        
    }

    bool stable() const { return old_conf.empty(); }
    bool empty() const { return conf.empty(); }
    void list_peers(std::set<PeerId>* peers) 
    {
        peers->clear();
        conf.append_peers(peers);
        old_conf.append_peers(peers);
    }
    bool contains(const PeerId& peer) const
    { return conf.contains(peer) || old_conf.contains(peer); }
};

// Manager the history of configuration changing
class ConfigurationManager {
public:
    ConfigurationManager() {}
    ~ConfigurationManager() {}

    // add new configuration at index
    int add(const ConfigurationEntry& entry);

    // [1, first_index_kept) are being discarded
    void truncate_prefix(int64_t first_index_kept);

    // (last_index_kept, infinity) are being discarded
    void truncate_suffix(int64_t last_index_kept);

    void set_snapshot(const ConfigurationEntry& snapshot);

    void get(int64_t last_included_index, ConfigurationEntry* entry);

    const ConfigurationEntry& last_configuration() const;

private:

    std::deque<ConfigurationEntry> _configurations;
    ConfigurationEntry _snapshot;
};

}  //  namespace braft

#endif 

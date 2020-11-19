#include "raft/NodeManager.h"

namespace horsedb {


    bool NodeManager::server_exists(const EndpointInfo &addr) 
    {
        return _addr_set.find(addr) != _addr_set.end();
    }
    bool NodeManager::add(NodeImpl* node)
    {
        if (server_exists(node->_server_id._addr))
        {
            return true;
        }
        _Maps.node_map[node->node_id()]=node;
        _Maps.group_map.insert(GroupMap::value_type( node->_group_id,node));

    }


    NodeImpl* NodeManager::get(const GroupId& group_id, const PeerId& peer_id)
    {
        NodeId nodeid=NodeId(group_id, peer_id);

        auto it=_Maps.node_map.find(nodeid);
        if (it!=_Maps.node_map.end())
        {
            return it->second;
        }

        return NULL;

    }
}
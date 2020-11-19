
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



#ifndef  RAFT_NODE_MANAGER_H
#define  RAFT_NODE_MANAGER_H


#include "raft/Raft.h"
#include "raft/Node.h"
#include "client/EndpointInfo.h"
#include "util/tc_singleton.h"
namespace horsedb {


class NodeManager :public TC_Singleton<NodeManager> {
public:
    

    // add raft node
    bool add(NodeImpl* node);

    // remove raft node
    bool remove(NodeImpl* node);

    // get node by group_id and peer_id
    NodeImpl* get(const GroupId& group_id, const PeerId& peer_id);

    // get all the nodes of |group_id|
    void get_nodes_by_group_id(const GroupId& group_id, std::vector<NodeImpl* >* nodes);

    void get_all_nodes(std::vector<NodeImpl* >* nodes);



    // Return true if |addr| is reachable by a RPC Server
    bool server_exists(const EndpointInfo &addr);

    // Remove the addr from _addr_set when the backing service is destroyed
    void remove_address(const EndpointInfo &addr);

    

    // To make implementation simplicity, we use two maps here, although
    // it works practically with only one GroupMap
    typedef std::map<NodeId,NodeImpl* > NodeMap;//一对一
    typedef std::multimap<GroupId, NodeImpl* > GroupMap;//一对多
    struct Maps {
        NodeMap node_map;
        GroupMap group_map;
    };
    // Functor to modify DBD
    static size_t _add_node(Maps&, const NodeImpl* node);
    static size_t _remove_node(Maps&, const NodeImpl* node);

    Maps _Maps;

    std::mutex _mutex;
    std::set<EndpointInfo> _addr_set;
};

}   //  namespace 

#endif  


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



#ifndef  _RAFT_BALLOT_H
#define  _RAFT_BALLOT_H

#include "raft/Configuration.h"

namespace horsedb {

class Ballot {
public:
    struct PosHint 
    {
        PosHint() : _pos0(-1), _pos1(-1) {}
        int _pos0;//新
        int _pos1;//旧
    };

    Ballot();
    ~Ballot();
    void swap(Ballot& rhs) 
    {
        _peers.swap(rhs._peers);
        std::swap(_quorum, rhs._quorum);
        _old_peers.swap(rhs._old_peers);
        std::swap(_old_quorum, rhs._old_quorum);
    }

    int init(const Configuration& conf, const Configuration* old_conf);
    PosHint grant(const PeerId& peer, PosHint hint);
    void grant(const PeerId& peer);
    bool granted() const { return _quorum <= 0 && _old_quorum <= 0; }
private:
    struct UnfoundPeerId 
    {
        UnfoundPeerId(const PeerId& peer_id) : _peer_id(peer_id), _found(false) {}
        PeerId _peer_id;
        bool _found;
        bool operator==(const PeerId& id) const 
        {
            return _peer_id == id;
        }
    };
    //在peers的pos_hint索引处查找peer
    std::vector<UnfoundPeerId>::iterator find_peer(const PeerId& peer, std::vector<UnfoundPeerId>& peers, int pos_hint) 
    {
        if (pos_hint < 0 || pos_hint >= (int)peers.size()|| peers[pos_hint]._peer_id != peer) 
        {
            for (std::vector<UnfoundPeerId>::iterator iter = peers.begin(); iter != peers.end(); ++iter) 
            {
                if (*iter == peer)
                {
                    return iter;
                }
            }
            return peers.end();
        }
        return peers.begin() + pos_hint;
    }
    std::vector<UnfoundPeerId> _peers;
    int _quorum;//过半数值
    std::vector<UnfoundPeerId> _old_peers;
    int _old_quorum;
};

};

#endif  //BRAFT_BALLOT_H

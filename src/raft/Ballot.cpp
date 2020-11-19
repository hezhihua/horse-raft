// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
// 
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

#include "raft/Ballot.h"

namespace horsedb {

Ballot::Ballot() : _quorum(0), _old_quorum(0) {}
Ballot::~Ballot() {}

int Ballot::init(const Configuration& conf, const Configuration* old_conf) {
    _peers.clear();
    _old_peers.clear();
    _quorum = 0;
    _old_quorum = 0;

    _peers.reserve(conf.size());
    for (Configuration::const_iterator iter = conf.begin(); iter != conf.end(); ++iter) 
    {
        _peers.push_back(*iter);
    }
    _quorum = _peers.size() / 2 + 1;
    if (!old_conf) {
        return 0;
    }
    _old_peers.reserve(old_conf->size());
    for (Configuration::const_iterator iter = old_conf->begin(); iter != old_conf->end(); ++iter)
    {
        _old_peers.push_back(*iter);
    }
    _old_quorum = _old_peers.size() / 2 + 1;
    return 0;
}

Ballot::PosHint Ballot::grant(const PeerId& peer, PosHint hint) 
{
    std::vector<UnfoundPeerId>::iterator iter;
    iter = find_peer(peer, _peers, hint._pos0);
    if (iter != _peers.end()) 
    {
        if (!iter->_found) 
        {
            iter->_found = true;
            --_quorum;
        }
        hint._pos0 = iter - _peers.begin();
    } 
    else 
    {
        hint._pos0 = -1;//找不到这个节点
    }

    if (_old_peers.empty()) 
    {
        hint._pos1 = -1;
        return hint;
    }

    iter = find_peer(peer, _old_peers, hint._pos1);

    if (iter != _old_peers.end()) 
    {
        if (!iter->_found) 
        {
            iter->_found = true;
            --_old_quorum;
        }
        hint._pos1 = iter - _old_peers.begin();
    } else {
        hint._pos1 = -1;
    }

    return hint;
}

void Ballot::grant(const PeerId& peer) 
{
    grant(peer, PosHint());
}

}  // namespace horsedb

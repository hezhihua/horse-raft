
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



#include "raft/BallotBox.h"

#include "raft/FsmCaller.h"
#include "logger/logger.h"
namespace horsedb {

BallotBox::BallotBox()
    : _waiter(NULL)
    , _last_committed_index(0)
    , _pending_index(0)
{
}

BallotBox::~BallotBox() 
{
    clear_pending_tasks();
}

int BallotBox::init(const BallotBoxOptions &options) 
{
    if (options._waiter == NULL ) 
    {
        TLOGERROR_RAFT("waiter is NULL") ;
        return EINVAL;
    }
    _waiter = options._waiter;

    return 0;
}

int BallotBox::commit_at( int64_t first_log_index, int64_t last_log_index, const PeerId& peer) 
{
    //  The cricital section is unacceptable because it 
    // blocks all the other Replicators and LogManagers
    std::unique_lock<std::mutex> lck(_mutex);
    if (_pending_index == 0) 
    {
        return EINVAL;
    }
    if (last_log_index < _pending_index) 
    {
        return 0;
    }
    if (last_log_index >= _pending_index + (int64_t)_pending_meta_queue.size())
     {
        return ERANGE;
    }

    int64_t last_committed_index = 0;
    const int64_t start_at = std::max(_pending_index, first_log_index);
    Ballot::PosHint pos_hint;
    for (int64_t log_index = start_at; log_index <= last_log_index; ++log_index) 
    {
        Ballot& bl = _pending_meta_queue[log_index - _pending_index];
        pos_hint = bl.grant(peer, pos_hint);
        if (bl.granted()) 
        {
            last_committed_index = log_index;
        }
    }

    if (last_committed_index == 0) 
    {
        return 0;
    }

    // When removing a peer off the raft group which contains even number of
    // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
    // this case, the log after removal may be committed before some previous
    // logs, since we use the new configuration to deal the quorum of the
    // removal request, we think it's safe to commit all the uncommitted 
    // previous logs, which is not well proved right now
    // TODO: add vlog when committing previous logs
    for (int64_t index = _pending_index; index <= last_committed_index; ++index) 
    {
        _pending_meta_queue.pop_front();
    }
   
    _pending_index = last_committed_index + 1;
    _last_committed_index.store(last_committed_index, std::memory_order_relaxed);
    lck.unlock();
    // The order doesn't matter
    _waiter->on_committed(last_committed_index);
    return 0;
}

int BallotBox::clear_pending_tasks() 
{
    std::deque<Ballot> saved_meta;
    {
        std::lock_guard<std::mutex> lck(_mutex);
        saved_meta.swap(_pending_meta_queue);
        _pending_index = 0;
    }
    _vContext.clear();
    return 0;
}

int BallotBox::reset_pending_index(int64_t new_pending_index) 
{
    std::lock_guard<std::mutex> lck(_mutex);
    if (!(_pending_index == 0 && _pending_meta_queue.empty()))
    {
        TLOGWARN_RAFT( "pending_index " << _pending_index << ", pending_meta_queue "  << _pending_meta_queue.size()<<endl);
    }
    
    //CHECK(_pending_index == 0 && _pending_meta_queue.empty()) << "pending_index " << _pending_index << " pending_meta_queue "  << _pending_meta_queue.size();
    //CHECK_GT(new_pending_index, _last_committed_index.load( std::memory_order_relaxed));
    if (new_pending_index < _last_committed_index.load( std::memory_order_relaxed))
    {
        TLOGWARN_RAFT( "new_pending_index " << new_pending_index << " ,_last_committed_index "  << _last_committed_index<<endl);
    }
    
    _pending_index = new_pending_index;
    //_closure_queue->reset_first_index(new_pending_index);
    _first_index=new_pending_index;
    return 0;
}

int BallotBox::append_pending_task(const Configuration& conf, const Configuration* old_conf,const ClientContext &clientContext) 
{
    Ballot bl;
    if (bl.init(conf, old_conf) != 0) 
    {
        TLOGWARN_RAFT( "Fail to init ballot");
        return -1;
    }

    std::lock_guard<std::mutex> lck(_mutex);
    //CHECK(_pending_index > 0);
    if (_pending_index <= 0)
    {
        TLOGWARN_RAFT( "_pending_index <= 0");
    }
    
    //_pending_meta_queue.push_back(Ballot());
    //_pending_meta_queue.back().swap(bl);
    _pending_meta_queue.push_back(bl);
    _vContext.push_back(clientContext);
    return 0;
}

int BallotBox::pop_until(int64_t index, std::vector<ClientContext> &out, int64_t *out_first_index) 
{
    out.clear();
    std::unique_lock<std::mutex> lck(_mutex);
    if (_vContext.empty() || index < _first_index) 
    {
        *out_first_index = index + 1;
        return 0;
    }
    if (index > _first_index + (int64_t)_vContext.size() - 1) 
    {
        TLOGERROR_RAFT( "Invalid index=" << index
                     << " _first_index=" << _first_index
                     << " _vContext.size=" << _vContext.size()<<endl);
        return -1;
    }
    *out_first_index = _first_index;
    for (int64_t i = _first_index; i <= index; ++i) 
    {    
        out.push_back(_vContext.front());
        _vContext.pop_front();
    }
    _first_index = index + 1;
    return 0;
}

int BallotBox::set_last_committed_index(int64_t last_committed_index) 
{
    // FIXME: it seems that lock is not necessary here
    std::unique_lock<std::mutex> lck(_mutex);
    if (_pending_index != 0 || !_pending_meta_queue.empty()) 
    {
        //CHECK(last_committed_index < _pending_index)
        if (last_committed_index >= _pending_index)
        {
            TLOGERROR_RAFT(  "node changes to leader, pending_index=" << _pending_index
            << ", parameter last_committed_index=" << last_committed_index<<endl);
            return -1;
        }
        
            
    }
    if (last_committed_index <  _last_committed_index.load(std::memory_order_relaxed)) 
    {
        return EINVAL;
    }
    if (last_committed_index > _last_committed_index.load(std::memory_order_relaxed)) {
        _last_committed_index.store(last_committed_index, std::memory_order_relaxed);

        _waiter->on_committed(last_committed_index);
    }
    return 0;
}

void BallotBox::describe(std::ostream& os, bool use_html) 
{
    std::unique_lock<std::mutex> lck(_mutex);
    int64_t committed_index = _last_committed_index;
    int64_t pending_index = 0;
    size_t pending_queue_size = 0;
    if (_pending_index != 0) 
    {
        pending_index = _pending_index;
        pending_queue_size = _pending_meta_queue.size();
    }
    lck.unlock();
    const char *newline = use_html ? "<br>" : "\r\n";
    os << "last_committed_index: " << committed_index << newline;
    if (pending_queue_size != 0) 
    {
        os << "pending_index: " << pending_index << newline;
        os << "pending_queue_size: " << pending_queue_size << newline;
    }
}

void BallotBox::get_status(BallotBoxStatus* status) {
    if (!status) 
    {
        return;
    }
    std::unique_lock<std::mutex> lck(_mutex);
    status->_committed_index = _last_committed_index;
    if (_pending_meta_queue.size() != 0) 
    {
        status->_pending_index = _pending_index;
        status->_pending_queue_size = _pending_meta_queue.size();
    }
}

}  //  namespace raft

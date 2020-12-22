
#include "raft/Lease.h"
#include "util/tc_timeprovider.h"
namespace horsedb {

 #define raft_enable_leader_lease false
            //"Enable or disable leader lease. only when all peers in a raft group "
            //"set this configuration to true, leader lease check and vote are safe.");


void LeaderLease::init(int64_t election_timeout_ms) 
{
    _election_timeout_ms = election_timeout_ms;
}

void LeaderLease::on_leader_start(int64_t term) 
{

    std::lock_guard<std::mutex> lck(_mutex);
    ++_lease_epoch;
    _term = term;
    _last_active_timestamp = 0;
}

void LeaderLease::on_leader_stop() {
    std::lock_guard<std::mutex> lck(_mutex);
    _last_active_timestamp = 0;
    _term = 0;
}

void LeaderLease::on_lease_start(int64_t expect_lease_epoch, int64_t last_active_timestamp) 
{
    std::lock_guard<std::mutex> lck(_mutex);
    if (_term == 0 || expect_lease_epoch != _lease_epoch) {
        return;
    }
    _last_active_timestamp = last_active_timestamp;
}

void LeaderLease::renew(int64_t last_active_timestamp) 
{
    std::lock_guard<std::mutex> lck(_mutex);
    _last_active_timestamp = last_active_timestamp;
}

void LeaderLease::get_lease_info(LeaseInfo* lease_info) 
{
    lease_info->term = 0;
    lease_info->lease_epoch = 0;
    if (!raft_enable_leader_lease) 
    {
        lease_info->state = LeaderLease::DISABLED;
        return;
    }

    std::lock_guard<std::mutex> lck(_mutex);
    if (_term == 0) 
    {
        lease_info->state = LeaderLease::EXPIRED;
        return;
    }
    if (_last_active_timestamp == 0) 
    {
        lease_info->state = LeaderLease::NOT_READY;
        return;
    }
    if (TNOWMS < _last_active_timestamp + _election_timeout_ms) 
    {
        lease_info->term = _term;
        lease_info->lease_epoch = _lease_epoch;
        lease_info->state = LeaderLease::VALID;
    } else {
        lease_info->state = LeaderLease::SUSPECT;
    }
}

int64_t LeaderLease::lease_epoch() 
{
    std::lock_guard<std::mutex> lck(_mutex);
    return _lease_epoch;
}

void LeaderLease::reset_election_timeout_ms(int64_t election_timeout_ms) 
{
    std::lock_guard<std::mutex> lck(_mutex);
    _election_timeout_ms = election_timeout_ms;
}

//////////////////////////////////////////////////////////////////////////
/////////////////FollowerLease//////////////////////////
void FollowerLease::init(int64_t election_timeout_ms, int64_t max_clock_drift_ms) 
{
    _election_timeout_ms = election_timeout_ms;
    _max_clock_drift_ms = max_clock_drift_ms;
    // When the node restart, we are not sure when the lease will be expired actually,
    // so just be conservative.
    _last_leader_timestamp = TNOWMS;
}

void FollowerLease::renew(const PeerId& leader_id) 
{
    _last_leader = leader_id;
    _last_leader_timestamp = TNOWMS;
}

int64_t FollowerLease::last_leader_timestamp() 
{
    return _last_leader_timestamp;
}

int64_t FollowerLease::votable_time_from_now() {
    if (!raft_enable_leader_lease)
    {
        return 0;
    }

    int64_t now = TNOWMS;
    int64_t votable_timestamp = _last_leader_timestamp + _election_timeout_ms +
                                _max_clock_drift_ms;
    if (now >= votable_timestamp) 
    {
        return 0;
    }
    return votable_timestamp - now;
}

const PeerId& FollowerLease::last_leader() {
    return _last_leader;
}

bool FollowerLease::expired() 
{
    return TNOWMS - _last_leader_timestamp>= _election_timeout_ms + _max_clock_drift_ms;
}

void FollowerLease::reset() {
    _last_leader = PeerId();
    _last_leader_timestamp = 0;
}
//心跳超时可以自己配置，看你自己的网络规模和拓扑；
//竞选超时是随机的，大概在几十到几百毫秒；
//通常情况下，心跳超时比竞选超时长得多，心跳是秒级。 _election_timeout_ms 是 心跳
void FollowerLease::reset_election_timeout_ms(int64_t election_timeout_ms,
                                              int64_t max_clock_drift_ms) {
    _election_timeout_ms = election_timeout_ms;
    _max_clock_drift_ms = max_clock_drift_ms;
}

} // namespace braft

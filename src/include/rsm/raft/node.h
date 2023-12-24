#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /*
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;

    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /*
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role=RaftRole::Follower;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int commit_idx;
//    int count_vote=0;
    int last_heartbeat=0;
    std::map<int, int> follower_save_log_idx;
    std::map<int, int> vote_record;
    std::shared_ptr<BlockManager> bm_;
    //snapshot
    int last_snapshot_idx = 0;
    int last_snapshot_term = 0;
    std::vector<u8> last_snapshot_data;
    int last_snapshot_offset = 0;
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */
    std::map<int, bool> network_availability;
    for (auto node : node_configs) {
        network_availability[node.node_id] = true;
    }
    set_network(network_availability);
//    rpc_clients_map.clear();
//    for (auto node : node_configs) {
//        if (node.node_id != my_id) {
//            rpc_clients_map[node.node_id] = std::make_unique<RpcClient>(node.ip_address, node.port,true);
//        }
//    }
    state = std::make_unique<StateMachine>();
    std::stringstream filename;
    filename << "/tmp/raft_log_" << my_id <<".log";
    // delete if exits
//    if(std::filesystem::exists(filename.str()))
//        std::filesystem::remove(filename.str());
    bm_ =
            std::make_shared<BlockManager>(filename.str(), KDefaultBlockCnt);
    log_storage = std::make_unique<RaftLog<Command>>(bm_);
    log_storage->restore_log_entries(my_id, commit_idx, current_term, leader_id, last_snapshot_offset);
//    commit_idx = 0;
    for(int i = 0; i < node_configs.size(); i++) {
        follower_save_log_idx.insert(std::make_pair(i, 0));
        vote_record.insert(std::make_pair(i, -1));
    }
    thread_pool = std::make_unique<ThreadPool>(4);


    rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    stopped.store(false);
    std::srand(std::time(nullptr));
    // FIXME: maybe need set_network

//    log_storage.reset(new RaftLog<Command>(bm_));
//    log_storage->restore_log_entries(my_id, commit_idx, current_term, leader_id);
    // DEBUG test persist
    std::vector<u8> data2(DiskBlockSize, 0);
    bm_->read_block(0, data2.data());
    int * int_data2 = (int*) data2.data();
    std::cout<<"test persist meta data, node_id"<<int_data2[0]<<", commit_idx"<<int_data2[1]<<", term"<<int_data2[2]<<", leader"<<int_data2[3]<<std::endl;

    this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped.store(true);
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    //lock
    std::unique_lock<std::mutex> lock(mtx);
    if(role == RaftRole::Leader)
        return std::make_tuple(true, current_term);
    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    /* If AppendEntries RPC received from new leader: convert to follower */
    // lock
    std::unique_lock<std::mutex> lock(mtx);
    if(role != RaftRole::Leader) {
//        if(leader_id == -1)
//            return std::make_tuple(false, current_term, -1);

        // TODO: followers need to transfer to leader？
//        auto res = rpc_clients_map[leader_id]->call(RAFT_RPC_NEW_COMMEND, cmd_data, cmd_size);
//        return res.unwrap()->as<std::tuple<bool, int, int>>();
//        RAFT_LOG("node %d is not leader and dismiss new cmd", my_id);
        return std::make_tuple(false, current_term, -1);

    }

    Command cmd(0);
    cmd.deserialize(cmd_data, cmd_size);



//    // send to all followers
//    AppendEntriesArgs<Command> arg;
//    arg.Term = current_term;
//    arg.LeaderId = my_id;
//    arg.LeaderCommit = commit_idx;
//    arg.PrevLogIndex = log_storage->get_prev_log_index();
//    arg.PrevLogTerm = log_storage->get_prev_log_term();
    // append log to log_storage
    log_storage->append_log_entry(LogEntry<Command>(current_term, cmd));
    int temp_idx = log_storage->get_prev_log_index();
//    RAFT_LOG("leader %d append log %d", my_id, temp_idx);
//    log_storage->get_log_entries(arg.Entries);


//    // send to all followers
//    for (auto node : node_configs) {
//        if (node.node_id != my_id) {
//            send_append_entries(node.node_id, arg);
//        }
//    }

//    // apply log to state machine
//    for(int i = state->store.size() - 1; i <= commit_idx; i++) {
//        Command cmd_ = log_storage->get_log_entry(i).command;
//        state->apply_log(cmd_);
//    }
    return std::make_tuple(true, current_term, temp_idx);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */
    // save snapshot
    std::vector<u8> snapshot = state->snapshot();
//    RAFT_LOG("node %d save snapshot %zu", my_id, snapshot.size());
    last_snapshot_idx = state->store.size() - 1;
    last_snapshot_term = log_storage->get_log_term(last_snapshot_idx);
    last_snapshot_data = snapshot;
    // delete log
    commit_idx = 0;
    log_storage->save_snapshot(my_id, last_snapshot_idx, last_snapshot_term, last_snapshot_offset, snapshot, true);
    last_snapshot_offset ++;
    // delete state machine
    state.reset();
    state = std::make_unique<StateMachine>();
//    RAFT_LOG("node %d has state %zu", my_id, state->store.size());

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    // lock
//    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> snapshot;
    int last_index_, last_term_;
    for(int i = 0; i < last_snapshot_offset; i++) {
        std::vector<u8> temp = log_storage->read_snapshot(my_id, i,last_index_, last_term_);
        snapshot.insert(snapshot.end(), temp.begin(), temp.end());
    }
    std::vector<u8> curr_ = state->snapshot();
    snapshot.insert(snapshot.end(), curr_.begin(), curr_.end());

//    RAFT_LOG("node %d get snapshot %zu", my_id, snapshot.size());
    return snapshot;
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    // TODO: maybe need a commit_idx
    RequestVoteReply reply;
    // lock
    std::unique_lock<std::mutex> lock(mtx);
    // If the request is coming from an old term then reject it.
    if (args.Term < current_term) {
//        RAFT_LOG("node %d with term :  %d ,not vote for %d with term : %d", my_id, current_term, args.CandidateId, args.Term);
        reply.CurrentTerm = current_term;
        reply.VoteGranted = false;
        return reply;
    }
//    RAFT_LOG("node %d with term :  %d ,is voting for %d with term : %d", my_id, current_term, args.CandidateId, args.Term);

    // If the term of the request peer is larger than this node, update the term
    // If the term is equal and we've already voted for a different candidate then
    // don't vote for this candidate.
    if (args.Term > current_term || leader_id == -1) {
        // compare log
        if(args.LastLogTerm > log_storage->get_prev_log_term() || (args.LastLogTerm <= log_storage->get_prev_log_term() && args.LastLogIndex >= log_storage->get_prev_log_index())) {
            leader_id = args.CandidateId;
            this->role = RaftRole::Follower;
//            RAFT_LOG("node %d with term :  %d ,vote for %d with term : %d", my_id, current_term, args.CandidateId, args.Term);
            current_term = args.Term;
//            RAFT_LOG("node %d update term to %d", my_id, current_term);
            reply.CurrentTerm = current_term;
            reply.VoteGranted = true;
            this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            return reply;
        }
        else {
            current_term = args.Term;
//            RAFT_LOG("node %d update term to %d with longer log", my_id, current_term);
            leader_id = -1;
            reply.CurrentTerm = current_term;
            reply.VoteGranted = false;
            return reply;
        }
    }
    // has voted for others with same terms
    reply.CurrentTerm = current_term;
    reply.VoteGranted = false;


    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    // lock
    std::unique_lock<std::mutex> lock(mtx);

    if(arg.CandidateId != my_id || role != RaftRole::Candidate)
        return;

//    RAFT_LOG("node %d get vote from node %d: %d", my_id, target, reply.VoteGranted);
    if (reply.VoteGranted) {
//        count_vote++;
        vote_record[target] = arg.CandidateId;
//        if (count_vote > node_configs.size() / 2) {
//            role = RaftRole::Leader;
////            leader_id = my_id;
//        }
        int cnt = 0;
        for(auto vote : vote_record)
            if(vote.second == my_id)
                cnt++;
        if(cnt > node_configs.size() / 2) {
            role = RaftRole::Leader;
            leader_id = my_id;
        }
    }
    else if(reply.CurrentTerm > current_term) {
        current_term = reply.CurrentTerm;
//        RAFT_LOG("node %d update term to %d", my_id, current_term);
        role = RaftRole::Follower;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
//        count_vote = 0;
        for(auto &vote : vote_record)
            vote.second = -1;
        leader_id = -1;
    }
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    AppendEntriesReply reply;
    AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);
    // If the request is coming from an old term then reject it.

    // lock
    std::unique_lock<std::mutex> lock(mtx);
    if (arg.Term < current_term) {
        RAFT_LOG("reject append entries from term %d", arg.Term);
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }
    role = RaftRole::Follower;
    //update term
    current_term = arg.Term;
//    RAFT_LOG("node %d update term to %d", my_id, current_term);

    // heartbeat
    if(arg.Entries.size() == 0) {
//        RAFT_LOG("node %d get heartbeat from node %d", my_id, arg.LeaderId);
        reply.Term = current_term;
        reply.Success = true;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        return reply;
    }
//    RAFT_LOG("Get append entries from node %d, %d, %d", arg.LeaderId,arg.PrevLogIndex, rpc_arg.SnapShotNum);

    if(rpc_arg.SnapShotNum && arg.PrevLogIndex <= rpc_arg.SnapShotNum) {
//        RAFT_LOG("Case snapshot prevLog is %d",arg.PrevLogIndex);
        for(int i = 0; i < arg.Entries.size(); i++) {
            log_storage->append_log_entry(arg.Entries[i]);
//            RAFT_LOG("node %d append log %d with snap", my_id, log_storage->get_prev_log_index());
        }
//        log_storage->log_length_ = arg.PrevLogIndex + 1;
        reply.Term = current_term;
        reply.Success = true;
        return reply;
    }

    // check prev log
    if (arg.PrevLogIndex > log_storage->get_prev_log_index()) {
//        RAFT_LOG("REject curr index is %d while arg is %d",log_storage->get_prev_log_index(), arg.PrevLogIndex);
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }

    // check prev log term
    if (arg.PrevLogTerm != log_storage->get_prev_log_term()) {
//        RAFT_LOG("REject curr term is %d while arg is %d",log_storage->get_prev_log_term(), arg.PrevLogTerm);
        log_storage->delete_entries(arg.PrevLogIndex);
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }


    this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    leader_id = arg.LeaderId;
    reply.Term = current_term;
    reply.Success = true;

    //apply current and following logs
    for(int i = arg.PrevLogIndex + 1; i < arg.Entries.size(); i++) {
        log_storage->append_log_entry(arg.Entries[i]);
//        RAFT_LOG("node %d append log %d", my_id, arg.Entries[i].command.value);
    }
    reply.SavedLogIndex = log_storage->get_prev_log_index();

    //update commit
    if (arg.LeaderCommit > commit_idx) {
        commit_idx = std::min(arg.LeaderCommit, log_storage->get_prev_log_index());
//        for (int i = state->store.size() - 1; i <= commit_idx; i++) {
//            Command cmd_ = log_storage->get_log_entry(i).command;
//            state->apply_log(cmd_);
//        }
    }

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    if (role != RaftRole::Leader)
        return;
    if(!reply.Success) {
        // lock
        std::unique_lock<std::mutex> lock(mtx);
        if(reply.Term > current_term) { // if the term is larger, then change to follower
            current_term = reply.Term;
//            RAFT_LOG("node %d update term to %d and become follower from %d", my_id, current_term,node_id);
            role = RaftRole::Follower;
            this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            leader_id = -1;
            return;
        }
        /* missing entries or deleted for conflicts */
        AppendEntriesArgs<Command> arg_var = arg;
        arg_var.PrevLogIndex--;
        arg_var.PrevLogTerm = log_storage->get_log_term(arg_var.PrevLogIndex);
        arg_var.SnapShotNum = log_storage->get_snap_num();
        // unlock
        lock.unlock();
        send_append_entries(node_id, arg_var);
        return;
    }
    if(reply.Success) {
        // lock
        std::unique_lock<std::mutex> lock(mtx);
        follower_save_log_idx[node_id] = reply.SavedLogIndex;
        int prev_log_idx = log_storage->get_prev_log_index();
        follower_save_log_idx[my_id] = prev_log_idx;
        // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
//        int i = commit_idx + 1;
//        for(; i <= follower_save_log_idx[my_id]; i++) {
            int cnt = 0;
            for(auto follower : follower_save_log_idx)
                if(follower.second >= prev_log_idx)
                    cnt++;

            if(cnt > node_configs.size() / 2)
                commit_idx = prev_log_idx;

//        }

        // unlock
        lock.unlock();
    }

    return;
}


template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    InstallSnapshotReply reply;
    reply.Term = current_term;
    // reject install
    if (this->role == RaftRole::Leader || args.Term < current_term)
        return reply;

    current_term = args.Term;

    if(args.Done) {
        state.reset();
        state = std::make_unique<StateMachine>();
        state->apply_snapshot(args.Data);
        commit_idx = args.LastIncludedIndex;
        return reply;
    }
    if(args.Offset >= last_snapshot_offset) {
        // install directly
        commit_idx -= args.Data.size();
        commit_idx = std::max(commit_idx, 0);
        log_storage->save_snapshot(my_id, args.LastIncludedIndex, args.LastIncludedTerm, args.Offset, args.Data,true);
        last_snapshot_offset = args.Offset;
        last_snapshot_data = args.Data;
        last_snapshot_idx = args.LastIncludedIndex;
        last_snapshot_term = args.LastIncludedTerm;
        return reply;
    }
    // check chunks
    int last_index_,last_term;
    auto old_chunks = log_storage->read_snapshot(my_id, args.Offset,last_index_,last_term);
    if (args.LastIncludedTerm > last_term || (args.LastIncludedTerm == last_term && args.Data.size() > old_chunks.size())) {
        // install directly
        log_storage->save_snapshot(my_id, args.LastIncludedIndex, args.LastIncludedTerm, args.Offset, args.Data);
        last_snapshot_offset = args.Offset;
        last_snapshot_data = args.Data;
        last_snapshot_idx = args.LastIncludedIndex;
        last_snapshot_term = args.LastIncludedTerm;
        return reply;
    }
    // reject install
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    if(reply.Term > current_term) {
        current_term = reply.Term;
//        RAFT_LOG("node %d update term %d from %d and become follower", my_id, current_term, node_id);
        role = RaftRole::Follower;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        leader_id = -1;
        return;
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }
//    RAFT_LOG("node %d send vote with term %d to node %d", my_id, arg.Term, target_id);

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (is_stopped()) {
                 return;
             }
             /* Lab3: Your code here */
             if (this->role != RaftRole::Leader) {
                 int now_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
                 int timeout = (rand()+139*my_id)%700 + 200;
                 if(my_id == node_configs.size()-1)
                     timeout=120;
                 if (now_ - this->last_heartbeat > timeout) { // election timeout
                     // lock

//                     RAFT_LOG("election time out with: %d", now_ - this->last_heartbeat);
                     std::unique_lock<std::mutex> lock(mtx);
                     this->role = RaftRole::Candidate;
                     this->current_term++;
//                     RAFT_LOG("node %d begin to hold election with term %d ", my_id, this->current_term);
                     this->leader_id = my_id;
//                     this->count_vote = 1; // vote for myself
                    for(auto &vote : vote_record)
                        vote.second = -1;
                     this->vote_record[my_id] = my_id;

                     // hold election
//                     RAFT_LOG("node %d begin to send vote with term %d ", my_id, this->current_term);

                     RequestVoteArgs arg;
                     arg.Term = this->current_term;
                     arg.CandidateId = this->my_id;
                     arg.LastLogIndex = log_storage->get_prev_log_index();
                     arg.LastLogTerm = log_storage->get_prev_log_term();
                     //unlock
                     lock.unlock();
                    for (auto node: this->node_configs) {
//                        RAFT_LOG("node %d send vote with term %d to node %d", my_id, this->current_term, node.node_id);

//                        RAFT_LOG("node role is %d", this->role);
                        if(this->role == RaftRole::Candidate && node.node_id != this->my_id) {
                            this->send_request_vote(node.node_id, arg);
                        }

                    }
                    this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
                 }
             }
             // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
         }
     }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (is_stopped()) {
                 return;
             }
             /* Lab3: Your code here */
             if(this->role == RaftRole::Leader) {
//                 RAFT_LOG("leader %d send log to followers", my_id);
                 //lock
                 std::unique_lock<std::mutex> lock(mtx);
                 // send to all followers
                 AppendEntriesArgs<Command> arg;
                 arg.Term = current_term;
                 arg.LeaderId = my_id;
                 arg.LeaderCommit = commit_idx;
                 arg.PrevLogIndex = log_storage->get_prev_log_index();
                 arg.PrevLogTerm = log_storage->get_prev_log_term();
                 log_storage->get_log_entries(arg.Entries);

                 // unlock
                 lock.unlock();

                 // send to all followers
                 for (auto node : node_configs) {
                     if (node.node_id != my_id && role == RaftRole::Leader) {
                         send_append_entries(node.node_id, arg);
                     }
                 }
             }

            // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
         }
     }

//    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
     while (true) {
         {
             if (is_stopped()) {
                 return;
             }
             /* Lab3: Your code here */
             // apply log to state machine
//             RAFT_LOG("node %d apply log to state machine, commit_idx: %d", my_id, commit_idx);
             // lock
             std::unique_lock<std::mutex> lock(mtx);
             int lastApplied = state->store.size() - 1;
             lastApplied = lastApplied > 0 ? lastApplied : 0;
             for(int i = lastApplied + 1; i <= commit_idx; i++) {
                 Command cmd_ = log_storage->get_log_entry(i).command;
                 state->apply_log(cmd_);
             }
             log_storage->persist_log_entries(my_id, commit_idx, current_term, leader_id, last_snapshot_offset);
            // unlock
            lock.unlock();
            // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
         }
     }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            if(this->role == RaftRole::Leader) {

                AppendEntriesArgs<Command> arg;
                arg.Term = this->current_term;
                arg.LeaderId = this->my_id;
                arg.Entries.clear();
                for(auto node: this->node_configs) {
                    //lock
                    std::unique_lock<std::mutex> lock(mtx);
                    if(this->role == RaftRole::Leader && node.node_id != this->my_id) {
                        // unlock
                        lock.unlock();
                        this->send_append_entries(node.node_id, arg);
//                        RAFT_LOG("node %d send ping to %d", my_id, node.node_id);
                    }
                }
                if(this->role == RaftRole::Leader && last_snapshot_offset > 0) {

                    for (auto node: this->node_configs) {
                        if (node.node_id != this->my_id) {

                            InstallSnapshotArgs snap_arg;
                            snap_arg.Term = this->current_term;
                            snap_arg.LeaderId = this->my_id;

                            for(int i = 0; i < last_snapshot_offset; i++) {
                                snap_arg.Offset = i;
                                snap_arg.Data = log_storage->read_snapshot(my_id, i, snap_arg.LastIncludedIndex , snap_arg.LastIncludedTerm);
                                snap_arg.Done = false;
                                if(this->role == RaftRole::Leader)
                                    this->send_install_snapshot(node.node_id, snap_arg);
                            }

                            // for the last time , sync the state machine
                            snap_arg.Done = true;
                            snap_arg.Data = state->snapshot();
                            snap_arg.LastIncludedIndex = commit_idx;
                            if(this->role == RaftRole::Leader)
                                this->send_install_snapshot(node.node_id, snap_arg);
                        }
                    }

                }
            }
            // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(230));
        }
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id)
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }

    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot();
}

}
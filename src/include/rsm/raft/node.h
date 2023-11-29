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
    int count_vote=0;
    int last_heartbeat=0;
    std::map<int, int> follower_save_log_idx;
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
    rpc_clients_map.clear();
    for (auto node : node_configs) {
        if (node.node_id != my_id) {
            rpc_clients_map[node.node_id] = std::make_unique<RpcClient>(node.ip_address, node.port,true);
        }
    }
    state = std::make_unique<StateMachine>();
    log_storage = std::make_unique<RaftLog<Command>>();
    for(int i = 0; i < node_configs.size(); i++) {
        follower_save_log_idx.insert(std::make_pair(i, 0));
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
    if(role == RaftRole::Follower) {
        if(leader_id == -1)
            return std::make_tuple(false, current_term, -1);

//        auto res = rpc_clients_map[leader_id]->call(RAFT_RPC_NEW_COMMEND, cmd_data, cmd_size);
//        return res.unwrap()->as<std::tuple<bool, int, int>>();
        return std::make_tuple(false, current_term, -1);

    }

    Command cmd(0);
    cmd.deserialize(cmd_data, cmd_size);


    // lock
    std::unique_lock<std::mutex> lock(mtx);

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
//    log_storage->get_log_entries(arg.Entries);

    // unlock
    lock.unlock();

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
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
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
    // If the request is coming from an old term then reject it.
    if (args.Term < current_term) {
        RAFT_LOG("node %d with term :  %d ,is voting for %d with term : %d", my_id, current_term, args.CandidateId, args.Term);
        reply.CurrentTerm = current_term;
        reply.VoteGranted = false;
        return reply;
    }
    RAFT_LOG("node %d with term :  %d ,is voting for %d with term : %d", my_id, current_term, args.CandidateId, args.Term);

    // If the term of the request peer is larger than this node, update the term
    // If the term is equal and we've already voted for a different candidate then
    // don't vote for this candidate.
    if (args.Term > current_term || leader_id == -1) {
        current_term = args.Term;
        leader_id = args.CandidateId;
        role = RaftRole::Follower;
        reply.CurrentTerm = current_term;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        reply.VoteGranted = true;
    }
    else if (args.Term == current_term && leader_id == -1) {
        leader_id = args.CandidateId;
        role = RaftRole::Follower;
        reply.CurrentTerm = current_term;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        reply.VoteGranted = true;
    }
    else {
        reply.CurrentTerm = current_term;
        reply.VoteGranted = false;
    }

    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    RAFT_LOG("node %d get vote from node %d: %d", my_id, target, reply.VoteGranted);
    if (arg.CandidateId == my_id && reply.VoteGranted) {
        count_vote++;
        if (count_vote > node_configs.size() / 2) {
            role = RaftRole::Leader;
            leader_id = my_id;
        }
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
    if (arg.Term < current_term) {
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }
    //update commit
    if (arg.LeaderCommit > commit_idx) {
        commit_idx = std::min(arg.LeaderCommit, log_storage->get_prev_log_index());
//        for (int i = state->store.size() - 1; i <= commit_idx; i++) {
//            Command cmd_ = log_storage->get_log_entry(i).command;
//            state->apply_log(cmd_);
//        }
    }

    // heartbeat
    if(arg.Entries.size() == 0) {
        reply.Term = current_term;
        reply.Success = true;
        this->last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        return reply;
    }

    // check prev log
    if (arg.PrevLogIndex > log_storage->get_prev_log_index()) {
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }

    // check prev log term
    if (arg.PrevLogTerm != log_storage->get_prev_log_term()) {
        log_storage->delete_entries(arg.PrevLogIndex);
        reply.Term = current_term;
        reply.Success = false;
        return reply;
    }

    current_term = arg.Term;
    leader_id = arg.LeaderId;
    reply.Term = current_term;
    reply.Success = true;

    //apply current and following logs
    for(int i = arg.PrevLogIndex + 1; i < arg.Entries.size(); i++) {
        log_storage->append_log_entry(arg.Entries[i]);
    }
    reply.SavedLogIndex = log_storage->get_prev_log_index();

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
            role = RaftRole::Follower;
            leader_id = -1;
            return;
        }
        /* missing entries or deleted for conflicts */
        AppendEntriesArgs<Command> arg_var = arg;
        arg_var.PrevLogIndex--;
        arg_var.PrevLogTerm = log_storage->get_log_term(arg_var.PrevLogIndex);
        // unlock
        lock.unlock();
        send_append_entries(node_id, arg_var);
        return;
    }
    if(reply.Success) {
        // lock
        std::unique_lock<std::mutex> lock(mtx);
        follower_save_log_idx[node_id] = reply.SavedLogIndex;
        follower_save_log_idx[my_id] = log_storage->get_prev_log_index();
        // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
        for(int i = follower_save_log_idx[my_id]; i > commit_idx; i--) {
            int cnt = 0;
            for(auto follower : follower_save_log_idx)
                if(follower.second >= i)
                    cnt++;

            if(cnt > node_configs.size() / 2) {
                commit_idx = i;
                break;
            }
        }

        // unlock
        lock.unlock();
    }

    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }
    RAFT_LOG("node %d send vote with term %d to node %d", my_id, arg.Term, target_id);

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
             if (this->role == RaftRole::Follower) {
                 int now_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
                 RAFT_LOG("heartbeat interval: %d", now_ - this->last_heartbeat);
                 if (now_ - this->last_heartbeat > (rand()%150 + 150)) {
                     // lock
                     std::unique_lock<std::mutex> lock(mtx);
                     this->role = RaftRole::Candidate;
                     this->current_term++;
                     this->leader_id = -1;
                     this->count_vote = 1; // vote for myself
                     //unlock
                     lock.unlock();
                     // hold election
                     RAFT_LOG("node %d begin to send vote with term %d ", my_id, this->current_term);
                    for (auto node: this->node_configs) {
                        RAFT_LOG("node %d send vote with term %d to node %d", my_id, this->current_term, node.node_id);
                        RequestVoteArgs arg;
                        arg.Term = this->current_term;
                        arg.CandidateId = this->my_id;
//                        arg.LastLogIndex =
//                        arg.LastLogTerm =

                        // lock the client
//                        std::unique_lock<std::mutex> clients_lock(clients_mtx);
                        RAFT_LOG("node role is %d", this->role);
                        if(this->role == RaftRole::Candidate && node.node_id != this->my_id) {
                            this->send_request_vote(node.node_id, arg);
                        }
                        // unlock the client
//                        clients_lock.unlock();
                    }
                 }
             }
             // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
                 if (node.node_id != my_id) {
                     send_append_entries(node.node_id, arg);
                 }
             }

            // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
         }
     }

    return;
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
             for(int i = state->store.size() - 1; i <= commit_idx; i++) {
                 Command cmd_ = log_storage->get_log_entry(i).command;
                 state->apply_log(cmd_);
             }
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
                for(auto node: this->node_configs) {
                    AppendEntriesArgs<Command> arg;
                    arg.Term = this->current_term;
                    arg.LeaderId = this->my_id;
                    arg.Entries.clear();
                    if(this->role == RaftRole::Leader && node.node_id != this->my_id) {
                        this->send_append_entries(node.node_id, arg);
                    }
                }
            }
            // sleep for 100ms
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
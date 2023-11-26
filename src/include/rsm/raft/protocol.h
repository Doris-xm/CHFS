#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";
/*
 * term： candidate’s term
 * candidateId： candidate requesting vote
 * lastLogIndex： index of candidate’s last log entry (§5.4)
 * lastLogTerm： term of candidate’s last log entry (§5.4)
 */
struct RequestVoteArgs {
    /* Lab3: Your code here */
    unsigned int Term;
    unsigned int CandidateId;
    unsigned int LastLogIndex;
    unsigned int LastLogTerm;
    MSGPACK_DEFINE(
        Term,
        CandidateId,
        LastLogIndex,
        LastLogTerm
    
    )
};

/*
 * term: currentTerm, for candidate to update itself
 * voteGranted: true means candidate received vote
 * */
struct RequestVoteReply {
    /* Lab3: Your code here */
    unsigned int CurrentTerm;
    bool VoteGranted;

    MSGPACK_DEFINE(
        CurrentTerm,
        VoteGranted
    )
};
/*Arguments:
 * term : leader’s term
 * leaderId : so follower can redirect clients
 * prevLogIndex : index of log entry immediately preceding new ones
 * prevLogTerm : term of prevLogIndex entry
 * entries[] : log entries to store (empty for heartbeat; may send more than one for efficiency)
 * leaderCommit : leader’s commitIndex
 * Results:
 * term : currentTerm, for leader to update itself
 * success : true if follower contained entry matching prevLogIndex and prevLogTerm
*/
template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */

    unsigned int Term;
    unsigned int LeaderId;
    unsigned int PrevLogIndex;
    unsigned int PrevLogTerm;
//    std::vector<RaftLog<Command>> Entries;
    unsigned int LeaderCommit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    unsigned int Term;
    unsigned int LeaderId;
    unsigned int PrevLogIndex;
    unsigned int PrevLogTerm;
//    std::vector<std::string> Entries;
    unsigned int LeaderCommit;
    MSGPACK_DEFINE(
            Term,
            LeaderId,
            PrevLogIndex,
            PrevLogTerm,
//            Entries,
            LeaderCommit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpcArgs;
    rpcArgs.Term = arg.Term;
    rpcArgs.LeaderId = arg.LeaderId;
    rpcArgs.PrevLogIndex = arg.PrevLogIndex;
    rpcArgs.PrevLogTerm = arg.PrevLogTerm;

    // Serialize log entries
//    for (const auto &entry : arg.entries) {
//        std::string serializedEntry;
//        // Serialize entry and add to rpcArgs.entries
//        rpcArgs.Entries.push_back(serializedEntry);
//    }
    rpcArgs.LeaderCommit = arg.LeaderCommit;

    return rpcArgs;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> args;
    args.term = rpc_arg.Term;
    args.leaderId = rpc_arg.LeaderId;
    args.prevLogIndex = rpc_arg.PrevLogIndex;
    args.prevLogTerm = rpc_arg.PrevLogTerm;

    args.LeaderCommit = rpc_arg.LeaderCommit;
    return args;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    unsigned int Term;
    bool Success;

    MSGPACK_DEFINE(
        Term,
        Success
    
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

} /* namespace chfs */
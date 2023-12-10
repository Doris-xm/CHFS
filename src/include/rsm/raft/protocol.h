#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

class sbuffer;
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
    int Term;
    int CandidateId;
    int LastLogIndex;
    int LastLogTerm;
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
    int CurrentTerm;
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

    int Term;
    int LeaderId;
    int PrevLogIndex;
    int PrevLogTerm;
    int SnapShotNum;
    std::vector<LogEntry<Command>> Entries;
    int LeaderCommit;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int Term;
    int LeaderId;
    int PrevLogIndex;
    int PrevLogTerm;
    int SnapShotNum;
    std::vector<u8> Entries;
    int LeaderCommit;
    MSGPACK_DEFINE(
            Term,
            LeaderId,
            PrevLogIndex,
            PrevLogTerm,
            SnapShotNum,
            Entries,
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
    rpcArgs.SnapShotNum = arg.SnapShotNum;

    // Serialize log entries
    RPCLIB_MSGPACK::sbuffer buffer;
    RPCLIB_MSGPACK::packer<RPCLIB_MSGPACK::sbuffer> pk(&buffer);
    pk.pack(arg.Entries);

    // Convert the packed data into a vector<u8>
    rpcArgs.Entries.assign(buffer.data(), buffer.data() + buffer.size());

    rpcArgs.LeaderCommit = arg.LeaderCommit;
    return rpcArgs;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> args;
    args.Term = rpc_arg.Term;
    args.LeaderId = rpc_arg.LeaderId;
    args.PrevLogIndex = rpc_arg.PrevLogIndex;
    args.PrevLogTerm = rpc_arg.PrevLogTerm;
    args.SnapShotNum = rpc_arg.SnapShotNum;

    // Deserialize log entries
    // Get the binary data of Entries from RpcAppendEntriesArgs
    std::vector<u8> entries_data = rpc_arg.Entries;

    // Unpack the binary data into a msgpack object
    RPCLIB_MSGPACK::object_handle oh = RPCLIB_MSGPACK::unpack(reinterpret_cast<const char*>(entries_data.data()), entries_data.size());
    RPCLIB_MSGPACK::object obj = oh.get();

    std::vector<LogEntry<Command>> entries;
    obj.convert(entries);  // Convert the msgpack object to a vector of LogEntry<Command>

    // Assign the unpacked entries to the AppendEntriesArgs structure
    args.Entries = entries;
    args.LeaderCommit = rpc_arg.LeaderCommit;
    return args;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int Term;
    bool Success;
    int SavedLogIndex;

    MSGPACK_DEFINE(
        Term,
        Success,
        SavedLogIndex
    
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int Term; //Leader’s term
    int LeaderId; //so follower can redirect clients
    int LastIncludedIndex; //the snapshot replaces all entries up through and including this index
    int LastIncludedTerm; //term of lastIncludedIndex
    int Offset; // byte offset where chunk is positioned in the snapshot file
    std::vector<u8> Data; // raw bytes of the snapshot chunk, starting at offset
    bool Done; // true if this is the last chunk

    MSGPACK_DEFINE(
        Term,
        LeaderId,
        LastIncludedIndex,
        LastIncludedTerm,
        Offset,
        Data,
        Done
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int Term; // currentTerm, for leader to update itself

    MSGPACK_DEFINE(
        Term
    )
};

} /* namespace chfs */
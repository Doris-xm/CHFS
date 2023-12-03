#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "rpc/msgpack/adaptor/define.hpp"
#include "rpc/msgpack/sbuffer.hpp"
#include <mutex>
#include <vector>
#include <cstring>

namespace chfs {

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
struct LogEntry {
    int term;
    Command command;
    MSGPACK_DEFINE(
        term,
        command
    )
    LogEntry() {}
    LogEntry(int term, Command command) : term(term), command(command) {}
};
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    RaftLog(int node_id);
    ~RaftLog();

    /* Lab3: Your code here */
    int get_prev_log_term();
    int get_prev_log_index();
    int get_log_term(int index);
    void append_log_entry(LogEntry<Command> entry);
    void delete_entries(int index); // delete entries include and after index
    void get_log_entries(std::vector<LogEntry<Command>> &entries);
    LogEntry<Command> get_log_entry(int index);
    void persist_log_entries(int node_id, int commit_idx, int term, int leader);
    void restore_log_entries(int node_id, int &commit_idx, int &term, int &leader);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::vector<LogEntry<Command>> log_entries_;
    int last_commit_ = 1;

};

    template<typename Command>
    void RaftLog<Command>::restore_log_entries(int node_id, int &commit_idx, int &term, int &leader) {
        std::unique_lock<std::mutex> lock(mtx);
        u8 data[DiskBlockSize];
        bm_->read_block(node_id*20, data);
        int * int_data = (int*) data;
        int id = int_data[0];
        if(id == 0) { // empty log
            last_commit_ = 1;
            term = 0;
            leader = -1;
            commit_idx = 0;
            return;
        }
        commit_idx = int_data[1];
        term = int_data[2];
        leader = int_data[3];
        for(int i = 1; i <= commit_idx; i++){
            bm_->read_block(node_id*20+i, data);
            std::vector<u8> intBytes(data+4, data+8);
            int entry_term = *(int*)intBytes.data();
            std::vector<u8> commandBytes(data, data+3);
            Command command;
            command.deserialize(commandBytes, command.size());
            log_entries_.emplace_back(entry_term, command);
        }
        last_commit_ = commit_idx;

    }

    template<typename Command>
    void RaftLog<Command>::persist_log_entries(int node_id, int commit_idx, int term, int leader) {
        if(commit_idx <= last_commit_)
            return;
        std::unique_lock<std::mutex> lock(mtx);
        for(int i = last_commit_; i <= commit_idx; i++){
            LogEntry<Command> entry = log_entries_[i];
            std::vector<u8> data = entry.command.serialize(entry.command.size());
            std::vector<u8> intBytes(sizeof(int));
            data.resize(DiskBlockSize, 0);
            memcpy(intBytes.data(), &entry.term, sizeof(int));
            // 插入到 data 向量中的第4位后面
            data.insert(data.begin() + 4, intBytes.begin(), intBytes.end());
            bm_->write_block(node_id*20+i, data.data());
        }
        last_commit_ = commit_idx;
        // persist meta data
        u8 data[DiskBlockSize];
        int * int_data = (int*) data;
        int_data[0] = node_id+1;// means not empty
        int_data[1] = commit_idx;
        int_data[2] = term;
        int_data[3] = leader;
        bm_->write_block(node_id*20, data);

    }

    template<typename Command>
    RaftLog<Command>::RaftLog(int node_id) {
        bm_ =
                std::shared_ptr<BlockManager>(new BlockManager(KDefaultBlockCnt, DiskBlockSize));
        // clear log
        std::vector<u8> zero(DiskBlockSize,0);
        bm_->write_block(node_id*20, zero.data());
        log_entries_.emplace_back(0, Command(0));
        last_commit_ = 1;
    }

    template<typename Command>
    LogEntry<Command> RaftLog<Command>::get_log_entry(int index) {
        if(index >= log_entries_.size())
            return LogEntry<Command>(0, Command(0));
        return log_entries_[index];
    }

    template<typename Command>
    int RaftLog<Command>::get_prev_log_index() {
        return log_entries_.size() - 1;
    }

    template<typename Command>
    int RaftLog<Command>::get_log_term(int index) {
        if(index >= log_entries_.size())
            return 0;
        return log_entries_[index].term;
    }

    template<typename Command>
    void RaftLog<Command>::delete_entries(int index){
        if (index >= log_entries_.size())
            return;
        log_entries_.erase(log_entries_.begin() + index, log_entries_.end());
    }

    template<typename Command>
    void RaftLog<Command>::append_log_entry(LogEntry<Command> entry) {
        log_entries_.push_back(entry);
    }

    template<typename Command>
    int RaftLog<Command>::get_prev_log_term() {
        if(log_entries_.size() == 0)
            return 0;
        return log_entries_.back().term;
    }

    template<typename Command>
    void RaftLog<Command>::get_log_entries(std::vector<LogEntry<Command>> &entries) {
        entries.clear();
        for (auto logEntry : log_entries_)
            entries.push_back(logEntry);
    }

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
{
    /* Lab3: Your code here */
    bm_ = bm;
    log_entries_.emplace_back(0, Command(0));
    last_commit_ = 1;
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */

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
    RaftLog();
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
    void save_snapshot(int node_id, int last_included_index, int last_included_term, int offset, std::vector<u8> data, bool delete_ = false);
    std::vector<u8> read_snapshot(int node_id, int offset, int &last_included_index, int &last_included_term);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */

public:
    std::vector<LogEntry<Command>> log_entries_;
    int last_commit_ = 1;
    bool isDeleted = false;
    int last_delete_index_ = 0;
    int last_delete_term_ = 0;

};

    template<typename Command>
    std::vector<u8> RaftLog<Command>::read_snapshot(int node_id, int offset, int &last_included_index, int &last_included_term) {
        std::unique_lock<std::mutex> lock(mtx);
        std::stringstream filename;
        filename << "/tmp/snapshot_" << node_id << ".log";
        auto snap_bm_ = std::make_shared<BlockManager>(filename.str(), KDefaultBlockCnt);
        std::vector<u8> data(DiskBlockSize, 0);
        snap_bm_->read_block(offset, data.data());
        int * int_data = (int*) data.data();
        last_included_index = int_data[0];
        last_included_term = int_data[1];
        int size = int_data[2];
        data.erase(data.begin(), data.begin()+3*sizeof(int));
        data.resize(size);

        return data;
    }

    template<typename Command>
    void RaftLog<Command>::save_snapshot(int node_id, int last_included_index, int last_included_term, int offset,
                                          std::vector<u8> data, bool delete_) {
        std::stringstream filename;
        filename << "/tmp/snapshot_" << node_id << ".log";
        if (offset == 0 && std::filesystem::exists(filename.str()))
            std::filesystem::remove(filename.str());
        auto snap_bm_ = std::make_shared<BlockManager>(filename.str(), KDefaultBlockCnt);
        // 将整数转换为字节序列
        int size_ = data.size();
        std::vector<u8> bytes1(reinterpret_cast<u8*>(&last_included_index), reinterpret_cast<u8*>(&last_included_index) + sizeof(int));
        std::vector<u8> bytes2(reinterpret_cast<u8*>(&last_included_term), reinterpret_cast<u8*>(&last_included_term) + sizeof(int));
        std::vector<u8> bytes3(reinterpret_cast<u8*>(&size_), reinterpret_cast<u8*>(&size_) + sizeof(int));

        // 在 data 向量的开头插入这3个整数的字节序列
        data.insert(data.begin(), bytes2.begin(), bytes2.end());
        data.insert(data.begin(), bytes1.begin(), bytes1.end());
        data.insert(data.begin(), bytes3.begin(), bytes3.end());
        data.resize(DiskBlockSize, 0);

        snap_bm_->write_block(offset, data.data());

//         delete log

        std::unique_lock<std::mutex> lock(mtx);
        if(delete_){
            if(!isDeleted) {
                last_delete_index_ = last_included_index;
                last_delete_term_ = last_included_term;
                isDeleted = true;
            } else {
                last_delete_index_ += last_included_index;
                last_delete_term_ = last_included_term;
            }
            int max_index = log_entries_.size() - 1;
            max_index = std::min(max_index, last_included_index);
            for(int i = 1; i <= max_index; i++){
                log_entries_.erase(log_entries_.begin()+1);
            }
            last_commit_ = 1;
        }

    }

    template<typename Command>
    void RaftLog<Command>::restore_log_entries(int node_id, int &commit_idx, int &term, int &leader) {
        std::unique_lock<std::mutex> lock(mtx);
        std::vector<u8> data(DiskBlockSize, 0);
        bm_->read_block(0, data.data());
        int * int_data = (int*) data.data();
        int id = int_data[0]; // 0 means empty
        if(!id || id != (node_id + 1) ) { // empty metadata
            last_commit_ = 1;
            term = 0;
            leader = -1;
            commit_idx = 0;
            std::cout<<"restore new log entries, node_id"<<node_id<<", commit_idx"<<commit_idx<<", term"<<term<<", leader"<<leader<<std::endl;
            return;
        }
        commit_idx = int_data[1];
        term = int_data[2];
        leader = int_data[3];
        for(int i = 1; i <= commit_idx; i++){
            bm_->read_block(i, data.data());
            std::vector<u8> intBytes(data.data()+4, data.data()+8);
            int entry_term = *(int*)intBytes.data();
            std::vector<u8> commandBytes(data.data(), data.data()+4);
            Command command;
            command.deserialize(commandBytes, command.size());
            log_entries_.emplace_back(entry_term, command);
        }
        last_commit_ = commit_idx;
        std::cout<<"restore log entries, node_id"<<node_id<<", commit_idx"<<commit_idx<<", term"<<term<<", leader"<<leader<<std::endl;

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
            bm_->write_block(i, data.data());
        }
        last_commit_ = commit_idx;
        // persist meta data
        u8 data[DiskBlockSize];
        int * int_data = (int*) data;
        int_data[0] = node_id + 1;// means not empty
        int_data[1] = commit_idx;
        int_data[2] = term;
        int_data[3] = leader;
        bm_->write_block(0, data);
        std::cout<<"persist log entries, node_id"<<node_id<<", commit_idx"<<commit_idx<<", term"<<term<<", leader"<<leader<<std::endl;
    }

    template<typename Command>
    RaftLog<Command>::RaftLog() {
        bm_ =
                std::shared_ptr<BlockManager>(new BlockManager(KDefaultBlockCnt, DiskBlockSize));
        // BUG: should not clear all data at the beginning
//        std::vector<u8> zero(DiskBlockSize,0);
//        bm_->write_block(node_id*20, zero.data());
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

        std::unique_lock<std::mutex> lock(mtx);
        if(isDeleted)
            return log_entries_.size() - 1 + last_delete_index_;
        return log_entries_.size() - 1;
    }

    template<typename Command>
    int RaftLog<Command>::get_log_term(int index) {
        if(isDeleted)
            index -= last_delete_index_;
        if(index >= log_entries_.size() || index < 0)
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
        std::unique_lock<std::mutex> lock(mtx);
        if(isDeleted) {
            if (log_entries_.size() > 1) {
                return log_entries_.back().term;
            }
            else
                return last_delete_term_;
        }
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

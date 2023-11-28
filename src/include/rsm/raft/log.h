#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include "rpc/msgpack/adaptor/define.hpp"
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
    void append_log_entry(std::vector<LogEntry<Command>> entries);
    void delete_entries(int index); // delete entries include and after index
    void get_log_entries(std::vector<LogEntry<Command>> &entries);
    LogEntry<Command> get_log_entry(int index);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    /* Lab3: Your code here */
    std::vector<LogEntry<Command>> log_entries_;

};

    template<typename Command>
    RaftLog<Command>::RaftLog() {
//        bm_ = new BlockManager();
        log_entries_.emplace_back(0, Command(0));
    }

    template<typename Command>
    LogEntry<Command> RaftLog<Command>::get_log_entry(int index) {
        return log_entries_[index];
    }

    template<typename Command>
    int RaftLog<Command>::get_prev_log_index() {
        return log_entries_.size() - 1;
    }

    template<typename Command>
    int RaftLog<Command>::get_log_term(int index) {
        return log_entries_[index].term;
    }

    template<typename Command>
    void RaftLog<Command>::delete_entries(int index){
        log_entries_.erase(log_entries_.begin() + index, log_entries_.end());
    }

    template<typename Command>
    void RaftLog<Command>::append_log_entry(std::vector<LogEntry<Command>> entries) {
        for (auto logEntry : entries)
            log_entries_.push_back(logEntry);
    }

    template<typename Command>
    int RaftLog<Command>::get_prev_log_term() {
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
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
    /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */

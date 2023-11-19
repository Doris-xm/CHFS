//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
   static txn_id_t global_txn_id_;
private:
    // blocks allocate
    block_id_t entry_table_; // 4096 - 1024 + 1
    uint32_t entry_per_block_;
    uint32_t entry_num_; // 5
    block_id_t commit_table_;
    uint32_t commit_num_; // 1
    block_id_t bit_map_;
    uint32_t bit_map_num_; // 1
    block_id_t log_data_;
    uint32_t log_data_num_; // 4096 - 1024 - 1 - 1 - 1 = 3070

};

struct LogEntry {
    block_id_t log_data_id_;
    block_id_t block_id_;
    txn_id_t txn_id_;
};

} // namespace chfs
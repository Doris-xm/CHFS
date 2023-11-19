#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
    this->global_txn_id_ = 0;
    this->entry_table_ = KDefaultBlockCnt - 1024 + 1; //4096 - 1024 + 1;
    this->entry_per_block_ = DiskBlockSize / sizeof(LogEntry);
    this->entry_num_ = 5;
    this->commit_table_ = this->entry_table_ + this->entry_num_;
    this->commit_num_ = 1;
    this->bit_map_ = this->commit_table_ + this->commit_num_;
    this->bit_map_num_ = 1;
    this->log_data_ = this->bit_map_ + this->bit_map_num_;
    this->log_data_num_ = KDefaultBlockCnt - 1024 -this->entry_num_ - this->commit_num_ - this->bit_map_num_;
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
    // 1. read bit_map_
    std::vector<u8> bitmap_buf(DiskBlockSize);
    bm_->read_block(bit_map_, bitmap_buf.data());
    auto bitmap = Bitmap(bitmap_buf.data(), DiskBlockSize);
    return bitmap.count_ones();
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  // 1. write into log_data_
  // check bit_map_
  std::vector<u8> bitmap_buf(DiskBlockSize);
  bm_->read_block(bit_map_, bitmap_buf.data());
  auto bitmap = Bitmap(bitmap_buf.data(), DiskBlockSize);

  for( auto iter:ops) {
      auto log_data_id = bitmap.find_first_free();
      if (!log_data_id.has_value() || log_data_id.value() >= log_data_num_) {
          std::cerr << "no free log data block" << std::endl;
          return;
      }
      bitmap.set(log_data_id.value());

      //write entry table
      std::vector<u8> entry_table_buf(sizeof(block_id_t) * 2 + sizeof (txn_id_t));
      LogEntry entry;
      entry.log_data_id_ = log_data_id.value() + this->log_data_;
      entry.block_id_ = iter->block_id_;
      entry.txn_id_ = txn_id;
      *(LogEntry*)entry_table_buf.data() = entry;
      bm_->write_partial_block(entry_table_ + log_data_id.value() / entry_per_block_,
                               entry_table_buf.data(),
                               log_data_id.value() % entry_per_block_, sizeof(LogEntry));

      //write log data
      bm_->write_block(log_data_id.value() + this->log_data_, iter->new_block_state_.data());

  }

  bm_->write_block(bit_map_, bitmap_buf.data());


}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  UNIMPLEMENTED();
}
}; // namespace chfs
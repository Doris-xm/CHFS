#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  auto version_block_num = KDefaultBlockCnt * sizeof(version_t) / bm->block_size();

  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_block_num, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, version_block_num, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
    std::vector<u8> buffer(block_allocator_->bm->block_size());
    std::vector<u8> version_buffer(block_allocator_->bm->block_size());
    // check version
    auto versions_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
    block_allocator_->bm->read_block(block_id / versions_per_block, version_buffer.data());
    if (*((version_t *)version_buffer.data() + block_id % versions_per_block) != version) {
        return {};
    }

    auto res = block_allocator_->bm->read_block(block_id, buffer.data());
    if (!res.is_ok()) {
        return {};
    }
    std::vector<u8> read_res(len);
    memcpy(read_res.data(), buffer.data() + offset, len > buffer.size()? buffer.size() : len);
    return read_res;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
    auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
    if(!res.is_ok()) {
        return false;
    }
    return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
    auto res = block_allocator_->allocate();
    if (!res.is_ok()) {
        return {0, 0};
    }
    std::vector<u8> buffer(block_allocator_->bm->block_size());
    // update version
    auto versions_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
    block_allocator_->bm->read_block(res.unwrap() / versions_per_block, buffer.data());
    auto old_version = *((version_t *)buffer.data() + res.unwrap() % versions_per_block);
    version_t new_version = old_version + 1;

    std::memcpy(buffer.data() + res.unwrap() % versions_per_block * (sizeof(version_t) / sizeof(u8)), &new_version, sizeof(version_t));
    block_allocator_->bm->write_block(res.unwrap() / versions_per_block, buffer.data());

    return {res.unwrap(), new_version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
    auto res = block_allocator_->deallocate(block_id);
    if (!res.is_ok()) {
        return false;
    }

    // update version
    std::vector<u8> buffer(block_allocator_->bm->block_size());
    auto versions_per_block = block_allocator_->bm->block_size() / sizeof(version_t);
    block_allocator_->bm->read_block(block_id / versions_per_block, buffer.data());
    auto old_version = *((version_t *)buffer.data() + block_id % versions_per_block);
    version_t new_version = old_version + 1;

    std::memcpy(buffer.data() + block_id % versions_per_block * (sizeof(version_t) / sizeof(u8)), &new_version, sizeof(version_t));
    block_allocator_->bm->write_block(block_id / versions_per_block, buffer.data());

    return true;
}
} // namespace chfs
#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  auto res = operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
    if (res.is_err()) {
        return 0;
    }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  auto res = operation_->unlink(parent, name.c_str());
  if (res.is_err()) {
    return false;
  }
    return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  auto res = operation_->lookup(parent, static_cast<const char *>(name.c_str()));
    if (res.is_err()) {
        return 0;
    }
    return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  auto buffer = operation_->read_file(id).unwrap();
  std::vector<BlockInfo> res;
    size_t blockSize = sizeof(BlockInfo);
    for (size_t i = 0; i + blockSize <= buffer.size(); i += blockSize) {
        BlockInfo info;
        std::memcpy(&info, buffer.data() + i, blockSize);
        res.push_back(info);
    }
  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
        auto dataserver_id = generator.rand(1,num_data_servers);
        auto res = clients_[dataserver_id]->call("alloc_block");
        if (res.is_err()) {
            return BlockInfo(0, 0, 0);
        }
        auto block_info = res.unwrap()->as<std::pair<block_id_t, version_t>>();
        BlockInfo ret_val = BlockInfo (block_info.first, dataserver_id, block_info.second);

        std::vector<u8> block_info_buffer(sizeof (BlockInfo) / sizeof(u8));
        *(BlockInfo *)(block_info_buffer.data()) = ret_val;
        auto buffer = operation_->read_file(id).unwrap();
        buffer.insert(block_info_buffer.end(), block_info_buffer.begin(), block_info_buffer.end());
        operation_->write_file(id, buffer);
        return ret_val;
}

// {Your code here}
    auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                    mac_id_t machine_id) -> bool {
        // TODO: Implement this function.
//  UNIMPLEMENTED();
        auto call_res = ChfsResult<std::shared_ptr<RpcResponse>>{std::make_shared<RpcResponse>()};
        {
            std::scoped_lock<std::shared_mutex> lock(data_mutex_[machine_id - 1]);
            call_res = clients_[machine_id]->call("free_block",block_id);
        }
        if(call_res.is_err()){
            return false;
        }
        auto is_free = call_res.unwrap()->as<bool>();
        if(!is_free){
            return false;
        }
        {
            std::scoped_lock<std::shared_mutex> lock(meta_mutex_);
            auto buffer = operation_->read_file(id).unwrap();
            u32 i = 0;
            for(; i < buffer.size()*sizeof(u8) / sizeof(BlockInfo); ++i){
                BlockInfo info = *((BlockInfo *) buffer.data() + i);
                if (std::get<0>(info) == block_id && std::get<1>(info) == machine_id) {
                    *((BlockInfo *)buffer.data() + i) = BlockInfo(0, 0, 0);
                    break;
                }
            }
            operation_->write_file(id,buffer);
            return true;
        }
    }

// {Your code here}
    auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
        // TODO: Implement this function.
//  UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(meta_mutex_);
        std::list<DirectoryEntry> list;
        read_directory(operation_.get(),node,list);
        auto res = std::vector<std::pair<std::string,inode_id_t>>();
        for(const auto& item : list){
            res.emplace_back(item.name,item.id);
        }

        return res;
    }

// {Your code here}
    auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
        // TODO: Implement this function.
//  UNIMPLEMENTED();
        std::scoped_lock<std::shared_mutex> lock(meta_mutex_);
        auto type_attr = operation_->inode_manager_->get_type_attr(id).unwrap();
        std::tuple<u64,u64,u64,u64,u8> res = {type_attr.second.size, type_attr.second.atime, type_attr.second.mtime, type_attr.second.ctime,
                                              static_cast<u8>(type_attr.first)};
        return res;
    }

    auto MetadataServer::reg_server(const std::string &address, u16 port,
                                    bool reliable) -> bool {
        num_data_servers += 1;
        auto cli = std::make_shared<RpcClient>(address, port, reliable);
        clients_.insert(std::make_pair(num_data_servers, cli));

        return true;
    }

    auto MetadataServer::run() -> bool {
        if (running)
            return false;
        data_mutex_ = std::vector<std::shared_mutex>(num_data_servers);
        // Currently we only support async start
        server_->run(true, num_worker_threads);
        running = true;
        return true;
    }

} // namespace chfs
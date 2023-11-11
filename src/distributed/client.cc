#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  auto res = metadata_server_->call("mknode", static_cast<chfs::u8>(type), parent, name);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    return res.unwrap()->get().as<chfs::inode_id_t>();
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return ChfsResult<inode_id_t>(0);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>({});
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return ChfsResult<std::pair<InodeType, FileAttr>>({});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
    auto blockinfo = metadata_server_->call("get_block_map", id);
    if (blockinfo.is_err()) {
        return blockinfo.unwrap_error();
    }
    auto block_info = blockinfo.unwrap()->as<std::vector<BlockInfo>>();
    auto num_block = (size + offset % DiskBlockSize) / DiskBlockSize;
    num_block += (size + offset % DiskBlockSize) % DiskBlockSize > 0;
    auto start_pos = offset / DiskBlockSize;

    std::vector<u8> read_res;
    auto curr_pos = offset % DiskBlockSize;
    for (auto i = start_pos; i < start_pos + num_block; ++i) {
        auto res = data_servers_[std::get<1>(block_info[i])]->call("read_data",
                                   std::get<0>(block_info[i]), curr_pos, DiskBlockSize - curr_pos,std::get<2>(block_info[i]));
        if (res.is_err()) {
            return res.unwrap_error();
        }
        auto buffer = res.unwrap()->as<std::vector<u8>>();
        read_res.insert(read_res.end(), buffer.begin(), buffer.end());
        curr_pos = 0;
    }
    return ChfsResult<std::vector<u8>>(read_res);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  auto blockinfo = metadata_server_->call("get_block_map", id);
    if (blockinfo.is_err()) {
        return blockinfo.unwrap_error();
    }
    auto block_info = blockinfo.unwrap()->as<std::vector<BlockInfo>>();
    auto num_block = (data.size() + offset % DiskBlockSize) / DiskBlockSize;
    num_block += (data.size() + offset % DiskBlockSize) % DiskBlockSize > 0;
    auto start_pos = offset / DiskBlockSize;

    for (auto i = block_info.size(); i < start_pos + num_block; ++i) {
        auto res = metadata_server_->call("alloc_block", id);
        if (res.is_err()) {
            return res.unwrap_error();
        }
        auto new_block = res.unwrap()->as<BlockInfo>();
        block_info.push_back(new_block);
    }

    auto res = data_servers_[std::get<1>(block_info[start_pos])]->call("write_data",
                                       std::get<0>(block_info[start_pos]), offset % DiskBlockSize, data);
    auto curr_pos = DiskBlockSize - offset % DiskBlockSize;
    for (auto i = start_pos + 1; i < start_pos + num_block; ++i) {
        std::vector<u8> buffer(data.begin()+curr_pos, data.end());
        data_servers_[std::get<1>(block_info[i])]->call("write_data",
                                                           std::get<0>(block_info[i]), 0, buffer);
        curr_pos += DiskBlockSize;
    }
    return KNullOk;

}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  UNIMPLEMENTED();
  return KNullOk;
}

} // namespace chfs
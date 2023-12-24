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
    auto ret_val = res.unwrap()->as<inode_id_t>();
    if(ret_val == KInvalidInodeID){
        return ChfsResult<inode_id_t>{ErrorType::AlreadyExist};
    }
    return res.unwrap()->get().as<chfs::inode_id_t>();
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
    auto res = metadata_server_->call("unlink", parent, name);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
    auto res = metadata_server_->call("lookup", parent, name);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    auto ret_val = res.unwrap()->as<inode_id_t>();
    if(ret_val == KInvalidInodeID){
        return ChfsResult<inode_id_t>{ErrorType::NotExist};
    }
    return res.unwrap()->get().as<chfs::inode_id_t>();
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
    auto res = metadata_server_->call("readdir", id);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    auto ret_val = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
    if(ret_val.empty()){
        return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>{ErrorType::NotExist};
    }
    return res.unwrap()->get().as<std::vector<std::pair<std::string, inode_id_t>>>();
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
    auto res = metadata_server_->call("get_type_attr", id);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    auto ret_val = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
    InodeType inodeType = (InodeType)std::get<4>(ret_val);
    FileAttr fileAttr = {std::get<1>(ret_val), std::get<2>(ret_val), std::get<3>(ret_val), std::get<0>(ret_val)};
    return ChfsResult<std::pair<InodeType, FileAttr>>{std::make_pair(inodeType, fileAttr)};
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
    auto has_read = 0;
    for (auto i = start_pos; i < start_pos + num_block; ++i) {
        auto res = data_servers_[std::get<1>(block_info[i])]->call("read_data",
                                   std::get<0>(block_info[i]), curr_pos, DiskBlockSize - curr_pos,std::get<2>(block_info[i]));
        if (res.is_err()) {
            return res.unwrap_error();
        }
        auto buffer = res.unwrap()->as<std::vector<u8>>();
        if (buffer.size() > size - has_read) {
            read_res.insert(read_res.end(), buffer.begin(), buffer.begin() + size - has_read);
            has_read += size - has_read;
        }
        else {
            read_res.insert(read_res.end(), buffer.begin(), buffer.end());
            has_read += DiskBlockSize - curr_pos;
        }
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
    auto free_call = metadata_server_->call("free_block",id,block_id,mac_id);
    if(free_call.is_err()){
        return ChfsNullResult {free_call.unwrap_error()};
    }
    return KNullOk;
}

} // namespace chfs
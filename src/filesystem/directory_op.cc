#include <algorithm>
#include <sstream>
#include <regex>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  DirectoryEntry entry;
  auto list = std::list<DirectoryEntry>();
  entry.name = filename;
  entry.id = id;
  list.push_back(entry);
  if(src.length())
    src += "/";
  src += dir_list_to_string(list);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
    if(src.length() == 0)
      return;
    std::regex split("/"), split_name(":");
    std::sregex_token_iterator pos(src.begin(), src.end(), split, -1);
    decltype(pos) end;
    for (; pos != end; ++pos) {
        std::string temp = pos->str();
        std::sregex_token_iterator pos_name(temp.begin(), temp.end(), split_name, -1);
        DirectoryEntry entry;
        entry.name = pos_name->str();
        ++pos_name;
        std::string temp_id = pos_name->str();
        entry.id = string_to_inode_id(temp_id);
        list.push_back(entry);
    }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (auto iter = list.begin(); iter != list.end(); ++iter)
  {
      if (strcmp(iter->name.c_str(), filename.c_str()) == 0)
      {
          list.erase(iter);
          break;
      }
  }
  res =  dir_list_to_string(list);

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  auto res = fs->read_file(id);
  if(res.is_err())
      return res.unwrap_error();
  std::vector<u8> content = res.unwrap();
  std::string src(content.begin(), content.end());
  parse_directory(src, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  read_directory(this, id, list);
  for (auto iter = list.begin(); iter != list.end(); ++iter)
  {
      if (strcmp(iter->name.c_str(), name) == 0)
      {
          return ChfsResult<inode_id_t>(iter->id);
      }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type, std::vector<std::shared_ptr<BlockOperation>> *ops)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  if(lookup(id, name).is_ok())
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);

  // 2. Create the new inode.
  auto alloc_res = alloc_inode(type, ops);
  if(alloc_res.is_err())
      return alloc_res.unwrap_error();

  // 3. Append the new entry to the parent directory.
  auto read_res = read_file(id);
  if(read_res.is_err())
      return read_res.unwrap_error();

  std::vector<u8> content = read_res.unwrap();
  std::string src = std::string(content.begin(), content.end());
  std::string append_src = append_to_directory(src, name, alloc_res.unwrap());

  auto write_res = write_file(id, std::vector<u8>(append_src.begin(), append_src.end()), ops);
  if(write_res.is_err())
      return write_res.unwrap_error();
  return ChfsResult<inode_id_t>(alloc_res.unwrap());
//  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto lookup_res = lookup(parent, name);
  if(lookup_res.is_err())
      return lookup_res.unwrap_error();

  auto remove_res = remove_file(lookup_res.unwrap());
  if(remove_res.is_err())
      return remove_res.unwrap_error();

  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, parent, list);
  if(read_res.is_err())
      return read_res.unwrap_error();
  for (auto iter = list.begin(); iter != list.end(); ++iter)
  {
      if (strcmp(iter->name.c_str(), name) == 0)
      {
          list.erase(iter);
          break;
      }
  }
  std::string src = dir_list_to_string(list);

  auto write_res = write_file(parent, std::vector<u8>(src.begin(), src.end()));
  if(write_res.is_err())
      return write_res.unwrap_error();
  
  return KNullOk;
}

} // namespace chfs

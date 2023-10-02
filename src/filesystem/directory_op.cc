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
  std::string src = std::string(res.unwrap().begin(), res.unwrap().end());
  parse_directory(src, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  UNIMPLEMENTED();

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  UNIMPLEMENTED();

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(0));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  UNIMPLEMENTED();
  
  return KNullOk;
}

} // namespace chfs

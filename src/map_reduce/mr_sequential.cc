#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<std::string> contents;
        std::vector<KeyVal> result;
        for (auto &file : files) {
            /* Deal with data */

            std::cout << "start finding: "<<file << std::endl;
            auto res_lookup = chfs_client->lookup(1, file);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());
            /* Map */
            auto res_map = Map(content);
            /* Reduce */
            //sort
            std::sort(res_map.begin(), res_map.end(), KeyVal::cmp);
            //reduce
            std::string last_key = "";
            std::vector<std::string> values;
            for (auto &key_val : res_map) {
                if (key_val.key != last_key) {
                    if (!last_key.empty())
                        result.emplace_back(last_key, Reduce(last_key, values));
                    last_key = key_val.key;
                    values.clear();
                }
                values.emplace_back(key_val.val);
            }
        }
        // Reduce results
        std::sort(result.begin(), result.end(), KeyVal::cmp);
        std::string last_key = "";
        std::vector<std::string> values;
        std::stringstream dump_buffer;
        for (auto &key_val : result) {
            if (key_val.key != last_key) {
                if (!last_key.empty()) {
                    dump_buffer << last_key << " " << Reduce(last_key, values) << std::endl;
                }
                last_key = key_val.key;
                values.clear();
            }
            values.emplace_back(key_val.val);
        }
        /* Write to file */
        auto res_lookup = chfs_client->lookup(1, outPutFile);
        auto inode_id = res_lookup.unwrap();
        std::string content = dump_buffer.str();
        std::vector<chfs::u8> vec(content.begin(), content.end());
        chfs_client->write_file(inode_id, 0, vec);
    }
}
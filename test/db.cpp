#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <string_view>

#include "pbt/environment.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

uint64_t div_ceil(uint64_t x, uint64_t y)
{
    return (x + y - 1) / y;
}

std::vector<std::string> generate_keys_sequence(uint64_t count)
{
    std::vector<std::string> keys;
    for (uint64_t i = 0; i < count; i++)
    {
        keys.push_back("key_" + std::to_string(i));
    }
    std::sort(keys.begin(), keys.end());
    return keys;
}

std::vector<std::string> generate_values_sequence(uint64_t count)
{
    std::vector<std::string> values;
    for (uint64_t i = 0; i < count; i++)
    {
        values.push_back("value_" + std::to_string(i));
    }
    std::sort(values.begin(), values.end());
    return values;
}

int main()
{
    constexpr uint64_t branch_factor = 4;
    std::string path = "test.pbt";

    tendb::pbt::Environment env(path);
    env.init();

    tendb::pbt::Writer writer(env);
    writer.append_header();
    uint64_t begin_key_value_offset = writer.get_offset();

    std::vector<std::string> keys = generate_keys_sequence(100);
    std::vector<std::string> values = generate_values_sequence(100);
    uint32_t num_entries = 0;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        writer.append_entry(keys[i], values[i]);
        ++num_entries;
    }
    uint64_t begin_node_offset = writer.get_offset();

    tendb::pbt::KeyValueItemScanner kv_scanner(reinterpret_cast<char *>(env.get_address()) + begin_key_value_offset, begin_key_value_offset);
    uint64_t num_leaf_nodes = 0;
    uint64_t last_node_offset = 0;
    for (size_t i = 0; i < keys.size(); i += branch_factor)
    {
        uint32_t entry_start = static_cast<uint32_t>(i);
        uint32_t entry_end = static_cast<uint32_t>(std::min(i + branch_factor, keys.size()));
        last_node_offset = writer.get_offset();
        writer.append_leaf_node(entry_start, entry_end, kv_scanner);
        ++num_leaf_nodes;
    }

    tendb::pbt::NodeScanner node_scanner(reinterpret_cast<char *>(env.get_address()) + begin_node_offset, begin_node_offset);
    uint64_t prev_depth_num_nodes = num_leaf_nodes;
    uint32_t num_internal_nodes = 0;
    uint32_t depth = 0;
    while (prev_depth_num_nodes > 1)
    {
        for (size_t i = 0; i < prev_depth_num_nodes; i += branch_factor)
        {
            uint32_t child_start = static_cast<uint32_t>(i);
            uint32_t child_end = static_cast<uint32_t>(std::min(i + branch_factor, prev_depth_num_nodes));
            last_node_offset = writer.get_offset();
            writer.append_internal_node(child_start, child_end, node_scanner);
            ++num_internal_nodes;
        }

        prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, branch_factor);
        ++depth;
    }

    writer.overwrite_header(depth, num_leaf_nodes, num_internal_nodes, num_entries, last_node_offset);

    tendb::pbt::Reader reader(env);

    for (const auto &key : keys)
    {
        tendb::pbt::KeyValueItem *entry = reader.get(key);
        if (entry)
        {
            std::cout << "Found entry: " << key << " -> " << entry->value() << std::endl;
        }
        else
        {
            std::cout << "Entry not found: " << key << std::endl;
        }
    }
}

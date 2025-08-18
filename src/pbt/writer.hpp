#pragma once

#include "pbt/environment.hpp"

namespace tendb::pbt
{
    uint64_t div_ceil(uint64_t x, uint64_t y)
    {
        return (x + y - 1) / y;
    }

    struct Writer
    {
        Environment &env;
        Appender appender;
        uint64_t branch_factor;
        uint64_t begin_key_value_items_offset;
        uint64_t num_items;

        Writer(Environment &env, uint64_t branch_factor) : env(env), appender(env), branch_factor(branch_factor)
        {
            env.init();
            appender.append_header();
            begin_key_value_items_offset = appender.get_offset();
            num_items = 0;
        }

        void add(const std::string_view &key, const std::string_view &value)
        {
            appender.append_item(key, value);
            ++num_items;
        }

        void finish()
        {
            uint64_t num_leaf_nodes = div_ceil(num_items, branch_factor);
            uint64_t begin_node_offset = appender.get_offset();

            tendb::pbt::KeyValueItemScanner kv_scanner(env, begin_key_value_items_offset);
            tendb::pbt::NodeScanner node_scanner(env, begin_node_offset);

            uint64_t last_node_offset = 0;
            for (uint64_t i = 0; i < num_items; i += branch_factor)
            {
                uint32_t item_start = static_cast<uint32_t>(i);
                uint32_t item_end = static_cast<uint32_t>(std::min(i + branch_factor, num_items));
                last_node_offset = appender.get_offset();
                appender.append_leaf_node(item_start, item_end, kv_scanner);
            }

            uint64_t prev_depth_num_nodes = num_leaf_nodes;
            uint32_t num_internal_nodes = 0;
            uint32_t depth = 0;
            while (prev_depth_num_nodes > 1)
            {
                for (size_t i = 0; i < prev_depth_num_nodes; i += branch_factor)
                {
                    uint32_t child_start = static_cast<uint32_t>(i);
                    uint32_t child_end = static_cast<uint32_t>(std::min(i + branch_factor, prev_depth_num_nodes));
                    last_node_offset = appender.get_offset();
                    appender.append_internal_node(child_start, child_end, node_scanner);
                }

                prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, branch_factor);
                num_internal_nodes += prev_depth_num_nodes;
                ++depth;
            }

            appender.overwrite_header(depth, num_leaf_nodes, num_internal_nodes, num_items, last_node_offset);
        }
    };
}

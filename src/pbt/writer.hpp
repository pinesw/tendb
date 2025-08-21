#pragma once

#include <cstdint>
#include <ranges>
#include <string_view>
#include <vector>

#include "pbt/appender.hpp"
#include "pbt/environment.hpp"
#include "pbt/format.hpp"

namespace tendb::pbt
{
    uint64_t div_ceil(uint64_t x, uint64_t y)
    {
        return (x + y - 1) / y;
    }

    struct Writer
    {
        Environment &environment;
        Appender appender;
        uint64_t begin_key_value_items_offset;
        uint64_t num_items;

        Writer(Environment &environment) : environment(environment), appender(environment.storage) {}

        void init()
        {
            appender.append_header();
            begin_key_value_items_offset = appender.get_offset();
            num_items = 0;
        }

        Header *get_header() const
        {
            return reinterpret_cast<Header *>(environment.storage.get_address());
        }

        void add(const std::string_view &key, const std::string_view &value)
        {
            appender.append_item(key, value);
            ++num_items;
        }

        void finish()
        {
            uint64_t num_leaf_nodes = div_ceil(num_items, environment.options.branch_factor);
            uint64_t first_node_offset = appender.get_offset();

            get_header()->first_node_offset = first_node_offset;
            get_header()->begin_key_value_items_offset = begin_key_value_items_offset;

            tendb::pbt::KeyValueItem::Iterator kv_itr{environment.storage, begin_key_value_items_offset};
            tendb::pbt::Node::Iterator node_itr{environment.storage, first_node_offset};

            uint64_t last_node_offset = 0;
            for (uint64_t i = 0; i < num_items; i += environment.options.branch_factor)
            {
                uint32_t item_start = static_cast<uint32_t>(i);
                uint32_t item_end = static_cast<uint32_t>(std::min(i + environment.options.branch_factor, num_items));
                last_node_offset = appender.get_offset();
                appender.append_leaf_node(item_start, item_end, kv_itr);
            }

            uint64_t prev_depth_num_nodes = num_leaf_nodes;
            uint32_t num_internal_nodes = 0;
            uint32_t depth = 0;
            while (prev_depth_num_nodes > 1)
            {
                for (size_t i = 0; i < prev_depth_num_nodes; i += environment.options.branch_factor)
                {
                    uint32_t child_start = static_cast<uint32_t>(i);
                    uint32_t child_end = static_cast<uint32_t>(std::min(i + environment.options.branch_factor, prev_depth_num_nodes));
                    last_node_offset = appender.get_offset();
                    appender.append_internal_node(child_start, child_end, node_itr);
                }

                prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, environment.options.branch_factor);
                num_internal_nodes += prev_depth_num_nodes;
                ++depth;
            }

            get_header()->depth = depth;
            get_header()->num_leaf_nodes = num_leaf_nodes;
            get_header()->num_internal_nodes = num_internal_nodes;
            get_header()->num_items = num_items;
            get_header()->root_offset = last_node_offset;

            environment.storage.set_size(appender.get_offset());
        }

        template <std::ranges::random_access_range T>
            requires std::same_as<std::ranges::range_value_t<T>, const Environment *>
        static void merge(const T &sources, Environment &target)
        {
            Writer writer(target);
            writer.init();

            uint64_t total_items = 0;
            std::vector<KeyValueItem::Iterator> iterators;
            std::vector<KeyValueItem::Iterator> ends;

            for (const Environment *source : sources)
            {
                Header *header = reinterpret_cast<Header *>(source->storage.get_address());
                total_items += header->num_items;
                iterators.emplace_back(source->storage, header->begin_key_value_items_offset);
                ends.emplace_back(source->storage, header->first_node_offset);
            }

            for (uint64_t i = 0; i < total_items; ++i)
            {
                uint64_t min_index;
                std::string_view min_key;

                for (uint64_t j = 0; j < iterators.size(); ++j)
                {
                    if (iterators[j] == ends[j])
                    {
                        continue;
                    }
                    if (min_key.empty() || target.options.compare_fn((*iterators[j])->key(), min_key) < 0)
                    {
                        min_index = j;
                        min_key = (*iterators[j])->key();
                    }
                }

                writer.add(min_key, (*iterators[min_index])->value());
                ++iterators[min_index];
            }

            writer.finish();
        }
    };
}

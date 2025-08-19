#pragma once

#include <cstdint>
#include <string_view>

#include "pbt/appender.hpp"
#include "pbt/environment.hpp"
#include "pbt/formats.hpp"

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

        Writer(Environment &environment) : environment(environment), appender(environment.storage)
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

        // TODO: add variadic merge (>= 2 sources)

        static void merge(const Environment &source_a, const Environment &source_b, Environment &target)
        {
            Header *header_a = reinterpret_cast<Header *>(source_a.storage.get_address());
            Header *header_b = reinterpret_cast<Header *>(source_b.storage.get_address());
            KeyValueItem::Iterator itr_a{source_a.storage, header_a->begin_key_value_items_offset};
            KeyValueItem::Iterator itr_b{source_b.storage, header_b->begin_key_value_items_offset};
            KeyValueItem::Iterator end_a{source_a.storage, header_a->first_node_offset};
            KeyValueItem::Iterator end_b{source_b.storage, header_b->first_node_offset};

            Writer writer(target);

            uint64_t total_items = header_a->num_items + header_b->num_items;
            for (uint64_t i = 0; i < total_items; ++i)
            {
                if (itr_a == end_a)
                {
                    const KeyValueItem *item_b = *itr_b;
                    writer.add(item_b->key(), item_b->value());
                    ++itr_b;
                }
                else if (itr_b == end_b)
                {
                    const KeyValueItem *item_a = *itr_a;
                    writer.add(item_a->key(), item_a->value());
                    ++itr_a;
                }
                else
                {
                    const KeyValueItem *item_a = *itr_a;
                    const KeyValueItem *item_b = *itr_b;
                    if (target.options.compare_fn(item_a->key(), item_b->key()) <= 0)
                    {
                        writer.add(item_a->key(), item_a->value());
                        ++itr_a;
                    }
                    else
                    {
                        writer.add(item_b->key(), item_b->value());
                        ++itr_b;
                    }
                }
            }

            writer.finish();
        }
    };
}

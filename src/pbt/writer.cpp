#include <cstdint>
#include <string_view>

#include "pbt/appender.hpp"
#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/storage.hpp"
#include "pbt/writer.hpp"

static uint64_t div_ceil(uint64_t x, uint64_t y)
{
    return (x + y - 1) / y;
}

tendb::pbt::Writer::Writer(Storage &storage, const Options &options) : storage(storage), appender(storage), options(options)
{
    appender.append_header();
    begin_key_value_items_offset = appender.get_offset();
    num_items = 0;
}

tendb::pbt::Header *tendb::pbt::Writer::get_header() const
{
    return reinterpret_cast<Header *>(storage.get_address());
}

const tendb::pbt::Options &tendb::pbt::Writer::get_options()
{
    return options;
}

void tendb::pbt::Writer::add(const std::string_view &key, const std::string_view &value)
{
    appender.append_item(key, value);
    ++num_items;
}

void tendb::pbt::Writer::finish()
{
    uint64_t num_leaf_nodes = div_ceil(num_items, options.branch_factor);
    uint64_t first_node_offset = appender.get_offset();

    get_header()->first_node_offset = first_node_offset;
    get_header()->begin_key_value_items_offset = begin_key_value_items_offset;

    tendb::pbt::KeyValueItem::Iterator kv_itr(storage, begin_key_value_items_offset);
    tendb::pbt::Node::Iterator node_itr(storage, first_node_offset);

    uint64_t last_node_offset = 0;
    for (uint64_t i = 0; i < num_items; i += options.branch_factor)
    {
        uint32_t item_start = static_cast<uint32_t>(i);
        uint32_t item_end = static_cast<uint32_t>(std::min(i + options.branch_factor, num_items));
        last_node_offset = appender.get_offset();
        appender.append_leaf_node(item_start, item_end, kv_itr);
    }

    uint64_t prev_depth_num_nodes = num_leaf_nodes;
    uint32_t num_internal_nodes = 0;
    uint32_t depth = 0;
    while (prev_depth_num_nodes > 1)
    {
        for (size_t i = 0; i < prev_depth_num_nodes; i += options.branch_factor)
        {
            uint32_t child_start = static_cast<uint32_t>(i);
            uint32_t child_end = static_cast<uint32_t>(std::min(i + options.branch_factor, prev_depth_num_nodes));
            last_node_offset = appender.get_offset();
            appender.append_internal_node(child_start, child_end, node_itr);
        }

        prev_depth_num_nodes = div_ceil(prev_depth_num_nodes, options.branch_factor);
        num_internal_nodes += prev_depth_num_nodes;
        ++depth;
    }

    get_header()->depth = depth;
    get_header()->num_leaf_nodes = num_leaf_nodes;
    get_header()->num_internal_nodes = num_internal_nodes;
    get_header()->num_items = num_items;
    get_header()->root_offset = last_node_offset;

    storage.set_size(appender.get_offset());
}

#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>

#include "pbt/format.hpp"
#include "pbt/options.hpp"
#include "pbt/storage.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

#include "pbt/environment.hpp"

tendb::pbt::Environment::Environment(const std::string &path, const Options &opts) : path(path), options(opts), storage(path, false) {}

void tendb::pbt::Environment::merge(const Reader **readers, size_t num_readers, Writer &target)
{
    uint64_t total_items = 0;
    std::vector<KeyValueItem::Iterator> iterators;
    std::vector<KeyValueItem::Iterator> ends;

    for (size_t i = 0; i < num_readers; ++i)
    {
        const Reader *source = readers[i];
        Header *header = source->get_header();
        total_items += header->num_items;
        iterators.emplace_back(source->begin());
        ends.emplace_back(source->end());
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
            if (min_key.empty() || target.get_options().compare_fn((*iterators[j])->key(), min_key) < 0)
            {
                min_index = j;
                min_key = (*iterators[j])->key();
            }
        }

        target.add(min_key, (*iterators[min_index])->value());
        ++iterators[min_index];
    }

    target.finish();
}

const tendb::pbt::Reader tendb::pbt::Environment::reader()
{
    storage.set_read_only(true);
    return Reader(storage, options);
}

tendb::pbt::Writer tendb::pbt::Environment::writer()
{
    storage.set_read_only(false);
    return Writer(storage, options);
}

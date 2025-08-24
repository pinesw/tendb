#pragma once

#include <cstdint>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>

#include "pbt/options.hpp"
#include "pbt/storage.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

namespace tendb::pbt
{
    struct Environment
    {
        const std::string path;
        const Options options;
        Storage storage;

        Environment(const std::string &path, const Options &opts = Options()) : path(path), options(opts), storage(path, false) {}

        const Reader reader() const
        {
            return Reader(storage, options);
        }

        Writer writer()
        {
            return Writer(storage, options);
        }

        template <std::ranges::random_access_range T>
            requires std::same_as<std::ranges::range_value_t<T>, const Reader *>
        static void merge(const T &sources, Writer &target)
        {
            uint64_t total_items = 0;
            std::vector<KeyValueItem::Iterator> iterators;
            std::vector<KeyValueItem::Iterator> ends;

            for (const auto *source : sources)
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

                target.add(min_key, (*iterators[min_index])->value());
                ++iterators[min_index];
            }

            target.finish();
        }
    };
}

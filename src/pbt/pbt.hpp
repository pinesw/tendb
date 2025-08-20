#pragma once

#include <cstdint>
#include <functional>
#include <ranges>
#include <string_view>
#include <vector>

#include "pbt/environment.hpp"
#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

namespace tendb::pbt
{
    struct PBT
    {
        Environment environment;
        Reader reader;
        Writer writer;

        PBT(const Options &opts = Options()) : environment(opts), reader(environment), writer(environment)
        {
            environment.storage.init();
        }

        void add(const std::string_view &key, const std::string_view &value)
        {
            writer.add(key, value);
        }

        void finish()
        {
            writer.finish();
        }

        template <std::ranges::random_access_range T>
            requires std::same_as<std::ranges::range_value_t<T>, const PBT *>
        static void merge(const T &sources, PBT &target)
        {
            std::vector<const Environment *> envs;
            for (const auto &source : sources)
            {
                envs.push_back(&source->environment);
            }
            Writer::merge(envs, target.environment);
        }

        const KeyValueItem::Iterator begin() const
        {
            return reader.begin();
        }

        const KeyValueItem::Iterator end() const
        {
            return reader.end();
        }

        const KeyValueItem::Iterator seek(const std::string_view &key) const
        {
            return reader.seek(key);
        }

        const KeyValueItem *get(const std::string_view &key) const
        {
            return reader.get(key);
        }
    };
}

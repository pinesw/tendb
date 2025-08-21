#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

#include "pbt/storage.hpp"

namespace tendb::pbt
{
    typedef std::function<int(const std::string_view &, const std::string_view &)> compare_fn_t;

    compare_fn_t compare_lexically = [](const std::string_view &a, const std::string_view &b)
    {
        return a.compare(b);
    };

    struct Options
    {
        uint32_t branch_factor = 8;
        compare_fn_t compare_fn = compare_lexically;
    };

    struct Environment
    {
        std::string path = "default.pbt";
        Options options;
        Storage storage;

        Environment(const std::string &path, const Options &opts) : path(path), options(opts), storage(path) {}
    };
}

#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

namespace tendb::pbt
{
    typedef std::function<int(const std::string_view &, const std::string_view &)> compare_fn_t;

    // template <std::ranges::random_access_range T>
    //     requires std::same_as<std::ranges::range_value_t<T>, const std::string_view *>
    // typedef std::function<const std::string(const T &values)> aggregate_fn_t;

    // template <std::ranges::random_access_range T>
    //     requires std::same_as<std::ranges::range_value_t<T>, const PBT *>
    // static void merge(const T &sources, PBT &target);

    compare_fn_t compare_lexically = [](const std::string_view &a, const std::string_view &b)
    {
        return a.compare(b);
    };

    struct Options
    {
        uint32_t branch_factor = 8;
        compare_fn_t compare_fn = compare_lexically;
    };
}

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "port.hpp"

namespace tendb::core_local
{
    static size_t get_size()
    {
        size_t num_cpus = static_cast<size_t>(std::thread::hardware_concurrency());
        size_t size_shift = 3;
        while (1 << size_shift < num_cpus)
        {
            ++size_shift;
        }
        return static_cast<size_t>(1) << size_shift;
    }

    static size_t size = get_size();

    static size_t get_size_mask()
    {
        return size - 1;
    }

    static size_t size_mask = get_size_mask();

    static size_t round_robin_index = 0;

    static size_t access_index()
    {
        int cpuid = port::physical_core_id();
        if (cpuid < 0)
        {
            return round_robin_index++ & size_mask;
        }
        else
        {
            return cpuid & size_mask;
        }
    }

    template <typename T>
    class CoreLocalArray
    {
    private:
        std::unique_ptr<T[]> data = std::make_unique<T[]>(size);

    public:
        size_t get_size() const
        {
            return size;
        }

        T *access() const
        {
            return access_element_and_index().first;
        }

        std::pair<T *, size_t> access_element_and_index() const
        {
            size_t core_idx = access_index();
            return {access_at_core(core_idx), core_idx};
        }

        T *access_at_core(size_t core_idx) const
        {
            assert(core_idx < size && "Core index out of bounds");

            return &data[core_idx];
        }
    };
}

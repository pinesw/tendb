#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "port.hpp"

namespace tendb::core_local
{
    template <typename T>
    class core_local_array
    {
    private:
        std::unique_ptr<T[]> data;
        size_t size;
        size_t size_mask;
        size_t round_robin_index = 0;

    public:
        core_local_array()
        {
            size_t num_cpus = static_cast<size_t>(std::thread::hardware_concurrency());
            size_t size_shift = 3;
            while (1 << size_shift < num_cpus)
            {
                ++size_shift;
            }
            size = static_cast<size_t>(1) << size_shift;
            size_mask = size - 1;
            data.reset(new T[size]);
        }

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
            int cpuid = port::physical_core_id();
            size_t core_idx;
            if (cpuid < 0)
            {
                core_idx = round_robin_index++ & size_mask;
            }
            else
            {
                core_idx = cpuid & size_mask;
            }
            return {access_at_core(core_idx), core_idx};
        }

        T *access_at_core(size_t core_idx) const
        {
            assert(core_idx < size && "Core index out of bounds");

            return &data[core_idx];
        }
    };
}

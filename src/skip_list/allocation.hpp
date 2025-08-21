#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

#include "core_local.hpp"

namespace tendb::skip_list
{
    constexpr static size_t ALIGNMENT = alignof(std::max_align_t);
    static thread_local size_t cpu_id = core_local::access_index();

    struct BlockAllocator
    {
    private:
        constexpr static size_t BLOCK_SIZE = 4096;
        constexpr static size_t LARGE_ALLOCATION_THRESHOLD = BLOCK_SIZE / 4;

        std::deque<std::unique_ptr<char[]>> blocks;
        char *current_begin = nullptr;
        char *current_end = nullptr;

        bool is_large_allocation(size_t requested_size) const
        {
            return requested_size > LARGE_ALLOCATION_THRESHOLD;
        }

        char *new_block(size_t size)
        {
            ZoneScoped;

            assert(size > 0 && "Allocation size must be greater than zero");

            char *block = new char[size];
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            return block;
        }

        char *allocate_small(size_t requested_size)
        {
            ZoneScoped;

            assert(requested_size > 0 && "Allocation size must be greater than zero");
            assert(requested_size <= BLOCK_SIZE && "Allocation size exceeds block size");

            requested_size += -requested_size & (ALIGNMENT - 1); // add padding to align to ALIGNMENT
            size_t current_size = current_end - current_begin;

            if (requested_size > current_size)
            {
                char *block = new_block(BLOCK_SIZE);
                current_begin = block;
                current_end = block + BLOCK_SIZE;
            }

            char *address = current_begin;
            current_begin += requested_size;

            return address;
        }

    public:
        char *allocate(size_t requested_size)
        {
            ZoneScoped;

            assert(requested_size > 0 && "Allocation size must be greater than zero");

            if (is_large_allocation(requested_size))
            {
                return new_block(requested_size);
            }
            else
            {
                return allocate_small(requested_size);
            }
        }
    };

    struct CoreLocalShardAllocator
    {
    private:
        constexpr static size_t BLOCK_SIZE = 4096;

        struct Shard
        {
            BlockAllocator allocator;
            std::mutex mutex;
        };

        core_local::CoreLocalArray<Shard> shards;

    public:
        char *allocate(size_t requested_size)
        {
            ZoneScoped;

            assert(requested_size > 0 && "Allocation size must be greater than zero");

            Shard *shard = shards.access_at_core(cpu_id);

            if (!shard->mutex.try_lock())
            {
                cpu_id = core_local::access_index();
                shard = shards.access_at_core(cpu_id);
                shard->mutex.lock();
            }

            char *address = shard->allocator.allocate(requested_size);
            shard->mutex.unlock();
            return address;
        }
    };
}

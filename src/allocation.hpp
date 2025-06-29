#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

#include "core_local.hpp"

namespace tendb::allocation
{
    typedef std::function<char *(size_t)> AllocateFunction;
    constexpr static size_t ALIGNMENT = alignof(std::max_align_t);
    static thread_local size_t cpu_id = core_local::access_index();

    struct ConcurrentMalloc
    {
    private:
        std::mutex mutex;
        std::deque<std::unique_ptr<char[]>> blocks;

        char *new_block(size_t requested_size)
        {
            char *block = new char[requested_size];

            mutex.lock();
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            mutex.unlock();

            return block;
        }

    public:
        char *allocate(size_t requested_size)
        {
            assert(requested_size > 0 && "Allocation size must be greater than zero");

            return new_block(requested_size);
        }
    };

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
            assert(size > 0 && "Allocation size must be greater than zero");

            char *block = new char[size];
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            return block;
        }

        char *allocate_small(size_t requested_size)
        {
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

    struct ConcurrentSmallBlockAllocator
    {
    private:
        constexpr static size_t BLOCK_SIZE = 8192 * 4096;
        constexpr static size_t LARGE_ALLOCATION_THRESHOLD = BLOCK_SIZE / 4;
        constexpr static size_t SIZE_BITMASK = 0x00000000FFFFFFFF;
        constexpr static size_t INDEX_BITMASK = 0xFFFFFFFF00000000;
        constexpr static size_t INDEX_SHIFT = 32;

        std::deque<std::unique_ptr<char[]>> blocks;
        std::atomic<size_t> index_and_size{0};
        std::atomic_flag block_allocation = ATOMIC_FLAG_INIT;

        void allocate_block()
        {
            if (block_allocation.test_and_set(std::memory_order_relaxed))
            {
                block_allocation.wait(true, std::memory_order_relaxed);
                return;
            }

            char *block = new char[BLOCK_SIZE];
            size_t index = blocks.size();
            blocks.emplace_back(std::unique_ptr<char[]>(block));

            index_and_size.store((index << INDEX_SHIFT) | BLOCK_SIZE, std::memory_order_release);

            block_allocation.clear(std::memory_order_relaxed);
            block_allocation.notify_all();
        }

    public:
        bool is_large_allocation(size_t requested_size) const
        {
            return requested_size > LARGE_ALLOCATION_THRESHOLD;
        }

        char *allocate(size_t requested_size)
        {
            assert(requested_size > 0 && "Allocation size must be greater than zero");
            assert(requested_size <= BLOCK_SIZE && "Requested size exceeds block size");

            requested_size += -requested_size & (ALIGNMENT - 1); // add padding to align to ALIGNMENT

            size_t current_index_and_size;
            size_t current_size;

            bool succeeded = false;
            while (!succeeded)
            {
                current_index_and_size = index_and_size.load(std::memory_order_relaxed);
                current_size = current_index_and_size & SIZE_BITMASK;

                if (current_size < requested_size)
                {
                    allocate_block();
                }
                else
                {
                    succeeded = index_and_size.compare_exchange_strong(current_index_and_size, current_index_and_size - requested_size, std::memory_order_acquire, std::memory_order_relaxed);
                }
            }

            size_t current_index = (current_index_and_size & INDEX_BITMASK) >> INDEX_SHIFT;
            char *current_block = blocks[current_index].get();
            char *current_begin = current_block + BLOCK_SIZE - current_size;

            return current_begin;
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
            assert(requested_size > 0 && "Allocation size must be greater than zero");
            assert(requested_size <= BLOCK_SIZE && "Requested size exceeds block size");

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

    struct FixedSizeArena
    {
    private:
        std::unique_ptr<char[]> memory;
        char *current_begin;
        size_t remaining_size;

    public:
        FixedSizeArena(size_t size)
        {
            assert(size > 0 && "Arena size must be greater than zero");

            memory = std::make_unique<char[]>(size);
            current_begin = memory.get();
            remaining_size = size;
        }

        char *allocate(size_t requested_size)
        {
            assert(requested_size > 0 && "Allocation size must be greater than zero");
            assert(requested_size <= remaining_size && "Allocation exceeds arena size");

            char *address = current_begin;
            current_begin += requested_size;
            remaining_size -= requested_size;

            return address;
        }
    };
}

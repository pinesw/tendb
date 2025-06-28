#pragma once

#include <atomic>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>

namespace tendb::allocation
{
    typedef std::function<char *(size_t)> AllocateFunction;

    struct MallocAllocator
    {
    private:
        std::mutex mutex;
        std::deque<std::unique_ptr<char[]>> blocks;

        char *new_block(size_t size)
        {
            char *block = new char[size];

            mutex.lock();
            blocks.emplace_back(std::unique_ptr<char[]>(block));
            mutex.unlock();

            return block;
        }

    public:
        char *allocate(size_t size)
        {
            return new_block(size);
        }
    };

    struct BlockAlignedAllocatorShard
    {
        std::deque<std::unique_ptr<char[]>> blocks;
        std::mutex mutex;
        char *current_begin = nullptr;
        char *current_end = nullptr;
    };

    struct BlockAlignedAllocator
    {
    private:
        constexpr static size_t ALIGNMENT = alignof(std::max_align_t);
        constexpr static size_t NUM_SHARDS = 4;
        constexpr static size_t BLOCK_SIZE = 4096;
        constexpr static size_t LARGE_ALLOCATION_THRESHOLD = BLOCK_SIZE / 4;

        std::array<BlockAlignedAllocatorShard, NUM_SHARDS> shards;
        std::atomic<size_t> current_shard_index = 0;

        bool is_large_allocation(size_t size) const
        {
            return size > LARGE_ALLOCATION_THRESHOLD;
        }

        char *new_block(BlockAlignedAllocatorShard &shard, size_t size)
        {
            char *block = new char[size];
            shard.blocks.emplace_back(std::unique_ptr<char[]>(block));
            return block;
        }

        char *allocate_small(BlockAlignedAllocatorShard &shard, size_t size)
        {
            size_t padding = -(size_t)shard.current_begin & (ALIGNMENT - 1);
            size_t required_size = size + padding;
            size_t current_size = shard.current_end - shard.current_begin;

            if (required_size > current_size)
            {
                char *block = new_block(shard, BLOCK_SIZE);
                shard.current_begin = block;
                shard.current_end = block + BLOCK_SIZE;
                padding = -(size_t)shard.current_begin & (ALIGNMENT - 1);
            }

            char *address = shard.current_begin + padding;
            shard.current_begin += size;

            return address;
        }

    public:
        char *allocate(size_t size)
        {
            size_t shard_index = NUM_SHARDS > 1 ? current_shard_index.fetch_add(1) % NUM_SHARDS : 0;
            shards[shard_index].mutex.lock();

            char *result;
            if (is_large_allocation(size))
            {
                result = new_block(shards[shard_index], size);
            }
            else
            {
                result = allocate_small(shards[shard_index], size);
            }

            shards[shard_index].mutex.unlock();

            return result;
        }
    };
}

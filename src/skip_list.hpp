#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstring>
#include <immintrin.h>
#include <iostream>
#include <optional>
#include <random>
#include <string_view>

#include "allocation.hpp"

namespace tendb::skip_list
{
    // RNG for random level generation
    static thread_local std::mt19937_64 rng{std::random_device{}()}; // Initialize thread-local RNG with a random seed
    static thread_local std::uniform_real_distribution<double> dist{0.0, 1.0};

    /**
     * Data structure to hold key-value pairs in the skip list.
     */
    struct Data
    {
    private:
        constexpr static size_t FLAG_DELETED = 0x0000000000000001; // Tombstone flag for deleted nodes

        size_t key_size;
        size_t key_size_padded;
        size_t value_size;
        size_t flags;   // Flags for the data, currently only used for deletion
        char buffer[1]; // Placeholder for the key and value data at the end of the structure, to be allocated by the caller of the constructor

        // Disallow copy and move operations
        Data(const Data &) = delete;
        Data(Data &&) = delete;
        Data &operator=(const Data &) = delete;
        Data &operator=(Data &&) = delete;

    public:
        /**
         * Constructor to initialize the Data structure with a key and value.
         * The caller must ensure that the bytes allocated for this object are sufficient to hold the key and value.
         * To get the size needed for the Data structure, use the static Data::size method.
         */
        Data(std::string_view key, std::string_view value)
            : key_size(key.size()), value_size(value.size()), flags(0)
        {
            size_t padding = -key.size() & (32 - 1);
            key_size_padded = key.size() + padding;
            std::memcpy(buffer, key.data(), key.size());
            std::memset(buffer + key.size(), 0, padding);
            std::memcpy(buffer + key_size_padded, value.data(), value.size());
        }

        /**
         * Static method to calculate the size needed for the Data structure given a key and value.
         */
        static size_t size(std::string_view key, std::string_view value)
        {
            // Calculate the size needed for the Data structure
            size_t padding = -key.size() & (32 - 1);
            return sizeof(Data) - 1 + key.size() + padding + value.size();
        }

        std::string_view key() const
        {
            return std::string_view(buffer, key_size);
        }

        std::string_view value() const
        {
            return std::string_view(buffer + key_size_padded, value_size);
        }

        /**
         * Check if the data is marked as deleted.
         * When the data of a node is marked as deleted, it is not considered part of the skip list anymore.
         */
        bool is_deleted() const
        {
            return (flags & FLAG_DELETED) != 0;
        }

        /**
         * Mark the data as deleted.
         */
        void mark_deleted()
        {
            flags |= FLAG_DELETED;
        }
    };

    /**
     * Node structure for the skip list.
     * Each node contains a pointer to the data, a pointer to the next node at the same level, and a pointer to the node below it (if any).
     */
    struct alignas(alignof(std::max_align_t)) SkipListNode
    {
    private:
        Data *data;                       // Does not need to be atomic, because concurrent operations are allowed to race to set the data pointer
        std::atomic<SkipListNode *> next; // Wrapped in atomic for lock-free updates
        SkipListNode *down;               // Once part of the skip list, the down pointer is never changed, so it does not need to be atomic either

    public:
        SkipListNode(Data *data, SkipListNode *next, SkipListNode *down)
            : data(data), next(next), down(down) {}

        // Disallow copy and move operations
        SkipListNode(const SkipListNode &) = delete;
        SkipListNode &operator=(const SkipListNode &) = delete;
        SkipListNode(SkipListNode &&) = delete;
        SkipListNode &operator=(SkipListNode &&) = delete;

        /**
         * Set the next pointer of this node to a new node.
         * This method uses compare-and-exchange to ensure that concurrent insertions can correctly set the next pointer without loss of data.
         */
        bool set_next(SkipListNode *new_next, SkipListNode *prev_expected)
        {
            assert(new_next != nullptr && "Next node cannot be null");

            return next.compare_exchange_strong(prev_expected, new_next, std::memory_order_relaxed);
        }

        /**
         * Override the next pointer of this node without checking the previous value.
         * Use this method only when the node is guaranteed to not have any concurrent modifications.
         */
        void override_next(SkipListNode *new_next)
        {
            next.store(new_next, std::memory_order_relaxed);
        }

        void set_data(Data *data_ptr)
        {
            assert(data_ptr != nullptr && "Data pointer cannot be null");
            assert(data->key() == data_ptr->key() && "Data key must match the existing data key");

            data = data_ptr;
        }

        /**
         * Clear the next pointer of this node.
         *
         */
        void clear_next()
        {
            // Since clearing the skip list is not a concurrent operation, we don't need to CAS the next pointer
            next.store(nullptr, std::memory_order_relaxed);
        }

        SkipListNode *get_next() const
        {
            return next.load(std::memory_order_relaxed);
        }

        SkipListNode *get_down() const
        {
            return down;
        }

        Data *get_data() const
        {
            return data;
        }
    };

    /**
     * Skip list data structure for storing key-value pairs, sorted by keys.
     * Inserting the same key multiple times will update the value, rather than creating duplicate entries.
     * Supports concurrent insertion, deletion and reading.
     * Creating and destroying (clearing) the skip list is not thread-safe.
     * Memory allocated during the lifetime of the skip list is not freed until the skip list is destroyed.
     */
    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_HEIGHT = 16;            // The number of levels in the skip list
        constexpr static std::size_t MAX_LEVEL = MAX_HEIGHT - 1; // The maximum (top) level index, where level 0 is the bottom level (data level)
        constexpr static std::double_t BRANCH_PROBABILITY = 0.5; // The probability of branching to the next level when inserting a new node

        // Heads of the skip list at each level
        // The index corresponds to the level, with 0 being the bottom level
        std::array<SkipListNode *, MAX_HEIGHT> heads;

        // Allocator to manage memory for nodes and data
        // Using the CoreLocalShardAllocator gives performant multi-threaded allocations
        allocation::CoreLocalShardAllocator allocator;

    public:
        SkipList()
        {
            // Initialize the heads of the skip list
            // All head nodes point down to the next level, except for the bottom level (level 0)

            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                char *memory = allocator.allocate(sizeof(SkipListNode));
                if (i == 0)
                {
                    heads[i] = new (memory) SkipListNode{nullptr, nullptr, nullptr};
                }
                else
                {
                    heads[i] = new (memory) SkipListNode{nullptr, nullptr, heads[i - 1]};
                }
            }
        }

        SkipList(const SkipList &) = delete;            // Disallow copy construction
        SkipList &operator=(const SkipList &) = delete; // Disallow copy assignment

        /**
         * Move constructor for SkipList.
         * Not thread-safe: do not use this constructor while other operations on the skip list are in progress.
         */
        SkipList(SkipList &&other) noexcept
        {
            if (this != &other)
            {
                heads = std::move(other.heads);
                other.heads.fill(nullptr);
            }
        }

        /**
         * Move assignment operator for SkipList.
         * Not thread-safe: do not use this operator while other operations on the skip list are in progress.
         */
        SkipList &operator=(SkipList &&other) noexcept
        {
            if (this != &other)
            {
                heads = std::move(other.heads);
                other.heads.fill(nullptr);
            }
            return *this;
        }

        /**
         * Not thread-safe: before the skip list is destroyed, the caller has to ensure that all other operations on the skip list are finished.
         */
        ~SkipList()
        {
            // Make the skip list unusable by clearing the heads
            heads.fill(nullptr);
        }

        /**
         * Clear the skip list.
         * Not thread-safe: before clearing the skip list, the caller has to ensure that all other operations on the skip list are finished.
         */
        void clear()
        {
            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                // Clear from the top level down to the bottom
                // This ensures that we do not access deleted nodes
                size_t level = MAX_LEVEL - i;

                // Reset the head node at this level
                heads[level]->clear_next();
            }
        }

        /**
         * Delete a key from the skip list.
         * Iterators that are currently at the deleted key will still be able to read the data.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        void del(std::string_view key)
        {
            ZoneScoped;

            const SkipListNode *current = find_node(key);
            Data *data = current->get_data();
            data->mark_deleted();
        }

        /**
         * Insert a key-value pair into the skip list.
         * If the key already exists, the value is updated.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        void put(std::string_view key, std::string_view value)
        {
            ZoneScoped;

            // Allocate memory for the new data
            char *memory = allocator.allocate(Data::size(key, value));
            Data *data = new (memory) Data{key, value};

            // Find the approximate path to insert the new node
            std::array<SkipListNode *, MAX_HEIGHT> path_to_bottom = find_approximate_path(data->key());

            // Insert the new node at each level from 0 to a random level
            size_t level = random_level();
            SkipListNode *down_node = nullptr;

            for (size_t i = 0; i <= level; ++i)
            {
                // The path is built in reverse order (top to bottom), so we need to access it from the end (bottom)
                size_t path_index = MAX_LEVEL - i;

                SkipListNode *new_node;
                bool succeeded = false;
                while (!succeeded)
                {
                    SkipListNode *prev = path_to_bottom[path_index];
                    SkipListNode *prev_next = prev->get_next();

                    // Check that the new node's key is less than the next node's key
                    // During concurrent inserts, another thread may have inserted a node with a key less than the current key
                    if (prev_next != nullptr && key.compare(prev_next->get_data()->key()) >= 0)
                    {
                        // The next node's key is greater than or equal to the current key, so we cannot insert here
                        // We need to find the correct position again, starting from the new previous node

                        SkipListNode *current = prev_next;
                        SkipListNode *next_node;
                        while ((next_node = current->get_next()) != nullptr && key.compare(next_node->get_data()->key()) >= 0)
                        {
                            current = next_node;
                        }
                        path_to_bottom[path_index] = current;
                    }
                    else
                    {
                        // We found a valid position to insert the new node

                        // First check if the key already exists
                        if (i == 0 && prev->get_data() != nullptr && key.compare(prev->get_data()->key()) == 0)
                        {
                            // Key already exists, update the value
                            prev->set_data(data);

                            // No need to propagate up, just return
                            return;
                        }

                        char *memory = allocator.allocate(sizeof(SkipListNode));
                        new_node = new (memory) SkipListNode{data, nullptr, down_node};

                        // Try to set the next pointer of the previous node
                        new_node->override_next(prev_next);
                        succeeded = path_to_bottom[path_index]->set_next(new_node, prev_next);
                    }
                }

                // Before we move up to the next level, set the down pointer of the new node
                down_node = new_node;
            }
        }

        /**
         * Check if the skip list is empty.
         */
        bool is_empty() const
        {
            return heads[0] == nullptr || heads[0]->get_next() == nullptr;
        }

        /**
         * Iterator for the skip list.
         */
        struct Iterator
        {
            const SkipListNode *current;

            Iterator(const SkipListNode *node) : current(node) {}

            bool operator!=(const Iterator &other) const
            {
                return current != other.current;
            }

            bool operator==(const Iterator &other) const
            {
                return current == other.current;
            }

            /**
             * Increment the iterator to the next valid node.
             * This skips over deleted nodes.
             * If the current node is deleted after incrementing, the deleted data will remain accessible.
             * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
             */
            Iterator &operator++()
            {
                if (current != nullptr)
                {
                    current = current->get_next();
                }
                while (current != nullptr && current->get_data()->is_deleted())
                {
                    current = current->get_next();
                }

                return *this;
            }

            Data *operator*() const
            {
                return current->get_data();
            }

            Data *operator->() const
            {
                return current->get_data();
            }
        };

        /**
         * Return an iterator to the beginning of the skip list.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        Iterator begin() const
        {
            return Iterator(heads[0]->get_next());
        }

        /**
         * Return an iterator to the end of the skip list.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        Iterator end() const
        {
            return Iterator(nullptr);
        }

        /**
         * Seek for a key in the skip list.
         * Returns an iterator to the node with the given key if it exists and is not deleted.
         * Otherwise, returns an iterator to the end of the skip list.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        Iterator seek(std::string_view key) const
        {
            ZoneScoped;

            const SkipListNode *current = find_node(key);
            Data *data = current->get_data();
            if (data != nullptr && !data->is_deleted() && key.compare(data->key()) == 0)
            {
                return Iterator(current);
            }

            return end();
        }

        /**
         * Get the value associated with a key in the skip list.
         * Thread-safe: this method can be called concurrently with other thread-safe operations on the skip list.
         */
        std::optional<std::string_view> get(std::string_view key) const
        {
            ZoneScoped;

            auto it = seek(key);
            if (it != end())
            {
                return it->value();
            }
            return std::nullopt;
        }

    private:
        /**
         * Generate a random level for the new node.
         */
        size_t random_level()
        {
            size_t level = 0;
            while (level < MAX_LEVEL && dist(rng) < BRANCH_PROBABILITY)
            {
                level++;
            }
            return level;
        }

        /**
         * Find the node with the given key in the skip list.
         */
        const SkipListNode *find_node(std::string_view key) const
        {
            ZoneScoped;

            // Traverse the skip list from the top level down to the bottom
            const SkipListNode *current = heads[MAX_LEVEL];
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                const SkipListNode *next_node;

                // Move right in the current level until we find a node with a key greater than or equal to the target key
                while ((next_node = current->get_next()) != nullptr && key.compare(next_node->get_data()->key()) >= 0)
                {
                    current = next_node;
                }

                if (i < MAX_LEVEL)
                {
                    current = current->get_down();
                }
            }

            return current;
        }

        /**
         * Find the approximate path to a node with the given key in the skip list.
         * The path is an array of nodes at each level, starting from the top level down to the bottom.
         * At each level of the path, the node is the last node that has a key less than the target key.
         * The path is considered "approximate", because during concurrent inserts, the actual path may change.
         */
        std::array<SkipListNode *, MAX_HEIGHT> find_approximate_path(std::string_view key) const
        {
            ZoneScoped;

            // Find the approximate position to insert the new nodes at each level
            // The actual position can change due to concurrent inserts, which we will handle later
            std::array<SkipListNode *, MAX_HEIGHT> path_to_bottom;
            SkipListNode *current = heads[MAX_LEVEL];
            SkipListNode *next_node;
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                ZoneScoped;

                while (key_is_after_node(key, next_node = current->get_next()))
                // while ((next_node = current->get_next()) != nullptr && key.compare(next_node->get_data()->key()) >= 0)
                {
                    current = next_node;
                }
                path_to_bottom[i] = current;
                current = current->get_down();
            }
            return path_to_bottom;
        }

        static bool key_is_after_node(std::string_view key, const SkipListNode *node)
        {
            ZoneScoped;

            // TODO: benchmark this function and use it in other places where we compare keys

            if (node == nullptr)
            {
                return false;
            }

            // return node != nullptr && key.compare(node->get_data()->key()) >= 0;

            const char *a = key.data();
            const char *b = node->get_data()->key().data();
            size_t min_length = std::min(key.size(), node->get_data()->key().size());

            // Compare 32-byte chunks
            for (size_t i = 0; i < min_length; i += 32)
            {
                __m256i av = *reinterpret_cast<const __m256i *>(a + i);
                __m256i bv = *reinterpret_cast<const __m256i *>(b + i);

                __m256i cmp_gt = _mm256_cmpgt_epi8(av, bv);
                __m256i cmp_eq = _mm256_cmpeq_epi8(av, bv);
                __m256i cmp_xor = _mm256_xor_si256(cmp_gt, cmp_eq);
                uint32_t mask = _mm256_movemask_epi8(cmp_xor);
                int index = std::countr_one(mask);

                if (index + i < min_length && index != 32)
                {
                    return false;
                }
            }

            return key.size() > node->get_data()->key().size();
        }
    };
}

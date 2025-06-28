#pragma once

#include <array>
#include <cassert>
#include <iostream>
#include <optional>
#include <random>
#include <string_view>

#include "allocation.hpp"
// #include "packed_pair.hpp"

namespace tendb::skip_list
{
    // TODO: consider:
    // Remove value from skip list nodes, keep in a separate data structure (possibly also backed by custom allocator)
    // In fact, remove value from skip list entirely? Eventually we just need a pointer to some value data...

    struct Data
    {
    private:
        size_t key_size;
        size_t value_size;
        char buffer[1];

        Data(size_t key_size, size_t value_size) : key_size(key_size), value_size(value_size) {}

        Data(const Data &) = delete;
        Data(Data &&) = delete;
        Data &operator=(const Data &) = delete;
        Data &operator=(Data &&) = delete;

    public:
        static Data *create(std::string_view key, std::string_view value, allocation::AllocateFunction allocate)
        {
            // Allocate memory for the new Data and initialize it
            char *memory = allocate(sizeof(Data) + key.size() + value.size() - 1);
            Data *data = new (memory) Data{key.size(), value.size()};
            std::memcpy(data->buffer, key.data(), key.size());
            std::memcpy(data->buffer + key.size(), value.data(), value.size());
            return data;
        }

        std::string_view key() const
        {
            return std::string_view(buffer, key_size);
        }

        std::string_view value() const
        {
            return std::string_view(buffer + key_size, value_size);
        }
    };

    struct SkipListNode
    {
    private:
        Data *data;
        std::atomic<SkipListNode *> next;
        SkipListNode *down;

    public:
        SkipListNode(Data *data, SkipListNode *next, SkipListNode *down)
            : data(data), next(next), down(down) {}

        SkipListNode(const SkipListNode &) = delete;            // Disallow copy construction
        SkipListNode &operator=(const SkipListNode &) = delete; // Disallow copy assignment
        SkipListNode(SkipListNode &&) = delete;                 // Disallow move construction
        SkipListNode &operator=(SkipListNode &&) = delete;      // Disallow move assignment

        bool set_next(SkipListNode *new_next, SkipListNode *prev_expected)
        {
            assert(new_next != nullptr && "Next node cannot be null");

            return next.compare_exchange_strong(prev_expected, new_next, std::memory_order_relaxed);
        }

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

        void clear_next()
        {
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

    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_HEIGHT = 16;
        constexpr static std::size_t MAX_LEVEL = MAX_HEIGHT - 1;
        constexpr static std::double_t BRANCH_PROBABILITY = 0.5;

        // RNG for random level generation
        static thread_local std::mt19937_64 rng;
        std::uniform_real_distribution<double> dist{0.0, 1.0};

        // Heads of the skip list at each level
        // The index corresponds to the level, with 0 being the bottom level (data level)
        std::array<SkipListNode *, MAX_HEIGHT> heads;

        // Custom allocator to manage memory for nodes and data
        allocation::BlockAlignedAllocator allocator;
        allocation::AllocateFunction allocate = std::bind(&allocation::BlockAlignedAllocator::allocate, &allocator, std::placeholders::_1);

    public:
        SkipList()
        {
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

        SkipList(const SkipList &) = delete;                     // Disallow copy construction
        SkipList &operator=(const SkipList &) = delete;          // Disallow copy assignment
        SkipList &operator=(SkipList &&other) noexcept = delete; // Disallow move assignment

        SkipList(SkipList &&other) noexcept : heads(std::move(other.heads))
        {
            // Reset the moved-from skip list's heads to nullptr
            other.heads.fill(nullptr);
        }

        ~SkipList()
        {
            // Clear the skip list
            heads.fill(nullptr);
        }

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

        void put(std::string_view key, std::string_view value)
        {
            Data *data = Data::create(key, value, allocate);

            // Find the approximate position to insert the new nodes at each level
            // The actual position can change due to concurrent inserts, which we will handle later
            std::array<SkipListNode *, MAX_HEIGHT> history;
            SkipListNode *current = heads[MAX_HEIGHT - 1];
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                SkipListNode *next_node;
                while ((next_node = current->get_next()) != nullptr && key.compare(next_node->get_data()->key()) >= 0)
                {
                    current = next_node;
                }
                history[i] = current;
                current = current->get_down();
            }

            // Insert the new node at each level from 0 to a random level
            size_t level = random_level();
            SkipListNode *down_node = nullptr;

            for (size_t i = 0; i <= level; ++i)
            {
                // The history is built in reverse order (top to bottom), so we need to access it from the end (bottom)
                size_t history_level = MAX_LEVEL - i;

                SkipListNode *new_node;
                bool succeeded = false;
                while (!succeeded)
                {
                    SkipListNode *prev = history[history_level];
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
                        history[history_level] = current;
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
                        succeeded = history[history_level]->set_next(new_node, prev_next);
                    }
                }

                // Before we move up to the next level, set the down pointer of the new node
                down_node = new_node;
            }
        }

        bool is_empty() const
        {
            return heads[0]->get_next() == nullptr;
        }

        struct Iterator
        {
            SkipListNode *current;

            Iterator(SkipListNode *node) : current(node) {}

            bool operator!=(const Iterator &other) const
            {
                return current != other.current;
            }

            bool operator==(const Iterator &other) const
            {
                return current == other.current;
            }

            Iterator &operator++()
            {
                if (current != nullptr)
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

        Iterator begin() const
        {
            return Iterator(heads[0]->get_next());
        }

        Iterator end() const
        {
            return Iterator(nullptr);
        }

        Iterator seek(std::string_view key) const
        {
            SkipListNode *current = heads[MAX_HEIGHT - 1];

            // Traverse the skip list from the top level down to the bottom
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                SkipListNode *next_node;

                // Move right in the current level until we find a node with a key greater than or equal to the target key
                while ((next_node = current->get_next()) != nullptr && key.compare(next_node->get_data()->key()) >= 0)
                {
                    current = next_node;
                }

                if (i < MAX_HEIGHT - 1)
                {
                    current = current->get_down();
                }
            }

            if (current->get_data() != nullptr && key.compare(current->get_data()->key()) == 0)
            {
                return Iterator(current);
            }

            return end();
        }

        std::optional<std::string_view> get(std::string_view key) const
        {
            auto it = seek(key);
            if (it != end())
            {
                return it->value();
            }
            return std::nullopt;
        }

    private:
        size_t random_level()
        {
            size_t level = 0;
            while (dist(rng) < BRANCH_PROBABILITY && level < MAX_HEIGHT - 1)
            {
                level++;
            }
            return level;
        }
    };

    thread_local std::mt19937_64 SkipList::rng{std::random_device{}()}; // Initialize thread-local RNG with a random seed
}

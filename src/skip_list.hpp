#pragma once

#include <array>
#include <iostream>
#include <optional>
#include <random>
#include <string_view>

#include "allocation.hpp"
// #include "packed_pair.hpp"

namespace tendb::skip_list
{
    // TODO: remove value from skip list nodes, keep in a separate data structure (possibly also backed by custom allocator)
    // In fact, remove value from skip list entirely? Eventually we just need a pointer to some value data...

    // TODO: add thread safety by using atomic operations

    struct Data
    {
        size_t key_size;
        size_t value_size;
        char buffer[1];

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
        Data *data;
        SkipListNode *next;
        SkipListNode *down;
    };

    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_HEIGHT = 16;
        constexpr static std::size_t MAX_LEVEL = MAX_HEIGHT - 1;
        constexpr static std::double_t P = 0.5;

        // RNG for random level generation
        static thread_local std::mt19937_64 rng;
        std::uniform_real_distribution<double> dist{0.0, 1.0};

        // Heads of the skip list at each level
        // The index corresponds to the level, with 0 being the bottom level (data level)
        std::array<SkipListNode *, MAX_HEIGHT> heads;

        // Custom allocator to manage memory for nodes and data
        allocation::BlockAlignedAllocator allocator;

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
                heads[level]->next = nullptr;
            }
        }

        void put(std::string_view key, std::string_view value)
        {
            // Allocate memory for the new Data and initialize it
            char *memory = allocator.allocate(sizeof(Data) + key.size() + value.size() - 1);
            Data *data = new (memory) Data{key.size(), value.size(), {0}};
            std::memcpy(data->buffer, key.data(), key.size());
            std::memcpy(data->buffer + key.size(), value.data(), value.size());

            // Find the position to insert the new nodes at each level
            std::array<SkipListNode *, MAX_HEIGHT> history;
            SkipListNode *current = heads[MAX_HEIGHT - 1];
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                while (current->next != nullptr && key.compare(current->next->data->key()) >= 0)
                {
                    current = current->next;
                }
                history[i] = current;
                current = current->down;
            }

            // Check if the key already exists
            if (history[MAX_LEVEL]->data != nullptr && key.compare(history[MAX_LEVEL]->data->key()) == 0)
            {
                // Key already exists, update the value
                history[MAX_LEVEL]->data = data;
                return;
            }

            // Insert the new node at the bottom level
            size_t level = random_level();
            SkipListNode *new_node = nullptr;

            for (size_t i = 0; i <= level; ++i)
            {
                char *memory = allocator.allocate(sizeof(SkipListNode));
                new_node = history[MAX_LEVEL - i]->next = new (memory) SkipListNode{data, history[MAX_LEVEL - i]->next, new_node};
            }
        }

        bool is_empty() const
        {
            return heads[0]->next == nullptr;
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
                    current = current->next;
                }
                return *this;
            }

            Data *operator*() const
            {
                return current->data;
            }

            Data *operator->() const
            {
                return current->data;
            }
        };

        Iterator begin() const
        {
            return Iterator(heads[0]->next);
        }

        Iterator end() const
        {
            return Iterator(nullptr);
        }

        Iterator seek(std::string_view key) const
        {
            SkipListNode *current = heads[MAX_HEIGHT - 1];
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                while (current->next != nullptr && key.compare(current->next->data->key()) >= 0)
                {
                    current = current->next;
                }
                if (i < MAX_HEIGHT - 1)
                {
                    current = current->down;
                }
            }
            if (current->data != nullptr && key.compare(current->data->key()) == 0)
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
            while (dist(rng) < P && level < MAX_HEIGHT - 1)
            {
                level++;
            }
            return level;
        }
    };

    thread_local std::mt19937_64 SkipList::rng{std::random_device{}()}; // Initialize thread-local RNG with a random seed
}

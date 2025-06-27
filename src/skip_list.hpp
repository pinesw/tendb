#pragma once

#include <array>
#include <iostream>
#include <random>
#include <string_view>

#include "packed_pair.hpp"

namespace tendb::skip_list
{
    struct SkipListNode
    {
        packed_pair::PackedPair *data;
        SkipListNode *next;
        SkipListNode *down;
    };

    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_HEIGHT = 16;
        constexpr static std::size_t MAX_LEVEL = MAX_HEIGHT - 1;
        constexpr static std::double_t P = 0.5;

        std::mt19937_64 rng;
        std::uniform_real_distribution<double> dist;
        std::array<SkipListNode *, MAX_HEIGHT> heads;

        // TODO: add thread safety by using atomic operations

    public:
        SkipList(uint64_t seed = 0) : rng(std::mt19937_64(seed)), dist(0.0, 1.0)
        {
            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                if (i == 0)
                {
                    heads[i] = new SkipListNode{nullptr, nullptr, nullptr};
                }
                else
                {
                    heads[i] = new SkipListNode{nullptr, nullptr, heads[i - 1]};
                }
            }
        }

        SkipList(const SkipList &) = delete;            // Disallow copy construction
        SkipList &operator=(const SkipList &) = delete; // Disallow copy assignment

        SkipList(SkipList &&other) noexcept : rng(std::move(other.rng)), dist(std::move(other.dist)), heads(std::move(other.heads))
        {
            // Reset the moved-from skip list's heads to nullptr
            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                other.heads[i] = nullptr;
            }
        }

        SkipList &operator=(SkipList &&other) noexcept
        {
            if (this != &other)
            {
                // Move the data from the other skip list
                rng = std::move(other.rng);
                dist = std::move(other.dist);
                heads = std::move(other.heads);

                // Reset the moved-from skip list's heads to nullptr
                for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
                {
                    other.heads[i] = nullptr;
                }
            }
            return *this;
        }

        ~SkipList()
        {
            // Clear the skip list
            clear();

            // Delete all head nodes
            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                if (heads[i] != nullptr)
                {
                    delete heads[i];
                }
            }

            heads.fill(nullptr);
        }

        void clear()
        {
            for (std::size_t i = 0; i < MAX_HEIGHT; ++i)
            {
                // Clear from the top level down to the bottom
                // This ensures that we do not access deleted nodes
                size_t level = MAX_LEVEL - i;

                if (heads[level] == nullptr)
                {
                    continue; // Skip if the head is already null
                }

                SkipListNode *current = heads[level]->next;

                while (current != nullptr)
                {
                    // If this is the first level, delete the data
                    if (level == 0)
                    {
                        delete current->data;
                        current->data = nullptr;
                    }

                    SkipListNode *next = current->next;
                    current->next = nullptr;
                    current->down = nullptr;
                    delete current;
                    current = next;
                }

                // Reset the head node at this level
                heads[level]->data = nullptr;
                heads[level]->next = nullptr;
            }
        }

        void put(std::string_view key, std::string_view value)
        {
            packed_pair::PackedPair *packed_pair = new packed_pair::PackedPair(key, value);

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
                delete history[MAX_LEVEL]->data;
                history[MAX_LEVEL]->data = packed_pair;
                return;
            }

            // Insert the new node at the bottom level
            size_t level = random_level();
            SkipListNode *new_node = nullptr;

            for (size_t i = 0; i <= level; ++i)
            {
                new_node = history[MAX_LEVEL - i]->next = new SkipListNode{packed_pair, history[MAX_LEVEL - i]->next, new_node};
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

            packed_pair::PackedPair *operator*() const
            {
                return current->data;
            }

            packed_pair::PackedPair *operator->() const
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
}

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
        packed_pair::PackedPair *pair;
        SkipListNode *next;
        SkipListNode *down;
    };

    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_HEIGHT = 4;
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

        ~SkipList()
        {
            // Delete all levels
            // Delete all head nodes
        }

        void clear()
        {
        }

        void put(std::string_view key, std::string_view value)
        {
            packed_pair::PackedPair *packed_pair = new packed_pair::PackedPair(key, value);

            // Find the position to insert the new nodes at each level
            std::array<SkipListNode *, MAX_HEIGHT> history;
            SkipListNode *current = heads[MAX_HEIGHT - 1];
            for (size_t i = 0; i < MAX_HEIGHT; i++)
            {
                while (current->next != nullptr && key.compare(current->next->pair->key()) >= 0)
                {
                    current = current->next;
                }
                history[i] = current;
                current = current->down;
            }

            // Check if the key already exists
            if (history[MAX_LEVEL]->pair != nullptr && key.compare(history[MAX_LEVEL]->pair->key()) == 0)
            {
                // Key already exists, update the value
                delete history[MAX_LEVEL]->pair;
                history[MAX_LEVEL]->pair = packed_pair;
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
                return current->pair;
            }

            packed_pair::PackedPair *operator->() const
            {
                return current->pair;
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
                while (current->next != nullptr && key.compare(current->next->pair->key()) >= 0)
                {
                    current = current->next;
                }
                if (i < MAX_HEIGHT - 1)
                {
                    current = current->down;
                }
            }
            if (current->pair != nullptr && key.compare(current->pair->key()) == 0)
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

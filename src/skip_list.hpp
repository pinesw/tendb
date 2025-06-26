#pragma once

#include <array>
#include <random>
#include <string_view>

#include "packed_pair.hpp"

namespace tendb::skip_list
{
    struct SkipListNode
    {
        packed_pair::PackedPair pair;
        SkipListNode *prev;
        SkipListNode *next;
        SkipListNode *down;
    };

    struct SkipList
    {
    private:
        constexpr static std::size_t MAX_LEVEL = 16;
        // constexpr static std::double_t P = 0.5;

        std::mt19937 rng;
        std::uniform_real_distribution<double> dist;
        SkipListNode *head;

        // TODO: add thread safety by using atomic operations

    public:
        SkipList(uint64_t seed = 0)
            : rng(std::mt19937_64(seed)), dist(0.0, 1.0), head(nullptr) {}

        void put(std::string_view key, std::string_view value)
        {
            if (is_empty())
            {
                head = new SkipListNode{packed_pair::PackedPair(key, value), nullptr, nullptr, nullptr};
                return;
            }

            std::array<SkipListNode *, MAX_LEVEL> history;
            SkipListNode *current = head;

            // Traverse down the skip list
            size_t level = 0;
            while (current->down != nullptr)
            {
                history[level] = current;
                while (current->next != nullptr && key.compare(current->pair.key()) > 0)
                {
                    current = current->next;
                    history[level] = current;
                }
                current = current->down;
                level++;
            }

            // Traverse to the rightmost node at the bottom level
            history[level] = current;
            while (current->next != nullptr && key.compare(current->pair.key()) > 0)
            {
                current = current->next;
                history[level] = current;
            }

            SkipListNode *new_node = new SkipListNode{packed_pair::PackedPair(key, value), nullptr, nullptr, nullptr};

            for (size_t i = 0; i < level; i++)
            {

            }
        }

        bool is_empty() const
        {
            return head == nullptr;
        }

        bool get(std::string_view key, std::string_view &value) const
        {
            if (is_empty())
            {
                return false;
            }

            SkipListNode *current = head;

            // Traverse down the skip list
            while (current->down != nullptr)
            {
                while (current->next != nullptr && key.compare(current->pair.key()) > 0)
                {
                    current = current->next;
                }
                current = current->down;
            }

            // Traverse to the rightmost node at the bottom level
            while (current->next != nullptr && key.compare(current->pair.key()) > 0)
            {
                current = current->next;
            }

            // Check if the key matches
            if (key.compare(current->pair.key()) == 0)
            {
                value = current->pair.value();
                return true;
            }
            else
            {
                return false;
            }
        }
    };
}

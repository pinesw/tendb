#pragma once

#include <string_view>

#include "pbt/environment.hpp"

namespace tendb::pbt
{
    struct Reader
    {
        Environment *env;
        Header *header;

        Reader(Environment &env) : env(&env)
        {
            header = reinterpret_cast<Header *>(env.get_address());
        }

        Header *get_header() const
        {
            return reinterpret_cast<Header *>(env->get_address());
        }

        Node *get_node_at_offset(uint64_t offset) const
        {
            return reinterpret_cast<Node *>(reinterpret_cast<char *>(env->get_address()) + offset);
        }

        KeyValueItem *get(const std::string_view &key) const
        {
            if (header->num_items == 0)
            {
                return nullptr; // No items in the tree
            }

            uint64_t offset = header->root_offset;
            uint32_t depth = header->depth;
            while (depth > 0 && offset != 0)
            {
                Node *node = get_node_at_offset(offset);
                ChildReferenceIterator itr = node->child_reference_iterator();
                offset = 0;

                while (itr.has_next())
                {
                    const ChildReference *child = itr.current();

                    if (env->compare_fn(key, child->key()) >= 0)
                    {
                        offset = child->offset;
                    }
                    else
                    {
                        break;
                    }

                    itr.next();
                }

                --depth;
            }

            if (offset == 0)
            {
                return nullptr;
            }

            Node *leaf_node = get_node_at_offset(offset);
            ChildReferenceIterator itr = leaf_node->child_reference_iterator();
            while (itr.has_next())
            {
                const ChildReference *child = itr.current();
                if (env->compare_fn(key, child->key()) == 0)
                {
                    // Found the item
                    return reinterpret_cast<KeyValueItem *>(reinterpret_cast<char *>(env->get_address()) + child->offset);
                }
                itr.next();
            }

            return nullptr;
        }
    };
}

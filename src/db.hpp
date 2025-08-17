#pragma once

#include <cstdint>
#include <fstream>
#include <iostream>
#include <mutex>
#include <new>

#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

#include "port.hpp"

namespace tendb::db
{
    static const uint32_t MAGIC = 0x1EAF1111;

    std::mutex file_lock_mutex; // Mutex to exclude threads from concurrently using the file_lock (there can only be one file_lock, per file, per process)

    struct shm_remove
    {
        std::string filename;

        shm_remove(const std::string &filename) : filename(filename)
        {
            boost::interprocess::shared_memory_object::remove(filename.c_str());
        }

        ~shm_remove() { boost::interprocess::shared_memory_object::remove(filename.c_str()); }
    };

    struct alignas(std::hardware_destructive_interference_size) agent_table_entry
    {
        uint64_t transaction_id; // ID of the transaction that the agent is associated with
        uint32_t pid;            // Process ID of the agent owning the entry
        uint32_t thread_id;      // Thread ID of the agent owning the entry
    };

    struct environment_table
    {
        uint32_t magic;                   // Magic number to identify the structure
        uint32_t agent_table_size;        // Size of the agent table
        agent_table_entry agent_table[1]; // Array of agent table entries
    };

    struct environment
    {
        std::unique_ptr<shm_remove> shm_remover;
        std::unique_ptr<boost::interprocess::shared_memory_object> shm_obj;
        std::unique_ptr<boost::interprocess::mapped_region> region;

        environment(std::unique_ptr<shm_remove> &&shm_remover,
                    std::unique_ptr<boost::interprocess::shared_memory_object> &&shm_obj,
                    std::unique_ptr<boost::interprocess::mapped_region> &&region)
            : shm_remover(std::move(shm_remover)),
              shm_obj(std::move(shm_obj)),
              region(std::move(region)) {}

        ~environment()
        {
            region->flush();
        }

        environment_table *get_table()
        {
            return static_cast<environment_table *>(region->get_address());
        }
    };

    struct scoped_environment_lock
    {
        boost::interprocess::file_lock file_lock;

        scoped_environment_lock()
        {
            // Gain exclusive access to the file lock object for this process.
            file_lock_mutex.lock();

            // Now we can safely create the file_lock object and obtain the lock.
            file_lock = boost::interprocess::file_lock("tendb_flock");
            file_lock.lock();
        }

        ~scoped_environment_lock()
        {
            // Release the file lock and the mutex.
            file_lock.unlock();
            file_lock_mutex.unlock();
        }

        static void create_lock_file()
        {
            // Ensure the lock file exists, otherwise opening the file_lock will throw an error.
            // Since this is idempotent, we can safely do this before acquiring the lock.
            std::ofstream lock_file("tendb_flock");
            if (!lock_file)
            {
                throw std::runtime_error("Failed to create lock file.");
            }
            lock_file.close();
        }
    };

    environment init()
    {
        // Create a file lock object to ensure exclusive access to the shared memory object.
        scoped_environment_lock::create_lock_file();
        scoped_environment_lock sel;

        // First, pre-emptively remove the shared memory object if it exists.
        // This is done in case a previous process was killed and did not gracefully remove the shared memory object.
        // If any process is still using the shared memory object, it will not be removed.
        std::unique_ptr<shm_remove> shm_remover = std::make_unique<shm_remove>("tendb_environment_table");

        // Initialize the shared memory object.
        std::unique_ptr<boost::interprocess::shared_memory_object> shm_obj = std::make_unique<boost::interprocess::shared_memory_object>(
            boost::interprocess::open_or_create, "tendb_environment_table", boost::interprocess::read_write);

        // If the size is zero, we need to set it to the size of the environment_table structure.
        // Also mark the shared memory object as new, so we can initialize it.
        bool is_new = false;
        ptrdiff_t size;
        shm_obj->get_size(size);
        if (size == 0)
        {
            is_new = true;
            shm_obj->truncate(sizeof(environment_table));
            size = sizeof(environment_table);
        }

        // Create a mapped region for the shared memory object.
        std::unique_ptr<boost::interprocess::mapped_region> region = std::make_unique<boost::interprocess::mapped_region>(*shm_obj, boost::interprocess::read_write, 0, size);

        // If the shared memory object was newly created, we need to initialize it.
        if (is_new)
        {
            new (region->get_address()) environment_table{
                MAGIC, // magic
                1      // agent_table_size
            };
        }

        return {std::move(shm_remover), std::move(shm_obj), std::move(region)};
    }

    void resize_table(environment &env, uint32_t new_size)
    {
        // Create a file lock object to ensure exclusive access to the shared memory object.
        scoped_environment_lock sel;

        if (new_size <= env.get_table()->agent_table_size)
        {
            // No need to resize, the current size is sufficient.
            return;
        }

        // Resize the shared memory object.
        size_t size = sizeof(environment_table) + (new_size - 1) * sizeof(agent_table_entry);
        env.shm_obj->truncate(size);
        env.region = std::make_unique<boost::interprocess::mapped_region>(*env.shm_obj, boost::interprocess::read_write, 0, size);
        env.get_table()->agent_table_size = new_size;
    }
}

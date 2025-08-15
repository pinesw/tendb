#pragma once

#include <iostream>
#include <new>

#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

#include "port.hpp"

namespace tendb::db
{
    uint32_t max_readers = 100;
    // static const uint32_t MAGIC = 0x1EAF1111;

    struct alignas(std::hardware_destructive_interference_size) ReadLockTableEntry
    {
        uint64_t transaction_id; // ID of the transaction that the reader is viewing
        uint32_t pid;            // Process ID of the reader holding the lock
        uint32_t thread_id;      // Thread ID of the reader holding the lock
    };

    struct ReadLockTable
    {
        boost::interprocess::interprocess_mutex mutex; // Mutex to protect access to the read lock table
        uint32_t max_readers;                          // Maximum number of readers
        uint32_t num_readers;                          // Current number of readers
        ReadLockTableEntry entries[1];                 // Array of read lock entries
    };

    void init()
    {
        boost::interprocess::shared_memory_object shm_obj(boost::interprocess::open_or_create, "tendb_read_lock_table", boost::interprocess::read_write);
        shm_obj.truncate(sizeof(ReadLockTable));

        // Important: does truncate fill the memory with zeros?
        // If not, we need to initialize the entries manually, but care that we do so only once (when the shared memory is created)

        boost::interprocess::mapped_region region(shm_obj, boost::interprocess::read_write, 0, sizeof(ReadLockTable));

        void *address = region.get_address();
        size_t size = region.get_size();

        // ReadLockTable *lock_table = 
    }
}

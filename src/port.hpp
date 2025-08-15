#pragma once

#if defined(_WIN32)

#define NOMINMAX
#include <windows.h>

namespace tendb::port
{
    int physical_core_id()
    {
        return GetCurrentProcessorNumber();
    }

    uint32_t get_current_process_id()
    {
        return GetCurrentProcessId();
    }

    uint32_t get_current_thread_id()
    {
        return GetCurrentThreadId();
    }
}

#else

namespace tendb::port
{
    int physical_core_id()
    {
#if defined(__x86_64__) && (__GNUC__ > 2 || (__GNUC__ == 2 && __GNUC_MINOR__ >= 22))
        return sched_getcpu();
#elif defined(__x86_64__) || defined(__i386__)
        unsigned eax, ebx = 0, ecx, edx;
        if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
        {
            return -1;
        }
        return ebx >> 24;
#else
        return -1;
#endif
    }

    uint32_t get_current_process_id()
    {
        return static_cast<uint32_t>(getpid());
    }

    uint32_t get_current_thread_id()
    {
        return static_cast<uint32_t>(gettid());
    }
}

#endif

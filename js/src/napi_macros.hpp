#pragma once

#define NAPI_STATUS_THROWS_NULL(call)                  \
    if ((call) != napi_ok)                             \
    {                                                  \
        napi_throw_error(env, NULL, #call " failed!"); \
        return NULL;                                   \
    }

#define NAPI_STATUS_THROWS_NULL_CLEANUP(call, cleanup) \
    if ((call) != napi_ok)                             \
    {                                                  \
        cleanup;                                       \
        napi_throw_error(env, NULL, #call " failed!"); \
        return NULL;                                   \
    }

#define NAPI_EXPORT_FUNCTION(name)                                                          \
    {                                                                                       \
        napi_value name##_fn;                                                               \
        NAPI_STATUS_THROWS_NULL(napi_create_function(env, NULL, 0, name, NULL, &name##_fn)) \
        NAPI_STATUS_THROWS_NULL(napi_set_named_property(env, exports, #name, name##_fn))    \
    }

#define NAPI_ARGV(n)                                                                \
    napi_value argv[n];                                                             \
    size_t argc = n;                                                                \
    NAPI_STATUS_THROWS_NULL(napi_get_cb_info(env, cbinfo, &argc, argv, NULL, NULL)) \
    if (argc < n)                                                                   \
    {                                                                               \
        napi_throw_error(env, NULL, "Expected " #n " arguments");                   \
        return nullptr;                                                             \
    }

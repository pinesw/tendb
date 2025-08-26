#include <memory>
#include <string>

#include <node_api.h>

#include "pbt/environment.hpp"

napi_status napi_utf8_to_string(napi_env env, napi_value val, std::string &result)
{
    napi_status status;

    size_t size;
    status = napi_get_value_string_utf8(env, val, nullptr, 0, &size);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to convert argument to string");
        return status;
    }

    result.reserve(size + 1);
    result.resize(size);
    status = napi_get_value_string_utf8(env, val, &result[0], result.capacity(), nullptr);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to convert argument to string");
        return status;
    }

    return napi_ok;
}

struct PbtEnvContext
{
    std::unique_ptr<tendb::pbt::Environment> pbt_env;
    napi_value external;
    napi_ref external_ref;

    PbtEnvContext(const std::string &path) : pbt_env(std::make_unique<tendb::pbt::Environment>(path)) {}
};

void cleanup_hook_pbt_env(void *arg)
{
    if (arg)
    {
        delete static_cast<PbtEnvContext *>(arg);
    }
}

void finalize_cb_pbt_env(napi_env env, void *data, void *hint)
{
    if (data)
    {
        napi_status status;
        PbtEnvContext *pbt_env_ctx = static_cast<PbtEnvContext *>(data);

        status = napi_delete_reference(env, pbt_env_ctx->external_ref);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to delete external reference");
            return;
        }

        status = napi_remove_env_cleanup_hook(env, cleanup_hook_pbt_env, pbt_env_ctx);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to remove cleanup hook");
            return;
        }

        delete pbt_env_ctx;
    }
}

napi_value create_pbt_env(napi_env env, napi_callback_info args)
{
    napi_status status;

    napi_value argv[1];
    size_t argc = 1;
    status = napi_get_cb_info(env, args, &argc, argv, NULL, NULL);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to parse arguments");
        return nullptr;
    }
    
    if (argc < 1)
    {
        napi_throw_error(env, NULL, "Expected one argument");
        return nullptr;
    }

    std::string path;
    status = napi_utf8_to_string(env, argv[0], path);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Invalid argument, expected string");
        return nullptr;
    }

    PbtEnvContext *pbt_env_ctx = new PbtEnvContext(path);

    status = napi_add_env_cleanup_hook(env, cleanup_hook_pbt_env, pbt_env_ctx);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to add cleanup hook");
        return nullptr;
    }

    status = napi_create_external(env, pbt_env_ctx, finalize_cb_pbt_env, NULL, &pbt_env_ctx->external);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to create external");
        return nullptr;
    }

    status = napi_create_reference(env, pbt_env_ctx->external, 0, &pbt_env_ctx->external_ref);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to create external reference");
        return nullptr;
    }

    return pbt_env_ctx->external;
}

napi_value init(napi_env env, napi_value exports)
{
    napi_status status;
    napi_value create_pbt_env_fn;

    status = napi_create_function(env, nullptr, 0, create_pbt_env, nullptr, &create_pbt_env_fn);
    if (status != napi_ok)
    {
        return nullptr;
    }

    status = napi_set_named_property(env, exports, "create_pbt_env", create_pbt_env_fn);
    if (status != napi_ok)
    {
        return nullptr;
    }

    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)

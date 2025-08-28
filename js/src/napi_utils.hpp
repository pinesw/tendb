#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <node_api.h>

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

napi_status napi_buffer_to_string_view(napi_env env, napi_value val, std::string_view &result)
{
    napi_status status;

    bool is_buffer;
    status = napi_is_buffer(env, val, &is_buffer);
    if (status != napi_ok || !is_buffer)
    {
        napi_throw_error(env, NULL, "Failed to convert argument to buffer");
        return status;
    }

    void *data;
    size_t size;
    status = napi_get_buffer_info(env, val, &data, &size);
    if (status != napi_ok)
    {
        napi_throw_error(env, NULL, "Failed to convert argument to buffer");
        return status;
    }

    result = std::string_view(static_cast<const char *>(data), size);

    return napi_ok;
}

template <class T, class... Args>
struct ExternalObjectHolder
{
    std::unique_ptr<T> ptr;
    napi_env env;
    napi_value external;
    napi_ref external_ref;

    ExternalObjectHolder(napi_env env, Args... args) : env(env), ptr(std::make_unique<T>(args...)) {}

    napi_status napi_init_eoh()
    {
        napi_status status;

        status = napi_add_env_cleanup_hook(env, ExternalObjectHolder<T>::cleanup_hook, this);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to add cleanup hook");
            return status;
        }

        status = napi_create_external(env, this, ExternalObjectHolder<T>::finalize_cb, NULL, &external);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to create external");
            return status;
        }

        status = napi_create_reference(env, external, 0, &external_ref);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to create external reference");
            return status;
        }

        return napi_ok;
    }

    napi_status increase_ref()
    {
        napi_status status;

        status = napi_reference_ref(env, external_ref, nullptr);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to increase external reference");
            return status;
        }

        return napi_ok;
    }

    napi_status decrease_ref()
    {
        napi_status status;

        status = napi_reference_unref(env, external_ref, nullptr);
        if (status != napi_ok)
        {
            napi_throw_error(env, NULL, "Failed to decrease external reference");
            return status;
        }

        return napi_ok;
    }

    static void ref_cb(napi_env env, void *finalize_data, void *finalize_hint)
    {
        if (finalize_hint)
        {
            ExternalObjectHolder<T> *eoh = static_cast<ExternalObjectHolder<T> *>(finalize_hint);
            eoh->increase_ref();
        }
    }

    static void deref_cb(napi_env env, void *finalize_data, void *finalize_hint)
    {
        if (finalize_hint)
        {
            ExternalObjectHolder<T> *eoh = static_cast<ExternalObjectHolder<T> *>(finalize_hint);
            eoh->decrease_ref();
        }
    }

    static void cleanup_hook(void *arg)
    {
        if (arg)
        {
            delete static_cast<ExternalObjectHolder<T> *>(arg);
        }
    }

    static void finalize_cb(napi_env env, void *finalize_data, void *finalize_hint)
    {
        if (finalize_data)
        {
            ExternalObjectHolder<T> *eoh = static_cast<ExternalObjectHolder<T> *>(finalize_data);

            napi_status status;

            status = napi_delete_reference(env, eoh->external_ref);
            if (status != napi_ok)
            {
                napi_throw_error(env, NULL, "Failed to delete external reference");
                return;
            }

            status = napi_remove_env_cleanup_hook(env, ExternalObjectHolder<T>::cleanup_hook, eoh);
            if (status != napi_ok)
            {
                napi_throw_error(env, NULL, "Failed to remove cleanup hook");
                return;
            }

            delete eoh;
        }
    }
};

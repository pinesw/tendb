#include <memory>
#include <string>
#include <string_view>

#include <node_api.h>
#include "napi_macros.hpp"
#include "napi_utils.hpp"

#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

typedef ExternalObjectHolder<tendb::pbt::Writer, const std::string &> WriterHolder;
typedef ExternalObjectHolder<tendb::pbt::Reader, const std::string &> ReaderHolder;

napi_value create_pbt_writer(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    std::string path;
    NAPI_STATUS_THROWS_NULL(napi_utf8_to_string(env, argv[0], path));

    WriterHolder *wh = new WriterHolder(env, path);
    NAPI_STATUS_THROWS_NULL_CLEANUP(wh->napi_init_eoh(), delete wh);

    return wh->external;
}

napi_value pbt_writer_add(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(3);

    WriterHolder *wh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&wh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    std::string_view value;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[2], value));

    wh->ptr->add(key, value);

    return nullptr;
}

napi_value pbt_writer_finish(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    WriterHolder *wh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&wh));

    wh->ptr->finish();

    return nullptr;
}

napi_value create_pbt_reader(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    std::string path;
    NAPI_STATUS_THROWS_NULL(napi_utf8_to_string(env, argv[0], path));

    ReaderHolder *rh = new ReaderHolder(env, path);
    NAPI_STATUS_THROWS_NULL_CLEANUP(rh->napi_init_eoh(), delete rh);

    return rh->external;
}

napi_value pbt_reader_get(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ReaderHolder *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    napi_value result;
    const tendb::pbt::KeyValueItem *item = rh->ptr->get(key);
    if (item)
    {
        rh->increase_ref();
        NAPI_STATUS_THROWS_NULL(napi_create_external_buffer(env, item->value().size(), (void *)item->value().data(), ReaderHolder::deref_cb, rh, &result));
    }
    else
    {
        NAPI_STATUS_THROWS_NULL(napi_get_null(env, &result));
    }

    return result;
}

napi_value init(napi_env env, napi_value exports)
{
    NAPI_EXPORT_FUNCTION(create_pbt_writer);
    NAPI_EXPORT_FUNCTION(pbt_writer_add);
    NAPI_EXPORT_FUNCTION(pbt_writer_finish);
    NAPI_EXPORT_FUNCTION(create_pbt_reader);
    NAPI_EXPORT_FUNCTION(pbt_reader_get);

    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)

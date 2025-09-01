#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <node_api.h>
#include "napi_macros.hpp"
#include "napi_utils.hpp"

#include "pbt/reader.hpp"
#include "pbt/writer.hpp"

typedef ExternalObject<tendb::pbt::Writer, const std::string &> ExternalWriter;
typedef ExternalObject<tendb::pbt::Reader, const std::string &> ExternalReader;
typedef ExternalObject<tendb::pbt::KeyValueItem::Iterator, const tendb::pbt::KeyValueItem::Iterator> ExternalKeyValueIterator;

napi_value create_pbt_writer(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    std::string path;
    NAPI_STATUS_THROWS_NULL(napi_utf8_to_string(env, argv[0], path));

    ExternalWriter *wh = new ExternalWriter(env, path);
    NAPI_STATUS_THROWS_NULL_CLEANUP(wh->napi_init_eoh(), delete wh);

    return wh->external;
}

napi_value pbt_writer_add(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(3);

    ExternalWriter *wh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&wh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    std::string_view value;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[2], value));

    wh->ptr->add(key, value);

    return nullptr;
}

napi_value pbt_writer_merge(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalWriter *wh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&wh));

    bool is_array;
    NAPI_STATUS_THROWS_NULL(napi_is_array(env, argv[1], &is_array));
    if (!is_array)
    {
        napi_throw_type_error(env, NULL, "Argument must be an array");
        return nullptr;
    }

    uint32_t length;
    NAPI_STATUS_THROWS_NULL(napi_get_array_length(env, argv[1], &length));

    std::vector<tendb::pbt::Reader *> reader_ptrs(length);
    for (uint32_t i = 0; i < length; i++)
    {
        napi_value element;
        NAPI_STATUS_THROWS_NULL(napi_get_element(env, argv[1], i, &element));

        ExternalReader *rh;
        NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, element, (void **)&rh));

        reader_ptrs[i] = rh->ptr.get();
    }

    wh->ptr->merge((const tendb::pbt::Reader **)reader_ptrs.data(), reader_ptrs.size());

    return nullptr;
}

napi_value pbt_writer_finish(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalWriter *wh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&wh));

    wh->ptr->finish();

    return nullptr;
}

napi_value create_pbt_reader(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    std::string path;
    NAPI_STATUS_THROWS_NULL(napi_utf8_to_string(env, argv[0], path));

    ExternalReader *rh = new ExternalReader(env, path);
    NAPI_STATUS_THROWS_NULL_CLEANUP(rh->napi_init_eoh(), delete rh);

    return rh->external;
}

napi_value pbt_reader_get(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    napi_value result;
    const tendb::pbt::KeyValueItem *item = rh->ptr->get(key);

    if (item)
    {
        rh->increase_ref();
        NAPI_STATUS_THROWS_NULL(napi_create_external_buffer(env, item->value().size(), (void *)item->value().data(), ExternalReader::deref_cb, rh, &result));
    }
    else
    {
        NAPI_STATUS_THROWS_NULL(napi_get_null(env, &result));
    }

    return result;
}

napi_value pbt_reader_get_copy_to(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(3);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    void *out_data;
    size_t out_length;
    NAPI_STATUS_THROWS_NULL(napi_get_buffer_info(env, argv[2], &out_data, &out_length));

    napi_value result;
    const tendb::pbt::KeyValueItem *item = rh->ptr->get(key);

    if (item)
    {
        if (item->value().size() > out_length)
        {
            napi_throw_range_error(env, NULL, "Output buffer is too small");
            return nullptr;
        }

        memcpy(out_data, item->value().data(), item->value().size());

        NAPI_STATUS_THROWS_NULL(napi_get_boolean(env, true, &result));
    }
    else
    {
        NAPI_STATUS_THROWS_NULL(napi_get_boolean(env, false, &result));
    }

    return result;
}

napi_value pbt_reader_at(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    uint32_t index;
    NAPI_STATUS_THROWS_NULL(napi_get_value_uint32(env, argv[1], &index));

    napi_value result;
    const tendb::pbt::KeyValueItem *item = rh->ptr->at(index);
    if (item)
    {
        rh->increase_ref();
        NAPI_STATUS_THROWS_NULL(napi_create_external_buffer(env, item->value().size(), (void *)item->value().data(), ExternalReader::deref_cb, rh, &result));
    }
    else
    {
        NAPI_STATUS_THROWS_NULL(napi_get_null(env, &result));
    }

    return result;
}

napi_value pbt_reader_begin(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    ExternalKeyValueIterator *kih = new ExternalKeyValueIterator(env, rh->ptr->begin());
    NAPI_STATUS_THROWS_NULL_CLEANUP(kih->napi_init_eoh(), delete kih);

    rh->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_add_finalizer(env, kih->external, NULL, ExternalReader::deref_cb, rh, NULL));

    return kih->external;
}

napi_value pbt_reader_end(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    ExternalKeyValueIterator *kih = new ExternalKeyValueIterator(env, rh->ptr->end());
    NAPI_STATUS_THROWS_NULL_CLEANUP(kih->napi_init_eoh(), delete kih);

    rh->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_add_finalizer(env, kih->external, NULL, ExternalReader::deref_cb, rh, NULL));

    return kih->external;
}

napi_value pbt_reader_seek(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    std::string_view key;
    NAPI_STATUS_THROWS_NULL(napi_buffer_to_string_view(env, argv[1], key));

    ExternalKeyValueIterator *kih = new ExternalKeyValueIterator(env, rh->ptr->seek(key));
    NAPI_STATUS_THROWS_NULL_CLEANUP(kih->napi_init_eoh(), delete kih);

    rh->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_add_finalizer(env, kih->external, NULL, ExternalReader::deref_cb, rh, NULL));

    return kih->external;
}

napi_value pbt_reader_seek_at(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalReader *rh;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&rh));

    uint32_t index;
    NAPI_STATUS_THROWS_NULL(napi_get_value_uint32(env, argv[1], &index));

    ExternalKeyValueIterator *kih = new ExternalKeyValueIterator(env, rh->ptr->seek_at(index));
    NAPI_STATUS_THROWS_NULL_CLEANUP(kih->napi_init_eoh(), delete kih);

    rh->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_add_finalizer(env, kih->external, NULL, ExternalReader::deref_cb, rh, NULL));

    return kih->external;
}

napi_value pbt_keyvalue_iterator_increment(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalKeyValueIterator *kih;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih));

    ++(*(kih->ptr));

    return nullptr;
}

napi_value pbt_keyvalue_iterator_equals(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalKeyValueIterator *kih1;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih1));

    ExternalKeyValueIterator *kih2;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[1], (void **)&kih2));

    bool equals = (*(kih1->ptr) == *(kih2->ptr));

    napi_value result;
    NAPI_STATUS_THROWS_NULL(napi_get_boolean(env, equals, &result));

    return result;
}

napi_value pbt_keyvalue_iterator_get_key(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalKeyValueIterator *kih;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih));

    const tendb::pbt::KeyValueItem *item = *(*(kih->ptr));

    napi_value result;
    kih->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_create_external_buffer(env, item->key().size(), (void *)item->key().data(), ExternalKeyValueIterator::deref_cb, kih, &result));

    return result;
}

napi_value pbt_keyvalue_iterator_get_key_copy_to(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalKeyValueIterator *kih;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih));

    void *out_data;
    size_t out_length;
    NAPI_STATUS_THROWS_NULL(napi_get_buffer_info(env, argv[1], &out_data, &out_length));

    const tendb::pbt::KeyValueItem *item = *(*(kih->ptr));

    if (item->key().size() > out_length)
    {
        napi_throw_range_error(env, NULL, "Output buffer is too small");
        return nullptr;
    }

    memcpy(out_data, item->key().data(), item->key().size());

    return nullptr;
}

napi_value pbt_keyvalue_iterator_get_value(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(1);

    ExternalKeyValueIterator *kih;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih));

    const tendb::pbt::KeyValueItem *item = *(*(kih->ptr));

    napi_value result;
    kih->increase_ref();
    NAPI_STATUS_THROWS_NULL(napi_create_external_buffer(env, item->value().size(), (void *)item->value().data(), ExternalKeyValueIterator::deref_cb, kih, &result));

    return result;
}

napi_value pbt_keyvalue_iterator_get_value_copy_to(napi_env env, napi_callback_info cbinfo)
{
    NAPI_ARGV(2);

    ExternalKeyValueIterator *kih;
    NAPI_STATUS_THROWS_NULL(napi_get_value_external(env, argv[0], (void **)&kih));

    void *out_data;
    size_t out_length;
    NAPI_STATUS_THROWS_NULL(napi_get_buffer_info(env, argv[1], &out_data, &out_length));

    const tendb::pbt::KeyValueItem *item = *(*(kih->ptr));

    if (item->value().size() > out_length)
    {
        napi_throw_range_error(env, NULL, "Output buffer is too small");
        return nullptr;
    }

    memcpy(out_data, item->value().data(), item->value().size());

    return nullptr;
}

napi_value init(napi_env env, napi_value exports)
{
    NAPI_EXPORT_FUNCTION(create_pbt_writer);
    NAPI_EXPORT_FUNCTION(pbt_writer_add);
    NAPI_EXPORT_FUNCTION(pbt_writer_merge);
    NAPI_EXPORT_FUNCTION(pbt_writer_finish);
    NAPI_EXPORT_FUNCTION(create_pbt_reader);
    NAPI_EXPORT_FUNCTION(pbt_reader_get);
    NAPI_EXPORT_FUNCTION(pbt_reader_get_copy_to);
    NAPI_EXPORT_FUNCTION(pbt_reader_at);
    NAPI_EXPORT_FUNCTION(pbt_reader_begin);
    NAPI_EXPORT_FUNCTION(pbt_reader_end);
    NAPI_EXPORT_FUNCTION(pbt_reader_seek);
    NAPI_EXPORT_FUNCTION(pbt_reader_seek_at);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_increment);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_equals);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_get_key);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_get_key_copy_to);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_get_value);
    NAPI_EXPORT_FUNCTION(pbt_keyvalue_iterator_get_value_copy_to);

    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)

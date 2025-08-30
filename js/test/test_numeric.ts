import {
    create_pbt_writer,
    pbt_writer_add,
    pbt_writer_finish,
    create_pbt_reader,
    pbt_reader_get,
    pbt_reader_at,
    pbt_reader_begin,
    pbt_reader_end,
    pbt_reader_seek,
    pbt_reader_seek_at,
    pbt_keyvalue_iterator_increment,
    pbt_keyvalue_iterator_equals,
    pbt_keyvalue_iterator_get_key,
    pbt_keyvalue_iterator_get_value,
} from "../binding";

const bufferKey = Buffer.alloc(4);
const bufferValue = Buffer.alloc(4);

const writer = create_pbt_writer("out/nodejs_numeric.pbt");
for (let i = 0; i < 1024; i += 8) {
    bufferKey.writeUInt32BE(i, 0);
    bufferValue.writeUInt32BE(i * 10, 0);
    pbt_writer_add(writer, bufferKey, bufferValue);
}
pbt_writer_finish(writer);

const reader = create_pbt_reader("out/nodejs_numeric.pbt");
for (let i = 0; i < 1024; i += 8) {
    bufferKey.writeUInt32BE(i, 0);
    const value = pbt_reader_get(reader, bufferKey);
    if (value) {
        console.log(`Key: ${i}, Value: ${value.readUInt32BE(0)}`);
    } else {
        console.log(`Key: ${i}, Value: null`);
    }
}

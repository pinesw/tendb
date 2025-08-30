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

const reader = create_pbt_reader("in/adminAreas.pbt");
const key = Buffer.alloc(16);
key.writeInt32LE(0x880E89, 0);
key.writeInt32LE(0x0, 4);
key.writeInt32LE(0x0, 8);
key.writeInt32LE(0x0, 12);
console.log(key);
console.log(key.toString("hex"));
const result = pbt_reader_get(reader, key);
console.log(result);

const begin = pbt_reader_begin(reader);
const begin_key = pbt_keyvalue_iterator_get_key(begin);
console.log(`Begin key: ${begin_key.toString("hex")}`);

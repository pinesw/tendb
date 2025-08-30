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

// const writer = create_pbt_writer("nodejs.pbt");
// pbt_writer_add(writer, Buffer.from("key-1"), Buffer.from("value-1"));
// pbt_writer_add(writer, Buffer.from("key-2"), Buffer.from("value-2"));
// pbt_writer_finish(writer);

// const reader = create_pbt_reader("nodejs.pbt");

// console.log(`Value at 0: ${pbt_reader_at(reader, 0)?.toString()}`);
// console.log(`Value at 1: ${pbt_reader_at(reader, 1)?.toString()}`);
// console.log(`Value at 2: ${pbt_reader_at(reader, 2)?.toString()}`);
// console.log(`Value at key-1: ${pbt_reader_get(reader, Buffer.from("key-1"))?.toString()}`);
// console.log(`Value at key-2: ${pbt_reader_get(reader, Buffer.from("key-2"))?.toString()}`);
// console.log(`Value at key-3: ${pbt_reader_get(reader, Buffer.from("key-3"))?.toString()}`);

// const itr = pbt_reader_begin(reader);
// const end = pbt_reader_end(reader);
// while (!pbt_keyvalue_iterator_equals(itr, end)) {
//     console.log(`Key: ${pbt_keyvalue_iterator_get_key(itr).toString()}, Value: ${pbt_keyvalue_iterator_get_value(itr).toString()}`);
//     pbt_keyvalue_iterator_increment(itr);
// }

const bufferKey = Buffer.alloc(4);
const bufferValue = Buffer.alloc(4);
const compareFn = (a: Buffer, b: Buffer) => {
    return a.readUInt32LE(0) - b.readUInt32LE(0);
};

const writer = create_pbt_writer("nodejs_numeric.pbt", { compareFn });
for (let i = 0; i < 1024; i += 8) {
    bufferKey.writeUInt32LE(i, 0);
    bufferValue.writeUInt32LE(i * 10, 0);
    pbt_writer_add(writer, bufferKey, bufferValue);
}
pbt_writer_finish(writer);

const reader = create_pbt_reader("nodejs_numeric.pbt", { compareFn });

// const itr = pbt_reader_begin(reader);
// const end = pbt_reader_end(reader);
// while (!pbt_keyvalue_iterator_equals(itr, end)) {
//     console.log(`Key: ${pbt_keyvalue_iterator_get_key(itr).readUInt32LE(0)}, Value: ${pbt_keyvalue_iterator_get_value(itr).readUInt32LE(0)}`);
//     pbt_keyvalue_iterator_increment(itr);
// }

for (let i = 0; i < 1024; i += 8) {
    bufferKey.writeUInt32LE(i, 0);
    const value = pbt_reader_get(reader, bufferKey);
    if (value) {
        console.log(`Key: ${i}, Value: ${value.readUInt32LE(0)}`);
    } else {
        console.log(`Key: ${i}, Value: null`);
    }
}

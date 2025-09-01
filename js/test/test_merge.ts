import {
    create_pbt_writer,
    pbt_writer_add,
    pbt_writer_merge,
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

const writer1 = create_pbt_writer("out/nodejs-1.pbt");
pbt_writer_add(writer1, Buffer.from("key-1"), Buffer.from("value-1"));
pbt_writer_add(writer1, Buffer.from("key-2"), Buffer.from("value-2"));
pbt_writer_finish(writer1);

const writer2 = create_pbt_writer("out/nodejs-2.pbt");
pbt_writer_add(writer2, Buffer.from("key-3"), Buffer.from("value-3"));
pbt_writer_add(writer2, Buffer.from("key-4"), Buffer.from("value-4"));
pbt_writer_finish(writer2);

const reader1 = create_pbt_reader("out/nodejs-1.pbt");
const reader2 = create_pbt_reader("out/nodejs-2.pbt");

const itr1 = pbt_reader_begin(reader1);
const end1 = pbt_reader_end(reader1);
while (!pbt_keyvalue_iterator_equals(itr1, end1)) {
    console.log(`Reader 1 - Key: ${pbt_keyvalue_iterator_get_key(itr1).toString()}, Value: ${pbt_keyvalue_iterator_get_value(itr1).toString()}`);
    pbt_keyvalue_iterator_increment(itr1);
}

const itr2 = pbt_reader_begin(reader2);
const end2 = pbt_reader_end(reader2);
while (!pbt_keyvalue_iterator_equals(itr2, end2)) {
    console.log(`Reader 2 - Key: ${pbt_keyvalue_iterator_get_key(itr2).toString()}, Value: ${pbt_keyvalue_iterator_get_value(itr2).toString()}`);
    pbt_keyvalue_iterator_increment(itr2);
}

const writer3 = create_pbt_writer("out/nodejs-merged.pbt");
pbt_writer_merge(writer3, [reader1, reader2]);
pbt_writer_finish(writer3);

const reader3 = create_pbt_reader("out/nodejs-merged.pbt");
const itr3 = pbt_reader_begin(reader3);
const end3 = pbt_reader_end(reader3);
while (!pbt_keyvalue_iterator_equals(itr3, end3)) {
    console.log(`Merged - Key: ${pbt_keyvalue_iterator_get_key(itr3).toString()}, Value: ${pbt_keyvalue_iterator_get_value(itr3).toString()}`);
    pbt_keyvalue_iterator_increment(itr3);
}

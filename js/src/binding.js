const binding = require("../build/Release/tendb.node");

const writer = binding.create_pbt_writer("nodejs.pbt");
binding.pbt_writer_add(writer, Buffer.from("key-1"), Buffer.from("value-1"));
binding.pbt_writer_add(writer, Buffer.from("key-2"), Buffer.from("value-2"));
binding.pbt_writer_finish(writer);

const reader = binding.create_pbt_reader("nodejs.pbt");
const value = binding.pbt_reader_get(reader, Buffer.from("key-1"));

console.log(value.toString());

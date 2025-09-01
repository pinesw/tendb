const os = require("os");
const pjson = require("./package.json");

let platform;
switch (os.platform()) {
    case "win32":
        platform = "windows";
        break;
    case "darwin":
        platform = "macos";
        break;
    case "linux":
        platform = "linux";
        break;
    default:
        throw new Error("Unsupported platform");
}

const arch = os.arch();
const node_version = process.versions.node.split(".")[0];

const path_template = pjson.binary.path;
const path = path_template
    .replace("{platform}", platform)
    .replace("{arch}", arch)
    .replace("{node_version}", node_version);

const binding = require(path);

declare const __brand: unique symbol;
type Brand<B> = { [__brand]: B };
type Branded<T, B> = T & Brand<B>;
type ExternalWriter = Branded<{}, "ExternalWriter">;
type ExternalReader = Branded<{}, "ExternalReader">;
type ExternalKeyValueIterator = Branded<{}, "ExternalKeyValueIterator">;

export function create_pbt_writer(path: string): ExternalWriter {
    return binding.create_pbt_writer(path);
}

export function pbt_writer_add(writer: ExternalWriter, key: Buffer, value: Buffer): void {
    binding.pbt_writer_add(writer, key, value);
}

export function pbt_writer_merge(writer: ExternalWriter, readers: ExternalReader[]): void {
    binding.pbt_writer_merge(writer, readers);
}

export function pbt_writer_finish(writer: ExternalWriter): void {
    binding.pbt_writer_finish(writer);
}

export function create_pbt_reader(path: string): ExternalReader {
    return binding.create_pbt_reader(path);
}

export function pbt_reader_get(reader: ExternalReader, key: Buffer): Buffer | null {
    return binding.pbt_reader_get(reader, key);
}

export function pbt_reader_get_copy_to(reader: ExternalReader, key: Buffer, out: Buffer): number | null {
    return binding.pbt_reader_get_copy_to(reader, key, out);
}

export function pbt_reader_at(reader: ExternalReader, index: number): Buffer | null {
    return binding.pbt_reader_at(reader, index);
}

export function pbt_reader_begin(reader: ExternalReader): ExternalKeyValueIterator {
    return binding.pbt_reader_begin(reader);
}

export function pbt_reader_end(reader: ExternalReader): ExternalKeyValueIterator {
    return binding.pbt_reader_end(reader);
}

export function pbt_reader_seek(reader: ExternalReader, key: Buffer): ExternalKeyValueIterator {
    return binding.pbt_reader_seek(reader, key);
}

export function pbt_reader_seek_at(reader: ExternalReader, index: number): ExternalKeyValueIterator {
    return binding.pbt_reader_seek_at(reader, index);
}

export function pbt_keyvalue_iterator_increment(itr: ExternalKeyValueIterator): void {
    binding.pbt_keyvalue_iterator_increment(itr);
}

export function pbt_keyvalue_iterator_equals(itr1: ExternalKeyValueIterator, itr2: ExternalKeyValueIterator): boolean {
    return binding.pbt_keyvalue_iterator_equals(itr1, itr2);
}

export function pbt_keyvalue_iterator_get_key(iterator: ExternalKeyValueIterator): Buffer {
    return binding.pbt_keyvalue_iterator_get_key(iterator);
}

export function pbt_keyvalue_iterator_get_key_copy_to(iterator: ExternalKeyValueIterator, out: Buffer): number {
    return binding.pbt_keyvalue_iterator_get_key_copy_to(iterator, out);
}

export function pbt_keyvalue_iterator_get_value(iterator: ExternalKeyValueIterator): Buffer {
    return binding.pbt_keyvalue_iterator_get_value(iterator);
}

export function pbt_keyvalue_iterator_get_value_copy_to(iterator: ExternalKeyValueIterator, out: Buffer): number {
    return binding.pbt_keyvalue_iterator_get_value_copy_to(iterator, out);
}

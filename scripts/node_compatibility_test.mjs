import { readFileSync } from 'node:fs';
import { compatibilityCases } from './compatibility_cases.mjs';

function extract(file, name) {
  const source = readFileSync('src/' + file + '.mbt', 'utf8');
  const match = source.match(new RegExp('extern "js" fn ' + name + '[\\s\\S]*?=\\n((?:\\s*#\\|.*\\n)+)'));
  if (!match) throw new Error('missing FFI: ' + name);
  return eval('(' + match[1].split('\n').filter(s => s.trim().startsWith('#|'))
    .map(s => s.replace(/^\s*#\|\s?/, '')).join('\n') + ')');
}
const ffi = {};
for (const [file, names] of [
  ['duckdb_js', ['js_connect', 'js_query_stream', 'js_stream_close', 'js_close']],
  ['duckdb_vector_js', ['js_stream_next_vectors_ffi']],
  ['duckdb_arrow_js', ['js_query_arrow', 'js_arrow_to_stream', 'js_arrow_columns', 'js_arrow_type_ids', 'js_arrow_close_handle']],
]) for (const name of names) ffi[name] = extract(file, name);
const conn = await new Promise((ok, err) => ffi.js_connect(':memory:', 1, ok, err));
try {
  const checks = await compatibilityCases(ffi, conn);
  console.log('Node compatibility PASS: ' + checks.join(', '));
} finally {
  await new Promise((ok, err) => ffi.js_close(conn, ok, err));
}

// Shared assertions exercise the actual FFI in Node and Chromium.
export async function compatibilityCases(ffi, conn) {
  const call = (name, ...args) => new Promise((ok, err) => ffi[name](...args, ok, err));
  const assert = (value, message) => { if (!value) throw new Error(message); };
  const equal = (actual, expected, message) => assert(
    JSON.stringify(actual) === JSON.stringify(expected),
    message + ': ' + JSON.stringify(actual),
  );
  const rejects = async (operation, marker) => {
    try { await operation(); } catch (error) {
      assert(String(error).includes(marker), 'unexpected error: ' + error);
      return;
    }
    throw new Error('expected rejection: ' + marker);
  };
  const query = (sql) => call('js_query_stream', conn, sql);
  const close = (stream) => call('js_stream_close', stream);
  const next = (stream) => new Promise((ok, err) => ffi.js_stream_next_vectors_ffi(stream,
    (columns, types, kinds, metas, offsets, slotRows, flags, shapes,
     shapeOffsets, names, nameOffsets, rows, i32, i64) => ok({ columns, types, rows, i32, i64 }), err));
  const drain = async (stream) => {
    const chunks = [];
    for (let i = 0; i < 100; i++) {
      const chunk = await next(stream);
      if (!chunk.columns.length) return chunks;
      chunks.push(chunk);
    }
    throw new Error('stream did not terminate');
  };
  const checks = [];
  const sql = 'SELECT 1::INTEGER AS a, CAST(NULL AS VARCHAR) AS b WHERE false';
  const empty = await query(sql);
  try {
    equal(empty.columns, ['a', 'b'], 'empty stream columns');
    const chunks = await drain(empty);
    equal(chunks.map(c => [c.columns, c.types, c.rows]),
      [[['a', 'b'], [4, 17], 0]], 'empty stream schema');
    equal((await next(empty)).columns, [], 'stable EOF');
  } finally { await close(empty); }
  const arrow = await call('js_query_arrow', conn, sql);
  let arrowReleases = 0;
  const arrowResource = arrow.result;
  if (arrowResource && typeof arrowResource.closeSync === 'function') {
    const original = arrowResource.closeSync.bind(arrowResource);
    arrowResource.closeSync = () => { arrowReleases++; original(); };
  }
  try {
    equal(ffi.js_arrow_columns(arrow), ['a', 'b'], 'empty Arrow columns');
    equal(ffi.js_arrow_type_ids(arrow), [4, 17], 'empty Arrow types');
    const stream = ffi.js_arrow_to_stream(arrow);
    try {
      const chunks = await drain(stream);
      equal(chunks.map(c => [c.columns, c.types, c.rows]),
        [[['a', 'b'], [4, 17], 0]], 'empty Arrow schema');
    } finally { await close(stream); }
  } finally { ffi.js_arrow_close_handle(arrow); }
  if (arrowResource && typeof arrowResource.closeSync === 'function') {
    assert(arrowReleases === 1, 'Arrow result release must run once');
  }
  ffi.js_arrow_close_handle(arrow);
  assert(arrowReleases <= 1, 'duplicate Arrow result release');
  assert(arrow.closed && arrow.result === null && arrow.table === null, 'Arrow references released');
  checks.push('empty-stream-and-arrow-schema');

  const many = await query('SELECT i::INTEGER AS i FROM range(10000) t(i)');
  try {
    const chunks = await drain(many);
    assert(chunks.length > 1, 'expected multiple batches');
    const values = chunks.flatMap(c => Array.from(c.i32));
    assert(values.length === 10000, 'stream row count');
    assert(values.every((v, i) => v === i), 'stream values/order');
    assert(chunks.reduce((n, c) => n + c.rows, 0) === 10000, 'batch row count');
  } finally { await close(many); }
  checks.push('multi-batch-values-and-eof');

  const early = await query('SELECT i::INTEGER AS i FROM range(100000) t(i)');
  await next(early);
  let releases = 0;
  const resource = conn.kind === 'node' ? early.result : early.reader;
  const method = conn.kind === 'node' ? 'closeSync' : 'cancel';
  if (typeof resource[method] === 'function') {
    const original = resource[method].bind(resource);
    resource[method] = (...args) => { releases++; return original(...args); };
    await close(early);
    assert(releases === 1, 'backend release must run once');
  } else {
    assert(conn.kind === 'node', 'WASM reader must support cancel');
    await close(early);
  }
  await rejects(() => next(early), 'ERR_CLOSED:stream');
  await rejects(() => close(early), 'ERR_CLOSED:stream');
  assert(releases <= 1, 'duplicate backend release');
  const reused = await query('SELECT 42::INTEGER AS answer');
  try { equal((await next(reused)).i32, [42], 'connection reusable after early close'); }
  finally { await close(reused); }
  checks.push('early-close-release-and-reuse');

  await rejects(() => query('SELECT * FROM missing_compatibility_table'), 'missing_compatibility_table');
  await rejects(async () => {
    const stream = await query("SELECT CASE WHEN i >= 4096 THEN error('compat-fetch-error') ELSE i END FROM range(10000) t(i)");
    try { await drain(stream); } finally { await close(stream); }
  }, 'compat-fetch-error');
  const broken = conn.kind === 'node'
    ? { kind: 'node', result: { fetchChunk: async () => { throw new Error('injected-fetch-error'); } } }
    : { kind: 'wasm', reader: { next: async () => { throw new Error('injected-fetch-error'); } } };
  await rejects(() => next(broken), 'injected-fetch-error');
  checks.push('query-and-fetch-errors-propagate');
  return checks;
}

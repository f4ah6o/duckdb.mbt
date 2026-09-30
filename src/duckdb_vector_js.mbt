///|
// ============================================================================
// JS backends (Node via @duckdb/node-api, browser via duckdb-wasm/Arrow):
// adapt backend-native columnar chunks into the canonical `VectorChunk`
// model (issue #61). Primitive columns move as parallel typed pools in a
// single FFI call per chunk — no per-cell string conversion. Nested/exotic
// cells (LIST/STRUCT/MAP/ARRAY/UNION/BIT/TIME_TZ/BIGNUM, ...) use a
// tagged-JSON encoding decoded by `value_from_any_json`.
//
// Column payload kinds emitted by `js_stream_next_vectors_ffi`:
//   0 BOOL      pool_i32 slot per row (0/1)
//   1 I32       pool_i32 slot per row (ints, DATE days)
//   2 I64       pool_i64 slot per row (bigint/uint, TIME/TIMESTAMP*)
//   3 U64       pool_i64 slot per row (ubit bigint as int64 bits)
//   4 F32       pool_f64 slot per row (float widened exactly)
//   5 F64       pool_f64 slot per row
//   6 STR       pool_str slot per row (varchar/enum)
//   7 BYTES     pool_bytes slot per row (blob/binary)
//   8 I128      pool_i64 *2 per row: upper(i64), lower(u64 bits)
//               (HUGEINT/UHUGEINT/UUID)
//   9 DECIMAL   pool_i64 *2 per row + meta (width << 16 | scale)
//  10 INTERVAL  pool_i64 *3 per row: months, days, micros
//  11 JSON      pool_str slot per row: tagged JSON -> Any(Value)
// ============================================================================

///|
const JS_KIND_BOOL = 0

///|
const JS_KIND_I32 = 1

///|
const JS_KIND_I64 = 2

///|
const JS_KIND_U64 = 3

///|
const JS_KIND_F32 = 4

///|
const JS_KIND_F64 = 5

///|
const JS_KIND_STR = 6

///|
const JS_KIND_BYTES = 7

///|
const JS_KIND_I128 = 8

///|
const JS_KIND_DECIMAL = 9

///|
const JS_KIND_INTERVAL = 10

///|
const JS_KIND_JSON = 11

///|
/// Fetch the next result chunk of `stream` as pooled columnar data.
/// `on_ok` receives, in order: column names, duckdb type ids (0 when the
/// backend cannot supply one), per-column kind tags, per-column meta ints
/// (decimal `(width << 16) | scale`), per-column slot offsets into their
/// kind's pool, the row count, per-cell null flags (`row * ncols + col`,
/// `1` = NULL), then the value pools. All parameters are `FixedArray` per
/// the JS FFI boundary rules.
extern "js" fn js_stream_next_vectors_ffi(
  stream : ResultStream,
  on_ok : (
    FixedArray[String],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    Int,
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int64],
    FixedArray[Double],
    FixedArray[String],
    FixedArray[Bytes],
  ) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stream, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const K_BOOL = 0, K_I32 = 1, K_I64 = 2, K_U64 = 3, K_F32 = 4, K_F64 = 5,
  #|    K_STR = 6, K_BYTES = 7, K_I128 = 8, K_DEC = 9, K_IV = 10, K_JSON = 11;
  #|  const U64_MASK = (1n << 64n) - 1n;
  #|  const textDecoder = new TextDecoder();
  #|
  #|  // node-api DuckDBTypeId -> payload kind (ids match the C API enum).
  #|  const kindForNodeType = (t) => {
  #|    switch (t) {
  #|      case 1: return K_BOOL;
  #|      case 2: case 3: case 4: case 6: case 7: case 13: return K_I32;
  #|      case 5: case 8: case 14: case 39: case 12: case 20: case 21:
  #|      case 22: case 31: return K_I64;
  #|      case 9: return K_U64;
  #|      case 10: return K_F32;
  #|      case 11: return K_F64;
  #|      case 17: case 23: return K_STR;
  #|      case 18: return K_BYTES;
  #|      case 16: case 32: case 27: return K_I128;
  #|      case 19: return K_DEC;
  #|      case 15: return K_IV;
  #|      default: return K_JSON;
  #|    }
  #|  };
  #|
  #|  // apache-arrow type -> duckdb type id (best effort; 0 = unknown).
  #|  const tidForArrowType = (t) => {
  #|    if (!t || t.typeId === undefined || t.typeId === null) return 0;
  #|    switch (t.typeId | 0) {
  #|      case 2: { // Int
  #|        const w = t.bitWidth | 0;
  #|        const s = t.isSigned !== false;
  #|        if (s) return w <= 8 ? 2 : w <= 16 ? 3 : w <= 32 ? 4 : 5;
  #|        return w <= 8 ? 6 : w <= 16 ? 7 : w <= 32 ? 8 : 9;
  #|      }
  #|      case 3: return (t.precision | 0) <= 1 ? 10 : 11; // Float
  #|      case 4: case 15: case 19: case 23: return 18;     // *Binary*
  #|      case 5: case 20: case 24: return 17;             // *Utf8*
  #|      case 6: return 1;                               // Bool
  #|      case 7: return 19;                              // Decimal
  #|      case 8: return 13;                              // Date
  #|      case 9: return 14;                              // Time
  #|      case 10: return 12;                             // Timestamp
  #|      case 11: return 15;                             // Interval
  #|      case 12: case 21: case 25: case 26: return 24;    // *List*
  #|      case 13: return 25;                             // Struct
  #|      case 14: return 28;                             // Union
  #|      case 16: return 33;                             // FixedSizeList
  #|      case 17: return 26;                             // Map
  #|      case 18: return 0;                              // Duration
  #|      case 27: return 23;                             // Dictionary ~ enum
  #|      default: return 0;
  #|    }
  #|  };
  #|
  #|  const kindForTid = (t) => {
  #|    switch (t) {
  #|      case 1: return K_BOOL;
  #|      case 2: case 3: case 4: case 6: case 7: case 13: return K_I32;
  #|      case 5: case 8: case 14: case 39: case 12: case 20: case 21:
  #|      case 22: case 31: return K_I64;
  #|      case 9: return K_U64;
  #|      case 10: return K_F32;
  #|      case 11: return K_F64;
  #|      case 17: case 23: return K_STR;
  #|      case 18: return K_BYTES;
  #|      case 16: case 32: case 27: return K_I128;
  #|      case 19: return K_DEC;
  #|      case 15: return K_IV;
  #|      default: return K_JSON;
  #|    }
  #|  };
  #|
  #|  // Recursive tagged-JSON encoder matching `value_from_any_json`.
  #|  const enc = (v) => {
  #|    if (v === null || v === undefined) return null;
  #|    const t = typeof v;
  #|    if (t === "bigint") return { "$kind": "i", "v": v.toString() };
  #|    if (t === "number" || t === "string" || t === "boolean") return v;
  #|    if (v instanceof Uint8Array) {
  #|      return { "$kind": "blob", "bytes": Array.from(v) };
  #|    }
  #|    if (v instanceof Date) {
  #|      return { "$kind": "date", "days": Math.floor(v.getTime() / 86400000) };
  #|    }
  #|    if (Array.isArray(v)) return v.map(enc);
  #|    if (t === "object") {
  #|      if ("months" in v && "days" in v && "micros" in v) {
  #|        return { "$kind": "iv", "months": v.months, "days": v.days,
  #|                 "micros": String(v.micros) };
  #|      }
  #|      if ("offset" in v && "micros" in v) { // DuckDBTimeTZValue
  #|        return { "$kind": "str", "v": String(v) };
  #|      }
  #|      if ("nanos" in v) {
  #|        return { "$kind": "ts", "micros": String(BigInt(v.nanos) / 1000n) };
  #|      }
  #|      if ("millis" in v) {
  #|        return { "$kind": "ts", "micros": String(BigInt(v.millis) * 1000n) };
  #|      }
  #|      if ("seconds" in v) {
  #|        return { "$kind": "ts", "micros": String(BigInt(v.seconds) * 1000000n) };
  #|      }
  #|      if ("micros" in v) {
  #|        return { "$kind": "ts", "micros": String(BigInt(v.micros)) };
  #|      }
  #|      if ("width" in v && "scale" in v && "value" in v) { // decimal
  #|        return { "$kind": "dec", "width": v.width, "scale": v.scale,
  #|                 "value": String(v.value) };
  #|      }
  #|      if ("hugeint" in v) { // DuckDBUUIDValue (toUint128 is unflipped)
  #|        return { "$kind": "str", "v": String(v) };
  #|      }
  #|      if ("items" in v) { // List / Array
  #|        return { "$kind": "list", "items": v.items.map(enc) };
  #|      }
  #|      if ("entries" in v) {
  #|        if (Array.isArray(v.entries)) { // DuckDBMapValue [{key,value}]
  #|          return { "$kind": "map",
  #|                   "keys": v.entries.map((e) => enc(e.key)),
  #|                   "values": v.entries.map((e) => enc(e.value)) };
  #|        }
  #|        const fields = {};
  #|        for (const k of Object.keys(v.entries)) {
  #|          fields[k] = enc(v.entries[k]);
  #|        }
  #|        return { "$kind": "struct", "fields": fields };
  #|      }
  #|      if ("bytes" in v && v.bytes instanceof Uint8Array) { // blob
  #|        return { "$kind": "blob", "bytes": Array.from(v.bytes) };
  #|      }
  #|      if ("data" in v && v.data instanceof Uint8Array) { // bit
  #|        return { "$kind": "str", "v": String(v) };
  #|      }
  #|      if ("tag" in v && "value" in v) { // union
  #|        return { "$kind": "str", "v": String(v) };
  #|      }
  #|      const fields = {};
  #|      for (const k of Object.keys(v)) fields[k] = enc(v[k]);
  #|      return { "$kind": "struct", "fields": fields };
  #|    }
  #|    return { "$kind": "str", "v": String(v) };
  #|  };
  #|
  #|  // 16-byte little-endian two's complement -> BigInt (arrow Decimal).
  #|  const bytesToI128 = (bytes) => {
  #|    let v = 0n;
  #|    for (let i = bytes.length - 1; i >= 0; i--) {
  #|      v = (v << 8n) | BigInt(bytes[i]);
  #|    }
  #|    if (bytes.length > 15 && bytes[bytes.length - 1] & 0x80) {
  #|      v -= 1n << (8n * BigInt(bytes.length));
  #|    }
  #|    return v;
  #|  };
  #|
  #|  const pushCell = (kind, tid, v, pools, flags, r, ncols, c) => {
  #|    const { i32, i64, f64, str, bytes } = pools;
  #|    const isNull = v === null || v === undefined;
  #|    const vt = typeof v;
  #|    if (isNull) flags[r * ncols + c] = 1;
  #|    switch (kind) {
  #|      case K_BOOL:
  #|        i32.push(isNull ? 0 : (v ? 1 : 0));
  #|        break;
  #|      case K_I32:
  #|        if (isNull) { i32.push(0); break; }
  #|        if (v instanceof Date) { i32.push(Math.floor(v.getTime() / 86400000)); break; }
  #|        if (v && v.days !== undefined) { i32.push(v.days | 0); break; }
  #|        i32.push(Number(v) | 0);
  #|        break;
  #|      case K_I64:
  #|        if (isNull) { i64.push(0n); break; }
  #|        if (vt === "bigint" || vt === "number") { i64.push(BigInt(v)); break; }
  #|        if (tid === 20 && v && v.seconds !== undefined) { i64.push(BigInt(v.seconds) * 1000000n); break; }
  #|        if (tid === 21 && v && v.millis !== undefined) { i64.push(BigInt(v.millis) * 1000n); break; }
  #|        if ((tid === 22 || tid === 39) && v && v.nanos !== undefined) { i64.push(BigInt(v.nanos)); break; }
  #|        if (v && v.micros !== undefined) { i64.push(BigInt(v.micros)); break; }
  #|        if (v && v.nanos !== undefined) { i64.push(BigInt(v.nanos)); break; }
  #|        i64.push(BigInt(v));
  #|        break;
  #|      case K_U64:
  #|        i64.push(isNull ? 0n : BigInt.asIntN(64, BigInt(v)));
  #|        break;
  #|      case K_F32:
  #|        f64.push(isNull ? 0 : Math.fround(v));
  #|        break;
  #|      case K_F64:
  #|        f64.push(isNull ? 0 : Number(v));
  #|        break;
  #|      case K_STR:
  #|        if (isNull) { str.push(""); break; }
  #|        if (typeof v === "string") { str.push(v); break; }
  #|        if (v instanceof Uint8Array) { str.push(textDecoder.decode(v)); break; }
  #|        str.push(String(v));
  #|        break;
  #|      case K_BYTES:
  #|        if (isNull) { bytes.push(new Uint8Array(0)); break; }
  #|        if (v instanceof Uint8Array) { bytes.push(v); break; }
  #|        if (v && v.bytes instanceof Uint8Array) { bytes.push(v.bytes); break; }
  #|        if (v && v.data instanceof Uint8Array) { bytes.push(v.data); break; }
  #|        bytes.push(new Uint8Array(0));
  #|        break;
  #|      case K_I128: {
  #|        let u = 0n;
  #|        if (!isNull) {
  #|          if (v && typeof v.toUint128 === "function") { u = v.toUint128(); }
  #|          else if (v instanceof Uint8Array) { u = bytesToI128(v); }
  #|          else if (v && v.hugeint !== undefined) { u = BigInt.asUintN(128, BigInt(v.hugeint)); }
  #|          else { u = BigInt.asUintN(128, BigInt(v)); }
  #|        }
  #|        const lo = u & U64_MASK;
  #|        const hi = u >> 64n;
  #|        i64.push(BigInt.asIntN(64, hi));
  #|        i64.push(BigInt.asIntN(64, lo));
  #|        break;
  #|      }
  #|      case K_DEC: {
  #|        let u = 0n;
  #|        if (!isNull) {
  #|          if (typeof v === "bigint" || typeof v === "number") { u = BigInt(v); }
  #|          else if (v instanceof Uint8Array) { u = bytesToI128(v); }
  #|          else if (v && v.value !== undefined) { u = BigInt(v.value); }
  #|        }
  #|        u = BigInt.asUintN(128, u);
  #|        i64.push(BigInt.asIntN(64, u >> 64n));
  #|        i64.push(BigInt.asIntN(64, u & U64_MASK));
  #|        break;
  #|      }
  #|      case K_IV:
  #|        if (isNull) { i64.push(0n); i64.push(0n); i64.push(0n); break; }
  #|        i64.push(BigInt(v && v.months !== undefined ? v.months : 0));
  #|        i64.push(BigInt(v && v.days !== undefined ? v.days : 0));
  #|        i64.push(v && v.micros !== undefined ? BigInt(v.micros) : 0n);
  #|        break;
  #|      default:
  #|        str.push(isNull ? "" : JSON.stringify(enc(v)));
  #|        break;
  #|    }
  #|  };
  #|
  #|  // Shared packer: per-column JS value arrays -> pooled layout.
  #|  const emit = (columns, typeIds, colValues, rowCount, arrowTypes) => {
  #|    const ncols = columns.length;
  #|    const kinds = [];
  #|    const metas = [];
  #|    const offsets = [];
  #|    const flags = new Array(rowCount * ncols).fill(0);
  #|    const pools = { i32: [], i64: [], f64: [], str: [], bytes: [] };
  #|    for (let c = 0; c < ncols; c++) {
  #|      const tid = typeIds[c] | 0;
  #|      const kind = kindForTid(tid);
  #|      kinds.push(kind);
  #|      offsets.push(0);
  #|      let meta = 0;
  #|      if (kind === K_DEC) {
  #|        const f = arrowTypes && arrowTypes[c];
  #|        if (f && f.scale !== undefined) {
  #|          meta = ((f.precision | 0) << 16) | (f.scale & 0xffff);
  #|        } else {
  #|          for (let r = 0; r < rowCount; r++) {
  #|            const v = colValues[c][r];
  #|            if (v && v.width !== undefined && v.scale !== undefined) {
  #|              meta = ((v.width | 0) << 16) | (v.scale & 0xffff);
  #|              break;
  #|            }
  #|          }
  #|        }
  #|      }
  #|      metas.push(meta);
  #|    }
  #|    for (let c = 0; c < ncols; c++) {
  #|      const kind = kinds[c];
  #|      const tid = typeIds[c] | 0;
  #|      const pool = kind === K_I128 || kind === K_DEC || kind === K_IV
  #|        ? pools.i64
  #|        : kind === K_BOOL || kind === K_I32 ? pools.i32
  #|        : kind === K_I64 || kind === K_U64 ? pools.i64
  #|        : kind === K_F32 || kind === K_F64 ? pools.f64
  #|        : kind === K_STR || kind === K_JSON ? pools.str
  #|        : pools.bytes;
  #|      offsets[c] = pool.length;
  #|      for (let r = 0; r < rowCount; r++) {
  #|        pushCell(kind, tid, colValues[c][r], pools, flags, r, ncols, c);
  #|      }
  #|    }
  #|    on_ok(columns, typeIds, kinds, metas, offsets, rowCount, flags,
  #|          pools.i32, pools.i64, pools.f64, pools.str, pools.bytes);
  #|  };
  #|
  #|  const runNode = async () => {
  #|    const chunk = await stream.result.fetchChunk();
  #|    if (!chunk) {
  #|      throw new Error("fetchChunk failed");
  #|    }
  #|    if (chunk.rowCount <= 0) {
  #|      on_ok([], [], [], [], [], 0, [], [], [], [], [], []);
  #|      return;
  #|    }
  #|    emit(stream.columns, stream.typeIds || [], chunk.getColumns(),
  #|         chunk.rowCount, null);
  #|  };
  #|
  #|  const runWasm = async () => {
  #|    if (stream.done) {
  #|      on_ok([], [], [], [], [], 0, [], [], [], [], [], []);
  #|      return;
  #|    }
  #|    const next = await stream.reader.next();
  #|    if (!next || next.done || !next.value) {
  #|      stream.done = true;
  #|      on_ok([], [], [], [], [], 0, [], [], [], [], [], []);
  #|      return;
  #|    }
  #|    const batch = next.value;
  #|    const rowCount = batch.numRows ?? batch.length ?? 0;
  #|    let ncols = batch.numCols ?? 0;
  #|    if (!ncols && batch.schema && batch.schema.fields) {
  #|      ncols = batch.schema.fields.length;
  #|    }
  #|    let columns = stream.columns || [];
  #|    let fields = (batch.schema && batch.schema.fields) || [];
  #|    if (columns.length === 0 && fields.length > 0) {
  #|      columns = fields.map((f) => f.name);
  #|      stream.columns = columns;
  #|    }
  #|    const typeIds = [];
  #|    const colValues = [];
  #|    for (let c = 0; c < ncols; c++) {
  #|      const field = fields[c];
  #|      typeIds.push(tidForArrowType(field && field.type));
  #|      const vector = batch.getChildAt ? batch.getChildAt(c) : batch.getChild(c);
  #|      const values = [];
  #|      for (let r = 0; r < rowCount; r++) {
  #|        values.push(vector ? vector.get(r) : null);
  #|      }
  #|      colValues.push(values);
  #|    }
  #|    emit(columns, typeIds, colValues, rowCount, fields.map((f) => f && f.type));
  #|  };
  #|
  #|  const run = async () => {
  #|    if (stream && stream.closed) { on_err("ERR_CLOSED:stream"); return; }
  #|    if (stream && stream.kind === "node") { await runNode(); return; }
  #|    if (stream && stream.kind === "wasm") { await runWasm(); return; }
  #|    throw new Error("unknown stream backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
/// Decode one pooled column into an owned `Vector`.
fn js_vector_from_pools(
  kind : Int,
  meta : Int,
  offset : Int,
  type_id : Int,
  rows : Int,
  ncols : Int,
  null_flags : FixedArray[Int],
  col : Int,
  pool_i32 : FixedArray[Int],
  pool_i64 : FixedArray[Int64],
  pool_f64 : FixedArray[Double],
  pool_str : FixedArray[String],
  pool_bytes : FixedArray[Bytes],
) -> Vector {
  let validity = FixedArray::makei(rows, fn(row) {
    row * ncols + col < null_flags.length() &&
    null_flags[row * ncols + col] == 0
  })
  let logical_type = if type_id > 0 {
    column_type_from_id(type_id)
  } else {
    ColumnType::Unknown(type_id)
  }
  let data : VectorData = match kind {
    JS_KIND_BOOL =>
      VectorData::Bool(
        FixedArray::makei(rows, fn(i) { pool_i32[offset + i] == 1 }),
      )
    JS_KIND_I32 =>
      VectorData::Int32(FixedArray::makei(rows, fn(i) { pool_i32[offset + i] }))
    JS_KIND_I64 =>
      VectorData::Int64(FixedArray::makei(rows, fn(i) { pool_i64[offset + i] }))
    JS_KIND_U64 =>
      VectorData::UInt64(
        FixedArray::makei(rows, fn(i) {
          pool_i64[offset + i].reinterpret_as_uint64()
        }),
      )
    JS_KIND_F32 =>
      VectorData::Float(
        FixedArray::makei(rows, fn(i) { pool_f64[offset + i].to_float() }),
      )
    JS_KIND_F64 =>
      VectorData::Double(
        FixedArray::makei(rows, fn(i) { pool_f64[offset + i] }),
      )
    JS_KIND_STR =>
      VectorData::Varchar(Array::makei(rows, fn(i) { pool_str[offset + i] }))
    JS_KIND_BYTES =>
      VectorData::Blob(Array::makei(rows, fn(i) { pool_bytes[offset + i] }))
    JS_KIND_I128 =>
      VectorData::HugeInt(
        upper=FixedArray::makei(rows, fn(i) { pool_i64[offset + i * 2] }),
        lower=FixedArray::makei(rows, fn(i) {
          pool_i64[offset + i * 2 + 1].reinterpret_as_uint64()
        }),
      )
    JS_KIND_DECIMAL =>
      VectorData::Decimal(
        FixedArray::makei(rows, fn(i) {
          {
            width: meta >> 16,
            scale: meta & 0xffff,
            upper: pool_i64[offset + i * 2],
            lower: pool_i64[offset + i * 2 + 1].reinterpret_as_uint64(),
          }
        }),
      )
    JS_KIND_INTERVAL =>
      VectorData::Interval(
        FixedArray::makei(rows, fn(i) {
          {
            months: pool_i64[offset + i * 3].to_int(),
            days: pool_i64[offset + i * 3 + 1].to_int(),
            micros: pool_i64[offset + i * 3 + 2],
          }
        }),
      )
    _ =>
      VectorData::Any(
        Array::makei(rows, fn(i) { value_from_any_json(pool_str[offset + i]) }),
      )
  }
  { logical_type, data, validity, }
}

///|
/// Fetch the next `VectorChunk` of a JS result stream, or `None` when the
/// stream is exhausted. Shared by `ResultStream::next_chunk`,
/// `Connection::query_chunks`, and `PreparedStatement::execute_chunks`.
fn js_stream_next_vector_chunk(
  stream : ResultStream,
  on_ok : (VectorChunk?) -> Unit,
  on_err : (String) -> Unit,
) -> Unit {
  js_stream_next_vectors_ffi(
    stream,
    fn(
      columns,
      type_ids,
      kinds,
      metas,
      offsets,
      row_count,
      null_flags,
      pool_i32,
      pool_i64,
      pool_f64,
      pool_str,
      pool_bytes,
    ) {
      if row_count <= 0 || columns.length() == 0 {
        on_ok(None)
        return
      }
      let vectors : Array[Vector] = []
      let ncols = columns.length()
      for col in 0..<ncols {
        vectors.push(
          js_vector_from_pools(
            kinds[col],
            metas[col],
            offsets[col],
            type_ids[col],
            row_count,
            ncols,
            null_flags,
            col,
            pool_i32,
            pool_i64,
            pool_f64,
            pool_str,
            pool_bytes,
          ),
        )
      }
      on_ok(Some({ columns: Array::from_fixed_array(columns), vectors, }))
    },
    on_err,
  )
}

///|
/// Open a stream for `sql` and drain it into `VectorChunk`s.
fn js_query_chunks_via_stream(
  conn : Connection,
  sql : String,
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  js_query_stream(
    conn,
    sql,
    fn(stream) { js_drain_stream_chunks(stream, [], on_done~) },
    fn(message) { on_done(Err(query_error(message))) },
  )
}

///|
/// Drain a stream into `VectorChunk`s, then close it.
fn js_drain_stream_chunks(
  stream : ResultStream,
  acc : Array[VectorChunk],
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  js_stream_next_vector_chunk(
    stream,
    fn(chunk) {
      match chunk {
        Some(c) => {
          acc.push(c)
          js_drain_stream_chunks(stream, acc, on_done~)
        }
        None =>
          js_stream_close(stream, fn() { on_done(Ok(acc)) }, fn(message) {
            on_done(Err(backend_error("js", message)))
          })
      }
    },
    fn(message) { on_done(Err(query_error(message))) },
  )
}

// ============================================================================
// Public canonical-chunk API (issue #61)
// ============================================================================

///|
/// Run `sql` and return the result as canonical typed `VectorChunk`s —
/// columnar `Vector`s with owned typed arrays and per-row validity, no
/// per-cell string conversion. Primitive columns are moved as typed pools;
/// nested types (LIST/STRUCT/MAP/ARRAY) decode into `Value`s without a
/// string round-trip. Consumed via `Vector::value_at`,
/// `Vector::ints`/`int64s`/`doubles`/..., `VectorChunk::get_value`, or
/// materialized to the compat `QueryResult` via `chunks_to_query_result`.
pub fn Connection::query_chunks(
  self : Connection,
  sql : String,
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  js_query_chunks_via_stream(self, sql, on_done~)
}

///|
/// Execute the prepared statement and return canonical typed `VectorChunk`s.
pub fn PreparedStatement::execute_chunks(
  self : PreparedStatement,
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  js_execute_prepared_stream(
    self,
    fn(stream) { js_drain_stream_chunks(stream, [], on_done~) },
    fn(message) { on_done(Err(query_error(message))) },
  )
}

///|
/// Fetch the next chunk of the stream as a canonical typed `VectorChunk`,
/// or `None` at end-of-stream. `ResultStream::next` builds its string
/// `DataChunk` on top of this same canonical chunk abstraction.
pub fn ResultStream::next_chunk(
  self : ResultStream,
  on_done~ : (Result[VectorChunk?, DuckDBError]) -> Unit,
) -> Unit {
  js_stream_next_vector_chunk(self, fn(chunk) { on_done(Ok(chunk)) }, fn(
    message,
  ) {
    on_done(Err(query_error(message)))
  })
}

// ============================================================================
// Tagged-JSON decode for Any cells (JS backend nested/exotic values)
// ============================================================================

///|
/// Decode one `Any` cell produced by the JS backends. The JS shim encodes a
/// cell as JSON: scalars verbatim, bigint as `{"$i": "<digits>"}`, objects
/// tagged via `$kind` (`date`, `ts`, `iv`, `dec`, `hi`, `list`, `struct`,
/// `map`, `blob`, `str`). Plain scalars map to the natural `Value`.
fn value_from_any_json(json : String) -> Value {
  match (try? @json.parse(json)) {
    Ok(parsed) => any_json_to_value(parsed)
    Err(_) => Value::String(json)
  }
}

///|
fn any_json_to_value(json : Json) -> Value {
  match json {
    Json::Null => Value::Null
    Json::True => Value::Bool(true)
    Json::False => Value::Bool(false)
    Json::Number(n, ..) => {
      // Integers stay integers.
      let as_int = n.to_int64()
      if as_int.to_double() == n && n.abs() < 9007199254740992.0 {
        Value::Int64(as_int)
      } else {
        Value::Double(n)
      }
    }
    Json::String(s) => Value::String(s)
    Json::Array(items) => {
      let values = items.map(fn(item) { any_json_to_value(item) })
      Value::List(values)
    }
    Json::Object(obj) => {
      let kind = match obj.get("$kind") {
        Some(Json::String(s)) => s
        _ => ""
      }
      match kind {
        "i" => // {"i": "<bigint digits>"}
          match obj.get("v") {
            Some(Json::String(s)) => Value::String(s)
            _ => Value::String(json.stringify())
          }
        "date" =>
          match obj.get("days") {
            Some(Json::Number(d, ..)) => Value::Date(d.to_int())
            _ => Value::String(json.stringify())
          }
        "ts" =>
          match obj.get("micros") {
            Some(Json::String(s)) => Value::Timestamp(parse_int64_digits(s))
            _ => Value::String(json.stringify())
          }
        "iv" =>
          match (obj.get("months"), obj.get("days"), obj.get("micros")) {
            (
              Some(Json::Number(m, ..)),
              Some(Json::Number(d, ..)),
              Some(Json::String(us)),
            ) =>
              Value::Interval({
                months: m.to_int(),
                days: d.to_int(),
                micros: parse_int64_digits(us),
              })
            _ => Value::String(json.stringify())
          }
        "dec" =>
          match (obj.get("width"), obj.get("scale"), obj.get("value")) {
            (
              Some(Json::Number(w, ..)),
              Some(Json::Number(s, ..)),
              Some(Json::String(v)),
            ) =>
              Value::Decimal(
                decimal_from_scaled_bigint(
                  BigInt::from_string(v),
                  w.to_int(),
                  s.to_int(),
                ),
              )
            _ => Value::String(json.stringify())
          }
        "hi" =>
          match (obj.get("lo"), obj.get("hi")) {
            (Some(Json::String(lo)), Some(Json::String(hi))) =>
              Value::HugeInt(
                lower=parse_uint64_digits(lo),
                upper=parse_int64_digits(hi),
              )
            _ => Value::String(json.stringify())
          }
        "list" =>
          match obj.get("items") {
            Some(Json::Array(items)) =>
              Value::List(items.map(fn(item) { any_json_to_value(item) }))
            _ => Value::String(json.stringify())
          }
        "struct" =>
          match obj.get("fields") {
            Some(Json::Object(fields_obj)) => {
              let fields : Array[String] = []
              let values : Array[Value] = []
              for name, field_value in fields_obj {
                fields.push(name)
                values.push(any_json_to_value(field_value))
              }
              Value::Struct(fields~, values~)
            }
            _ => Value::String(json.stringify())
          }
        "map" =>
          match (obj.get("keys"), obj.get("values")) {
            (Some(Json::Array(ks)), Some(Json::Array(vs))) => {
              let keys = ks.map(fn(item) { any_json_to_value(item) })
              let values = vs.map(fn(item) { any_json_to_value(item) })
              Value::Map(keys~, values~)
            }
            _ => Value::String(json.stringify())
          }
        "blob" =>
          match obj.get("bytes") {
            Some(Json::Array(items)) => {
              let bytes : Array[Byte] = []
              for item in items {
                match item {
                  Json::Number(n, ..) => bytes.push(n.to_int().to_byte())
                  _ => ()
                }
              }
              Value::Blob(Bytes::from_array(bytes))
            }
            _ => Value::String(json.stringify())
          }
        "str" =>
          match obj.get("v") {
            Some(Json::String(s)) => Value::String(s)
            _ => Value::String(json.stringify())
          }
        _ =>
          // Untagged object → struct-like string fallback.
          Value::String(json.stringify())
      }
    }
  }
}

///|
/// Parse a decimal integer string into Int64 (digits with optional `-`).
fn parse_int64_digits(s : String) -> Int64 {
  let mut result = 0L
  let mut negative = false
  let mut start = 0
  if s.length() > 0 && s[0] == '-' {
    negative = true
    start = 1
  }
  for i in start..<s.length() {
    let c = s[i]
    if c >= '0' && c <= '9' {
      result = result * 10L + (c.to_int() - '0'.to_int()).to_int64()
    }
  }
  if negative {
    -result
  } else {
    result
  }
}

///|
/// Parse a decimal integer string into UInt64.
fn parse_uint64_digits(s : String) -> UInt64 {
  let mut result = 0UL
  for c in s {
    if c >= '0' && c <= '9' {
      result = result * 10UL + (c.to_int() - '0'.to_int()).to_uint64()
    }
  }
  result
}

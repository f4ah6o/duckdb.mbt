///|
// ============================================================================
// JS backends (Node via @duckdb/node-api, browser via duckdb-wasm/Arrow):
// adapt backend-native columnar chunks into the canonical `VectorChunk`
// model (issue #61). Primitive columns move as parallel typed pools in a
// single FFI call per chunk — no per-cell string conversion. Nested
// LIST/STRUCT/MAP/ARRAY columns are decomposed into extra pseudo-columns
// ("slots") appended after the real columns; a small `shapes` pool describes
// each nested column's child slot ids and a `names` pool carries struct
// field names, so nested values decode into real `VectorData::List`/`Struct`/
// `Map` without a string round-trip. Genuinely unsupported types
// (UNION/BIT/TIME_TZ/BIGNUM, ...) keep the tagged-JSON encoding decoded by
// `value_from_any_json`.
//
// Slot tables: every emitted column — real or nested child — gets one slot
// i with its own `typeIds[i]`, `kinds[i]`, `metas[i]`, `offsets[i]` (into the
// kind's pool), `slotRows[i]` (child slots differ from the parent row
// count), and a validity segment inside the flat `flags` array starting at
// the prefix sum of `slotRows[0..i)`. `shapeOffsets[i]` points into `shapes`
// (-1 when the slot is not nested) and `nameOffsets[i]` into `names`.
// Slots are allocated DFS: a column's slot precedes its nested children's,
// so the real columns are the slots `js_top_level_slots` reports, not a
// contiguous `0..<columns.length` range.
//
// Shape tags in `shapes` at `shapeOffsets[i]`:
//   1 LIST/ARRAY: [1, childSlot]
//   2 STRUCT:     [2, nFields, childSlot..] with names[nameOffsets..+nFields]
//   3 MAP:        [3, keysSlot, valuesSlot]
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
//  12 LIST      pool_i64 *2 per row: child offset, length (LIST and ARRAY)
//  13 STRUCT    no payload; children via shapes pool + names pool
//  14 MAP       pool_i64 *2 per row: entry offset, length
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
const JS_KIND_LIST = 12

///|
const JS_KIND_STRUCT = 13

///|
const JS_KIND_MAP = 14

///|
// Shape descriptor tags inside the `shapes` pool (see the protocol comment
// above `js_stream_next_vectors_ffi`).
const JS_SHAPE_LIST = 1

///|
const JS_SHAPE_STRUCT = 2

///|
const JS_SHAPE_MAP = 3

///|
/// Fetch the next result chunk of `stream` as pooled columnar data.
/// `on_ok` receives, in order: top-level column names; the per-slot tables
/// (`typeIds`, `kinds`, `metas`, `offsets`, `slotRows`); the flat `flags`
/// validity segments; the nested `shapes` descriptor pool plus per-slot
/// `shapeOffsets`; the `names` pool plus per-slot `nameOffsets`; the top-level
/// `rowCount`; then the value pools. Top-level columns take their slot
/// before their nested children are appended (DFS), so they are not
/// necessarily slots `0..<columns.length` — `js_top_level_slots` recovers
/// the top-level set. All parameters are `FixedArray` per the JS FFI
/// boundary rules.
extern "js" fn js_stream_next_vectors_ffi(
  stream : ResultStream,
  on_ok : (
    FixedArray[String],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[Int],
    FixedArray[String],
    FixedArray[Int],
    Int,
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
  #|    K_STR = 6, K_BYTES = 7, K_I128 = 8, K_DEC = 9, K_IV = 10, K_JSON = 11,
  #|    K_LIST = 12, K_STRUCT = 13, K_MAP = 14;
  #|  const S_LIST = 1, S_STRUCT = 2, S_MAP = 3;
  #|  const U64_MASK = (1n << 64n) - 1n;
  #|  const textDecoder = new TextDecoder();
  #|  const emptyOk = () => on_ok([], [], [], [], [], [], [], [], [], [], [],
  #|    0, [], [], [], [], []);
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
  #|      case 9: return (t.unit | 0) === 3 ? 39 : 14;    // Time (ns -> TIME_NS)
  #|      case 10: {                                     // Timestamp
  #|        const u = t.unit | 0;
  #|        return u === 0 ? 20 : u === 1 ? 21 : u === 3 ? 22 : 12;
  #|      }
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
  #|  // duckdb type id -> payload kind.
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
  #|      case 24: case 33: return K_LIST;   // LIST / ARRAY
  #|      case 25: return K_STRUCT;
  #|      case 26: return K_MAP;
  #|      default: return K_JSON;
  #|    }
  #|  };
  #|
  #|  // Recursive tagged-JSON encoder matching `value_from_any_json`.
  #|  const enc = (v) => {
  #|    if (v === null || v === undefined) return null;
  #|    const t = typeof v;
  #|    // Bigints render as strings here — same fidelity trade-off the
  #|    // tagged-JSON Value decode already makes elsewhere (Value::String).
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
  #|      if ("nanos" in v) { // DuckDBTimestampNsValue — keep raw nanos
  #|        return { "$kind": "tsns", "nanos": String(BigInt(v.nanos)) };
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
  #|  // Append one cell of a primitive/JSON slot; `flags` is the slot's own
  #|  // validity segment (flags[r] = 1 means NULL).
  #|  const pushCell = (kind, tid, meta, v, pools, flags, r) => {
  #|    const { i32, i64, f64, str, bytes } = pools;
  #|    const isNull = v === null || v === undefined;
  #|    if (isNull) flags[r] = 1;
  #|    const vt = typeof v;
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
  #|        if (vt === "bigint" || vt === "number") {
  #|          // Raw scalars (Arrow values): normalize S/MS to micros, NS stays raw.
  #|          const raw = BigInt(v);
  #|          if (tid === 20) { i64.push(raw * 1000000n); break; }
  #|          if (tid === 21) { i64.push(raw * 1000n); break; }
  #|          if (tid === 14 && meta > 0) { i64.push(raw * (10n ** BigInt(meta))); break; }
  #|          i64.push(raw); break;
  #|        }
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
  #|  // ---- column sources -------------------------------------------------
  #|  // A "source" abstracts one logical column (real or nested child):
  #|  //   rows:      element count of this slot
  #|  //   tid:       duckdb type id
  #|  //   dtype:     node-api DuckDBType (or null)
  #|  //   atype:     arrow DataType (or null)
  #|  //   get(r):    JS cell value for row r (primitive payload + validity)
  #|  //   isValid(r):row validity
  #|  //   nested:    {list|struct|map} decomposers, or null when the backend
  #|  //              cannot decompose this slot (falls back to tagged JSON)
  #|
  #|  const nodeSrc = (values, dtype) => {
  #|    const src = {
  #|      rows: values.length,
  #|      tid: dtype && dtype.typeId !== undefined && dtype.typeId !== null
  #|        ? dtype.typeId | 0 : 0,
  #|      dtype: dtype || null,
  #|      atype: null,
  #|      get: (r) => values[r],
  #|      isValid: (r) => values[r] !== null && values[r] !== undefined,
  #|      nested: null,
  #|    };
  #|    src.nested = {
  #|      // LIST/ARRAY: flatten per-row .items into one child source.
  #|      list: () => {
  #|        const flat = [], offs = [], lens = [];
  #|        for (let r = 0; r < src.rows; r++) {
  #|          const v = values[r];
  #|          const items = v && Array.isArray(v.items) ? v.items : null;
  #|          offs.push(flat.length);
  #|          lens.push(items ? items.length : 0);
  #|          if (items) for (const item of items) flat.push(item);
  #|        }
  #|        return {
  #|          bounds: (r) => [offs[r], lens[r]],
  #|          child: nodeSrc(flat, src.dtype && src.dtype.valueType),
  #|        };
  #|      },
  #|      // STRUCT: one child source per field; names come from the declared
  #|      // DuckDBStructType, falling back to the first non-null row's keys.
  #|      struct: () => {
  #|        const dtype = src.dtype;
  #|        let fieldNames = (dtype && dtype.entryNames) || null;
  #|        if (!fieldNames) {
  #|          for (let r = 0; r < src.rows; r++) {
  #|            const v = values[r];
  #|            if (v && v.entries) { fieldNames = Object.keys(v.entries); break; }
  #|          }
  #|        }
  #|        fieldNames = fieldNames || [];
  #|        const entryTypes = (dtype && dtype.entryTypes) || [];
  #|        return fieldNames.map((name, i) => ({
  #|          name: String(name),
  #|          src: nodeSrc(
  #|            Array.from({ length: src.rows }, (_, r) => {
  #|              const v = values[r];
  #|              return v && v.entries ? v.entries[name] : null;
  #|            }),
  #|            entryTypes[i] || null,
  #|          ),
  #|        }));
  #|      },
  #|      // MAP: flatten per-row .entries [{key,value}] into keys/values.
  #|      map: () => {
  #|        const keys = [], vals = [], offs = [], lens = [];
  #|        for (let r = 0; r < src.rows; r++) {
  #|          const v = values[r];
  #|          const entries = v && Array.isArray(v.entries) ? v.entries : null;
  #|          offs.push(keys.length);
  #|          lens.push(entries ? entries.length : 0);
  #|          if (entries) {
  #|            for (const e of entries) {
  #|              keys.push(e ? e.key : null);
  #|              vals.push(e ? e.value : null);
  #|            }
  #|          }
  #|        }
  #|        return {
  #|          bounds: (r) => [offs[r], lens[r]],
  #|          keys: nodeSrc(keys, src.dtype && src.dtype.keyType),
  #|          values: nodeSrc(vals, src.dtype && src.dtype.valueType),
  #|        };
  #|      },
  #|    };
  #|    return src;
  #|  };
  #|
  #|  // apache-arrow >= 9 wraps `Data` chunks in `vector.data` (an array);
  #|  // older arrows exposed a single `Data`. Normalize to a chunk list.
  #|  const dataChunks = (v) => {
  #|    const d = v && v.data;
  #|    return Array.isArray(d) ? d : d ? [d] : [];
  #|  };
  #|
  #|  // Row bounds of a variable-width parent (LIST/MAP): each chunk's
  #|  // `valueOffsets` is windowed to the chunk's own row range while its
  #|  // values are absolute indexes into that chunk's child data, so the
  #|  // emitted bounds shift by the chunk's base row and its base element
  #|  // index inside the concatenated child vector.
  #|  const nestedBounds = (vector, child) => {
  #|    const chunks = dataChunks(vector);
  #|    const childChunks = dataChunks(child);
  #|    const rowBase = [], childBase = [];
  #|    let rAcc = 0, cAcc = 0;
  #|    for (let i = 0; i < chunks.length; i++) {
  #|      rowBase.push(rAcc);
  #|      childBase.push(cAcc);
  #|      rAcc += chunks[i].length;
  #|      cAcc += childChunks[i] ? childChunks[i].length : 0;
  #|    }
  #|    return (r) => {
  #|      let c = 0;
  #|      while (c + 1 < chunks.length && r >= rowBase[c] + chunks[c].length) c++;
  #|      const vo = chunks[c] && chunks[c].valueOffsets;
  #|      if (!vo) {
  #|        throw new Error("nested vector is missing valueOffsets");
  #|      }
  #|      const local = r - rowBase[c];
  #|      const off = Number(vo[local]);
  #|      return [childBase[c] + off, Number(vo[local + 1]) - off];
  #|    };
  #|  };
  #|
  #|  // Arrow interval cells: vector.get is wrong for MONTH_DAY_NANO
  #|  // (arrow's visitInterval only special-cases DAY_TIME, so MDN values
  #|  // decode through the YearMonth path as garbage) and returns raw
  #|  // Int32Array tuples otherwise. Decode per `type.unit` from the
  #|  // chunk-local `values` buffers, converting to the node-api
  #|  // {months, days, micros} shape: YEAR_MONTH total months;
  #|  // DAY_TIME [days, ms] pairs; MONTH_DAY_NANO 16-byte
  #|  // [months:i32, days:i32, nanos:i64] records.
  #|  const intervalGet = (vector, type) => {
  #|    const chunks = dataChunks(vector);
  #|    const unit = type && type.unit !== undefined && type.unit !== null
  #|      ? type.unit | 0 : 2;
  #|    const stride = unit === 0 ? 4 : unit === 1 ? 8 : 16;
  #|    const rowBase = [];
  #|    let rAcc = 0;
  #|    for (const d of chunks) { rowBase.push(rAcc); rAcc += d.length; }
  #|    return (r) => {
  #|      if (vector && typeof vector.isValid === "function" &&
  #|          !vector.isValid(r)) {
  #|        return null;
  #|      }
  #|      let c = 0;
  #|      while (c + 1 < chunks.length && r >= rowBase[c] + chunks[c].length) {
  #|        c++;
  #|      }
  #|      const d = chunks[c];
  #|      const vals = d && d.values;
  #|      if (!vals || !vals.buffer) return null;
  #|      const i = r - rowBase[c];
  #|      const dv = new DataView(vals.buffer, vals.byteOffset, vals.byteLength);
  #|      const at = stride * i;
  #|      if (unit === 0) {
  #|        return { months: dv.getInt32(at, true), days: 0, micros: 0n };
  #|      }
  #|      if (unit === 1) {
  #|        return { months: 0, days: dv.getInt32(at, true),
  #|                 micros: BigInt(dv.getInt32(at + 4, true)) * 1000n };
  #|      }
  #|      return { months: dv.getInt32(at, true), days: dv.getInt32(at + 4, true),
  #|               micros: dv.getBigInt64(at + 8, true) / 1000n };
  #|    };
  #|  };
  #|
  #|  const wasmSrc = (vector, atype) => {
  #|    const type = (vector && vector.type) || atype || null;
  #|    const src = {
  #|      rows: vector ? vector.length : 0,
  #|      tid: tidForArrowType(type),
  #|      dtype: null,
  #|      atype: type,
  #|      vector: vector,
  #|      get: (r) => vector.get(r),
  #|      isValid: (r) => {
  #|        if (vector && typeof vector.isValid === "function") {
  #|          return !!vector.isValid(r);
  #|        }
  #|        const v = vector ? vector.get(r) : null;
  #|        return v !== null && v !== undefined;
  #|      },
  #|      nested: null,
  #|    };
  #|    if (vector && typeof vector.getChildAt === "function") {
  #|      // 0-row chunks carry no child data at all (`data.children` is
  #|      // empty), so arrow's getChildAt throws while building a Vector;
  #|      // fall back to a schema-only source then.
  #|      const childSrc = (index, field) => {
  #|        let v = null;
  #|        try {
  #|          v = vector.getChildAt(index);
  #|        } catch (e) {
  #|          v = null;
  #|        }
  #|        return v ? wasmSrc(v) : wasmSchemaSrc(field);
  #|      };
  #|      const typeChildren = (type && type.children) || [];
  #|      src.nested = {
  #|        // LIST/FixedSizeList: real offsets buffer, or synthesized
  #|        // r * listSize bounds for fixed arrays (their children are
  #|        // sliced to the parent window, so emitted indexes are
  #|        // child-relative).
  #|        list: () => {
  #|          let childVec = null;
  #|          try { childVec = vector.getChildAt(0); } catch (e) { childVec = null; }
  #|          const child =
  #|            childVec ? wasmSrc(childVec) : wasmSchemaSrc(typeChildren[0]);
  #|          const fixed = type && type.listSize !== undefined
  #|            ? type.listSize | 0 : -1;
  #|          const bounds = fixed >= 0
  #|            ? (r) => [r * fixed, fixed]
  #|            : childVec
  #|              ? nestedBounds(vector, childVec)
  #|              : (r) => [0, 0];
  #|          return { bounds, child };
  #|        },
  #|        // STRUCT: child vectors align 1:1 with parent rows; field names
  #|        // come from type.children (arrow Field).
  #|        struct: () => {
  #|          return typeChildren.map((f, i) => ({
  #|            name: (f && f.name) || "",
  #|            src: childSrc(i, f),
  #|          }));
  #|        },
  #|        // MAP: list-of-entries; entries child is a struct vector whose
  #|        // children are keys/values.
  #|        map: () => {
  #|          let entries = null;
  #|          try { entries = vector.getChildAt(0); } catch (e) { entries = null; }
  #|          const entriesField = typeChildren[0] || null;
  #|          const entryFields =
  #|            (entriesField && entriesField.type && entriesField.type.children) || [];
  #|          let keysVec = null, valuesVec = null;
  #|          if (entries) {
  #|            try { keysVec = entries.getChildAt(0); } catch (e) { keysVec = null; }
  #|            try { valuesVec = entries.getChildAt(1); } catch (e) { valuesVec = null; }
  #|          }
  #|          return {
  #|            bounds: entries ? nestedBounds(vector, entries) : (r) => [0, 0],
  #|            keys: keysVec ? wasmSrc(keysVec) : wasmSchemaSrc(entryFields[0]),
  #|            values: valuesVec ? wasmSrc(valuesVec) : wasmSchemaSrc(entryFields[1]),
  #|          };
  #|        },
  #|      };
  #|    }
  #|    if (vector && type && (type.typeId | 0) === 11) {
  #|      src.get = intervalGet(vector, type);
  #|    }
  #|    return src;
  #|  };
  #|
  #|  // Schema-only source for empty results: mirrors wasmSrc but walks the
  #|  // arrow field tree instead of vectors, so a 0-row emit still carries
  #|  // the full nested shape/name descriptors.
  #|  const wasmSchemaSrc = (field) => {
  #|    const type = field && field.type;
  #|    // arrow keeps child Fields on the DataType, not on the Field.
  #|    const children = (type && type.children) || [];
  #|    return {
  #|      rows: 0,
  #|      tid: tidForArrowType(type),
  #|      dtype: null,
  #|      atype: type,
  #|      get: () => null,
  #|      isValid: () => true,
  #|      nested: {
  #|        list: () => ({
  #|          bounds: () => [0, 0],
  #|          child: wasmSchemaSrc(children[0]),
  #|        }),
  #|        struct: () => children.map((cf) => ({
  #|          name: (cf && cf.name) || "",
  #|          src: wasmSchemaSrc(cf),
  #|        })),
  #|        map: () => {
  #|          const ek =
  #|            (children[0] && children[0].type && children[0].type.children) || [];
  #|          return {
  #|            bounds: () => [0, 0],
  #|            keys: wasmSchemaSrc(ek[0]),
  #|            values: wasmSchemaSrc(ek[1]),
  #|          };
  #|        },
  #|      },
  #|    };
  #|  };
  #|
  #|  // ---- pooled emission --------------------------------------------------
  #|  // Appends one slot per logical column, recursing into nested children.
  #|  const emit = (columns, sources, rowCount) => {
  #|    stream.emitted = true;
  #|    const tids = [], kinds = [], metas = [], offsets = [], slotRows = [];
  #|    const flagSegs = [];
  #|    const shapes = [], shapeOffsets = [];
  #|    const names = [], nameOffsets = [];
  #|    const pools = { i32: [], i64: [], f64: [], str: [], bytes: [] };
  #|
  #|    const poolFor = (kind) =>
  #|      kind === K_I128 || kind === K_DEC || kind === K_IV ||
  #|      kind === K_LIST || kind === K_MAP ? pools.i64
  #|      : kind === K_BOOL || kind === K_I32 ? pools.i32
  #|      : kind === K_I64 || kind === K_U64 ? pools.i64
  #|      : kind === K_F32 || kind === K_F64 ? pools.f64
  #|      : kind === K_STR || kind === K_JSON ? pools.str
  #|      : pools.bytes;
  #|
  #|    const metaFor = (kind, tid, src) => {
  #|      let meta = 0;
  #|      if (tid === 14 && src.atype) {
  #|        // Arrow Time32 carries s/ms; store the 10^meta scale to micros.
  #|        const u = src.atype.unit | 0;
  #|        meta = u === 0 ? 6 : u === 1 ? 3 : 0;
  #|      }
  #|      if (kind === K_DEC) {
  #|        const dt = src.dtype, at = src.atype;
  #|        if (dt && dt.width !== undefined && dt.scale !== undefined) {
  #|          meta = ((dt.width | 0) << 16) | (dt.scale & 0xffff);
  #|        } else if (at && at.scale !== undefined) {
  #|          meta = ((at.precision | 0) << 16) | (at.scale & 0xffff);
  #|        } else {
  #|          for (let r = 0; r < src.rows; r++) {
  #|            const v = src.get(r);
  #|            if (v && v.width !== undefined && v.scale !== undefined) {
  #|              meta = ((v.width | 0) << 16) | (v.scale & 0xffff);
  #|              break;
  #|            }
  #|          }
  #|        }
  #|      }
  #|      return meta;
  #|    };
  #|
  #|    const emitColumn = (src) => {
  #|      const tid = src.tid | 0;
  #|      let kind = kindForTid(tid);
  #|      if ((kind === K_LIST || kind === K_STRUCT || kind === K_MAP) &&
  #|          !src.nested) {
  #|        kind = K_JSON;
  #|      }
  #|      const s = kinds.length;
  #|      const flags = new Array(src.rows).fill(0);
  #|      flagSegs.push(flags);
  #|      kinds.push(kind);
  #|      tids.push(tid);
  #|      slotRows.push(src.rows);
  #|      shapeOffsets.push(-1);
  #|      nameOffsets.push(0);
  #|      metas.push(metaFor(kind, tid, src));
  #|
  #|      if (kind === K_LIST) {
  #|        offsets.push(pools.i64.length);
  #|        const nested = src.nested.list();
  #|        for (let r = 0; r < src.rows; r++) {
  #|          const b = nested.bounds(r);
  #|          pools.i64.push(BigInt(b[0]));
  #|          pools.i64.push(BigInt(b[1]));
  #|          if (!src.isValid(r)) flags[r] = 1;
  #|        }
  #|        const childSlot = emitColumn(nested.child);
  #|        shapeOffsets[s] = shapes.length;
  #|        shapes.push(S_LIST, childSlot);
  #|        return s;
  #|      }
  #|      if (kind === K_STRUCT) {
  #|        offsets.push(0);
  #|        for (let r = 0; r < src.rows; r++) {
  #|          if (!src.isValid(r)) flags[r] = 1;
  #|        }
  #|        const fields = src.nested.struct();
  #|        nameOffsets[s] = names.length;
  #|        for (const f of fields) names.push(f.name);
  #|        const childSlots = fields.map((f) => emitColumn(f.src));
  #|        shapeOffsets[s] = shapes.length;
  #|        shapes.push(S_STRUCT, fields.length, ...childSlots);
  #|        return s;
  #|      }
  #|      if (kind === K_MAP) {
  #|        offsets.push(pools.i64.length);
  #|        const nested = src.nested.map();
  #|        for (let r = 0; r < src.rows; r++) {
  #|          const b = nested.bounds(r);
  #|          pools.i64.push(BigInt(b[0]));
  #|          pools.i64.push(BigInt(b[1]));
  #|          if (!src.isValid(r)) flags[r] = 1;
  #|        }
  #|        const keysSlot = emitColumn(nested.keys);
  #|        const valuesSlot = emitColumn(nested.values);
  #|        shapeOffsets[s] = shapes.length;
  #|        shapes.push(S_MAP, keysSlot, valuesSlot);
  #|        return s;
  #|      }
  #|      const pool = poolFor(kind);
  #|      offsets.push(pool.length);
  #|      for (let r = 0; r < src.rows; r++) {
  #|        pushCell(kind, tid, metas[s], src.get(r), pools, flags, r);
  #|      }
  #|      return s;
  #|    };
  #|
  #|    for (let c = 0; c < columns.length; c++) {
  #|      emitColumn(sources[c]);
  #|    }
  #|    const flags = [];
  #|    for (const seg of flagSegs) for (const f of seg) flags.push(f);
  #|    on_ok(columns, tids, kinds, metas, offsets, slotRows, flags,
  #|          shapes, shapeOffsets, names, nameOffsets, rowCount,
  #|          pools.i32, pools.i64, pools.f64, pools.str, pools.bytes);
  #|  };
  #|
  #|  const runNode = async () => {
  #|    const chunk = await stream.result.fetchChunk();
  #|    if (!chunk) {
  #|      throw new Error("fetchChunk failed");
  #|    }
  #|    if (chunk.rowCount <= 0) {
  #|      if (stream.emitted) {
  #|        emptyOk();
  #|        return;
  #|      }
  #|      // First read on an empty result: fall through and emit a 0-row
  #|      // slot table so callers still see the column schema.
  #|    }
  #|    const columns = stream.columns || [];
  #|    const typeIds = stream.typeIds || [];
  #|    const duckTypes = stream.duckTypes || (
  #|      stream.result && typeof stream.result.columnType === "function"
  #|        ? columns.map((_, i) => {
  #|          try { return stream.result.columnType(i); } catch (e) { return null; }
  #|        })
  #|        : null
  #|    );
  #|    const colValues = chunk.getColumns();
  #|    const sources = [];
  #|    for (let c = 0; c < columns.length; c++) {
  #|      const src = nodeSrc(colValues[c] || [], duckTypes ? duckTypes[c] : null);
  #|      if (typeIds[c] !== undefined && typeIds[c] !== null) {
  #|        src.tid = typeIds[c] | 0;
  #|      }
  #|      sources.push(src);
  #|    }
  #|    emit(columns, sources, chunk.rowCount);
  #|  };
  #|
  #|  const runWasm = async () => {
  #|    if (stream.done) {
  #|      emptyOk();
  #|      return;
  #|    }
  #|    const next = await stream.reader.next();
  #|    if (!next || next.done || !next.value) {
  #|      stream.done = true;
  #|      if (!stream.emitted) {
  #|        // Empty result (e.g. WHERE false): emit a 0-row slot table from
  #|        // the stream schema so column names/types survive the drain.
  #|        const fields =
  #|          (stream.reader.schema && stream.reader.schema.fields) || [];
  #|        if (fields.length > 0) {
  #|          emit(fields.map((f) => f.name),
  #|               fields.map(wasmSchemaSrc), 0);
  #|          return;
  #|        }
  #|      }
  #|      emptyOk();
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
  #|    const sources = [];
  #|    for (let c = 0; c < ncols; c++) {
  #|      const field = fields[c];
  #|      const vector = batch.getChildAt ? batch.getChildAt(c) : batch.getChild(c);
  #|      sources.push(wasmSrc(vector, field && field.type));
  #|    }
  #|    emit(columns, sources, rowCount);
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
/// All pooled slot tables for one emitted chunk. Slot `i` decodes column i
/// (a real top-level column for `i < ncols`, a nested child slot beyond
/// that). `flag_bases[i]` is the prefix sum of `slot_rows[0..i)` — the start
/// of slot i's segment inside the flat `null_flags` array. Nested children
/// are located through the `shapes`/`shape_offsets` descriptor pool; struct
/// field names through `names`/`name_offsets`.
priv struct JsVectorPools {
  type_ids : FixedArray[Int]
  kinds : FixedArray[Int]
  metas : FixedArray[Int]
  offsets : FixedArray[Int]
  slot_rows : FixedArray[Int]
  flag_bases : FixedArray[Int]
  null_flags : FixedArray[Int]
  shapes : FixedArray[Int]
  shape_offsets : FixedArray[Int]
  names : FixedArray[String]
  name_offsets : FixedArray[Int]
  pool_i32 : FixedArray[Int]
  pool_i64 : FixedArray[Int64]
  pool_f64 : FixedArray[Double]
  pool_str : FixedArray[String]
  pool_bytes : FixedArray[Bytes]
}

///|
/// Decode one pooled slot (column or nested child) into an owned `Vector`,
/// recursing into child slots described by the `shapes` pool.
fn js_vector_from_pools(pools : JsVectorPools, slot : Int) -> Vector {
  let rows = pools.slot_rows[slot]
  let flag_base = pools.flag_bases[slot]
  let validity = FixedArray::makei(rows, fn(row) {
    flag_base + row < pools.null_flags.length() &&
    pools.null_flags[flag_base + row] == 0
  })
  let type_id = if slot < pools.type_ids.length() {
    pools.type_ids[slot]
  } else {
    0
  }
  let logical_type = if type_id > 0 {
    column_type_from_id(type_id)
  } else {
    ColumnType::Unknown(type_id)
  }
  let kind = pools.kinds[slot]
  let offset = pools.offsets[slot]
  let meta = pools.metas[slot]
  let shape_at = if slot < pools.shape_offsets.length() {
    pools.shape_offsets[slot]
  } else {
    -1
  }
  let data : VectorData = match kind {
    JS_KIND_BOOL =>
      VectorData::Bool(
        FixedArray::makei(rows, fn(i) { pools.pool_i32[offset + i] == 1 }),
      )
    JS_KIND_I32 =>
      VectorData::Int32(
        FixedArray::makei(rows, fn(i) { pools.pool_i32[offset + i] }),
      )
    JS_KIND_I64 =>
      VectorData::Int64(
        FixedArray::makei(rows, fn(i) { pools.pool_i64[offset + i] }),
      )
    JS_KIND_U64 =>
      VectorData::UInt64(
        FixedArray::makei(rows, fn(i) {
          pools.pool_i64[offset + i].reinterpret_as_uint64()
        }),
      )
    JS_KIND_F32 =>
      VectorData::Float(
        FixedArray::makei(rows, fn(i) {
          Float::from_double(pools.pool_f64[offset + i])
        }),
      )
    JS_KIND_F64 =>
      VectorData::Double(
        FixedArray::makei(rows, fn(i) { pools.pool_f64[offset + i] }),
      )
    JS_KIND_STR =>
      VectorData::Varchar(
        Array::makei(rows, fn(i) { pools.pool_str[offset + i] }),
      )
    JS_KIND_BYTES =>
      VectorData::Blob(
        Array::makei(rows, fn(i) { pools.pool_bytes[offset + i] }),
      )
    JS_KIND_I128 =>
      VectorData::HugeInt(
        upper=FixedArray::makei(rows, fn(i) { pools.pool_i64[offset + i * 2] }),
        lower=FixedArray::makei(rows, fn(i) {
          pools.pool_i64[offset + i * 2 + 1].reinterpret_as_uint64()
        }),
      )
    JS_KIND_DECIMAL =>
      VectorData::Decimal(
        FixedArray::makei(rows, fn(i) {
          {
            width: meta >> 16,
            scale: meta & 0xffff,
            upper: pools.pool_i64[offset + i * 2],
            lower: pools.pool_i64[offset + i * 2 + 1].reinterpret_as_uint64(),
          }
        }),
      )
    JS_KIND_INTERVAL =>
      VectorData::Interval(
        FixedArray::makei(rows, fn(i) {
          {
            months: pools.pool_i64[offset + i * 3].to_int(),
            days: pools.pool_i64[offset + i * 3 + 1].to_int(),
            micros: pools.pool_i64[offset + i * 3 + 2],
          }
        }),
      )
    JS_KIND_LIST =>
      if shape_at >= 0 && shape_at + 1 < pools.shapes.length() {
        let child = js_vector_from_pools(pools, pools.shapes[shape_at + 1])
        VectorData::List(
          child~,
          offsets=FixedArray::makei(rows, fn(i) {
            pools.pool_i64[offset + i * 2].reinterpret_as_uint64()
          }),
          lengths=FixedArray::makei(rows, fn(i) {
            pools.pool_i64[offset + i * 2 + 1].reinterpret_as_uint64()
          }),
        )
      } else {
        VectorData::Any(Array::make(rows, Value::Null))
      }
    JS_KIND_STRUCT =>
      if shape_at >= 0 && shape_at + 1 < pools.shapes.length() {
        let nfields = pools.shapes[shape_at + 1]
        let name_base = if slot < pools.name_offsets.length() {
          pools.name_offsets[slot]
        } else {
          0
        }
        let fields : Array[String] = []
        let children : Array[Vector] = []
        for i in 0..<nfields {
          fields.push(
            if name_base + i < pools.names.length() {
              pools.names[name_base + i]
            } else {
              ""
            },
          )
          children.push(
            js_vector_from_pools(pools, pools.shapes[shape_at + 2 + i]),
          )
        }
        VectorData::Struct(fields~, children~)
      } else {
        VectorData::Any(Array::make(rows, Value::Null))
      }
    JS_KIND_MAP =>
      if shape_at >= 0 && shape_at + 2 < pools.shapes.length() {
        VectorData::Map(
          offsets=FixedArray::makei(rows, fn(i) {
            pools.pool_i64[offset + i * 2].reinterpret_as_uint64()
          }),
          lengths=FixedArray::makei(rows, fn(i) {
            pools.pool_i64[offset + i * 2 + 1].reinterpret_as_uint64()
          }),
          keys=js_vector_from_pools(pools, pools.shapes[shape_at + 1]),
          values=js_vector_from_pools(pools, pools.shapes[shape_at + 2]),
        )
      } else {
        VectorData::Any(Array::make(rows, Value::Null))
      }
    _ =>
      VectorData::Any(
        Array::makei(rows, fn(i) {
          value_from_any_json(pools.pool_str[offset + i])
        }),
      )
  }
  { logical_type, data, validity, }
}

///|
/// Slots of the real top-level columns, in column order. Nested children are
/// appended to the slot table in DFS order as their parent is emitted, so
/// top-level slots are not necessarily `0..<ncols`: a top-level slot is any
/// slot no `shapes` descriptor references as a child.
fn js_top_level_slots(pools : JsVectorPools) -> Array[Int] {
  let nslots = pools.kinds.length()
  let referenced = FixedArray::makei(nslots, fn(_) { false })
  for slot in 0..<nslots {
    let shape_at = if slot < pools.shape_offsets.length() {
      pools.shape_offsets[slot]
    } else {
      -1
    }
    if shape_at >= 0 && shape_at < pools.shapes.length() {
      let tag = pools.shapes[shape_at]
      if tag == JS_SHAPE_LIST && shape_at + 1 < pools.shapes.length() {
        referenced[pools.shapes[shape_at + 1]] = true
      } else if tag == JS_SHAPE_STRUCT && shape_at + 1 < pools.shapes.length() {
        let nfields = pools.shapes[shape_at + 1]
        for i in 0..<nfields {
          if shape_at + 2 + i < pools.shapes.length() {
            referenced[pools.shapes[shape_at + 2 + i]] = true
          }
        }
      } else if tag == JS_SHAPE_MAP && shape_at + 2 < pools.shapes.length() {
        referenced[pools.shapes[shape_at + 1]] = true
        referenced[pools.shapes[shape_at + 2]] = true
      }
    }
  }
  let out : Array[Int] = []
  for slot in 0..<nslots {
    if !referenced[slot] {
      out.push(slot)
    }
  }
  out
}

///|
/// Whether an emitted slot table marks end-of-stream for the caller. No
/// columns always means EOS; a 0-row schema-only emit means EOS only for
/// the public `next_chunk`/`next` path — matching native `next_chunk`,
/// which returns `None` whenever `row_count() <= 0`. Drains pass
/// `keep_schema=true` so empty results still materialize their column
/// names/types into `VectorChunk`s.
fn js_chunk_is_eos(
  columns : FixedArray[String],
  row_count : Int,
  keep_schema~ : Bool,
) -> Bool {
  columns.length() == 0 || (row_count <= 0 && !keep_schema)
}

///|
/// Fetch the next `VectorChunk` of a JS result stream, or `None` when the
/// stream is exhausted. Shared by `ResultStream::next_chunk` (which passes
/// `keep_schema=false`), `Connection::query_chunks`, and
/// `PreparedStatement::execute_chunks`.
fn js_stream_next_vector_chunk(
  stream : ResultStream,
  on_ok : (VectorChunk?) -> Unit,
  on_err : (String) -> Unit,
  keep_schema~ : Bool,
) -> Unit {
  js_stream_next_vectors_ffi(
    stream,
    fn(
      columns,
      type_ids,
      kinds,
      metas,
      offsets,
      slot_rows,
      null_flags,
      shapes,
      shape_offsets,
      names,
      name_offsets,
      row_count,
      pool_i32,
      pool_i64,
      pool_f64,
      pool_str,
      pool_bytes,
    ) {
      // A schema-only emit (0 rows but named columns) still materializes
      // a real chunk for drains so emptied queries keep their column
      // names/types; the public next_chunk path sees it as end-of-stream.
      if js_chunk_is_eos(columns, row_count, keep_schema~) {
        on_ok(None)
        return
      }
      let nslots = kinds.length()
      let flag_bases = FixedArray::makei(nslots, fn(_) { 0 })
      let mut base = 0
      for s in 0..<nslots {
        flag_bases[s] = base
        base += if s < slot_rows.length() { slot_rows[s] } else { 0 }
      }
      let pools : JsVectorPools = {
        type_ids,
        kinds,
        metas,
        offsets,
        slot_rows,
        flag_bases,
        null_flags,
        shapes,
        shape_offsets,
        names,
        name_offsets,
        pool_i32,
        pool_i64,
        pool_f64,
        pool_str,
        pool_bytes,
      }
      let vectors : Array[Vector] = []
      for slot in js_top_level_slots(pools) {
        vectors.push(js_vector_from_pools(pools, slot))
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
    keep_schema=true,
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
    // Close before reporting so a failed drain cannot leak the stream.
    fn(message) {
      js_stream_close(stream, fn() { on_done(Err(query_error(message))) }, fn(
        _,
      ) {
        on_done(Err(query_error(message)))
      })
    },
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
  js_stream_next_vector_chunk(
    self,
    keep_schema=false,
    fn(chunk) { on_done(Ok(chunk)) },
    fn(message) { on_done(Err(query_error(message))) },
  )
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
  match
    (try @json.parse(json) |> Ok catch {
      e => Err(e)
    }) {
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
        "tsns" =>
          match obj.get("nanos") {
            Some(Json::String(s)) => Value::TimestampNs(parse_int64_digits(s))
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

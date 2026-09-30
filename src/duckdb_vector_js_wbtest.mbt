///|
// Tests for the JS typed-vector chunk path (issue #61). The extern bodies
// are exercised in a child node process (async APIs) by extracting them
// from the source file — same pattern as `duckdb_js_wbtest.mbt`.

///|
/// Run `sql` via `query_stream` + `js_stream_next_vectors_ffi` in a child
/// node process and return the pooled chunk payloads as a JSON string.
fn run_js_stream_vectors(
  backend : JsBackend,
  sql : String,
) -> Result[String, String] {
  let js_backend = match backend {
    JsBackend::Auto => 0
    JsBackend::Node => 1
    JsBackend::Wasm => 2
  }
  Ok(js_run_stream_vectors(js_backend, sql))
}

///|
extern "js" fn js_run_stream_vectors(backend : Int, sql : String) -> String =
  #|(backend, sql) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const fs = require('fs');",
  #|    "const path = require('path');",
  #|    "const extract = (file, name) => {",
  #|    "  const source = fs.readFileSync(path.join(process.cwd(), file), 'utf8');",
  #|    "  const pattern = 'extern \"js\" fn ' + name + '[\\\\s\\\\S]*?=\\\\n((?:\\\\s*#\\\\|.*\\\\n)+)';",
  #|    "  const match = source.match(new RegExp(pattern));",
  #|    "  if (!match) throw new Error('missing js ffi: ' + name);",
  #|    "  return match[1]",
  #|    "    .split(/\\r?\\n/)",
  #|    "    .filter((line) => line.trim().startsWith('#|'))",
  #|    "    .map((line) => line.replace(/^\\s*#\\|\\s?/, ''))",
  #|    "    .join('\\n');",
  #|    "};",
  #|    "const js_connect = eval(extract('src/duckdb_js.mbt', 'js_connect'));",
  #|    "const js_query_stream = eval(extract('src/duckdb_js.mbt', 'js_query_stream'));",
  #|    "const js_stream_close = eval(extract('src/duckdb_js.mbt', 'js_stream_close'));",
  #|    "const js_close = eval(extract('src/duckdb_js.mbt', 'js_close'));",
  #|    "const js_stream_next_vectors_ffi = eval(extract('src/duckdb_vector_js.mbt', 'js_stream_next_vectors_ffi'));",
  #|    "const stringify = (v) => JSON.stringify(v, (k, x) => {",
  #|    "  if (typeof x === 'bigint') return x.toString();",
  #|    "  if (x instanceof Uint8Array) return Array.from(x);",
  #|    "  return x;",
  #|    "});",
  #|    "const nextVectors = (stream) => new Promise((resolve, reject) => {",
  #|    "  js_stream_next_vectors_ffi(stream,",
  #|    "    (columns, typeIds, kinds, metas, offsets, slotRows, flags, shapes, shapeOffsets, names, nameOffsets, rowCount, i32, i64, f64, str, bytes) => {",
  #|    "      resolve({ columns, typeIds, kinds, metas, offsets, slotRows, flags, shapes, shapeOffsets, names, nameOffsets, rowCount, i32, i64, f64: f64.map((x) => Number.isNaN(x) ? 'NaN' : x === Infinity ? 'Inf' : x === -Infinity ? '-Inf' : x), str, bytes: bytes.map((b) => Array.from(b)) });",
  #|    "    }, reject);",
  #|    "});",
  #|    "const run = async () => {",
  #|    "  const conn = await new Promise((res, rej) => js_connect(':memory:', backend, res, rej));",
  #|    "  const stream = await new Promise((res, rej) => js_query_stream(conn, sql, res, rej));",
  #|    "  const chunks = [];",
  #|    "  for (;;) {",
  #|    "    const chunk = await nextVectors(stream);",
  #|    "    if (chunk.columns.length === 0) break;",
  #|    "    chunks.push(chunk);",
  #|    "  }",
  #|    "  await new Promise((res, rej) => js_stream_close(stream, res, rej));",
  #|    "  await new Promise((res, rej) => js_close(conn, res, rej));",
  #|    "  return chunks;",
  #|    "};",
  #|    "run()",
  #|    "  .then((chunks) => console.log(stringify({ ok: true, chunks })))",
  #|    "  .catch((err) => console.log(stringify({ ok: false, error: String(err) })));",
  #|  ].join("\n");
  #|  try {
  #|    const out = execFileSync(process.execPath, ["-e", script], {
  #|      encoding: "utf8",
  #|      maxBuffer: 64 * 1024 * 1024,
  #|    });
  #|    return out;
  #|  } catch (err) {
  #|    return "ERR:" + (err && err.message ? err.message : String(err));
  #|  }
  #|}

///|
fn js_stream_vector_chunks(
  backend : JsBackend,
  sql : String,
) -> Result[Array[VectorChunk], String] {
  match run_js_stream_vectors(backend, sql) {
    Ok(json) =>
      if json.has_prefix("ERR:") {
        Err(json)
      } else {
        parse_chunks_json(json)
      }
    Err(err) => Err(err)
  }
}

///|
/// Parse the child process's JSON output back into `VectorChunk`s via the
/// real pooled decoder.
fn parse_chunks_json(json : String) -> Result[Array[VectorChunk], String] {
  match
    (try @json.parse(json) |> Ok catch {
      e => Err(e)
    }) {
    Err(_) => Err("child output not json: " + json)
    Ok(Json::Object(top)) =>
      match top.get("ok") {
        Some(Json::True) =>
          match top.get("chunks") {
            Some(Json::Array(chunks_json)) =>
              Ok(chunks_json.map(fn(chunk) { chunk_from_json(chunk) }))
            _ => Err("missing chunks")
          }
        Some(Json::False) =>
          match top.get("error") {
            Some(Json::String(message)) => Err(message)
            _ => Err("unknown child error")
          }
        _ => Err("missing ok")
      }
    _ => Err("unexpected child output")
  }
}

///|
fn json_int_array(json : Json?) -> Array[Int] {
  match json {
    Some(Json::Array(items)) => items.map(fn(v) { json_to_int(Some(v)) })
    _ => []
  }
}

///|
fn json_i64_array(json : Json?) -> FixedArray[Int64] {
  match json {
    Some(Json::Array(items)) =>
      FixedArray::makei(items.length(), fn(i) {
        match items[i] {
          Json::String(s) => parse_int64_digits(s)
          Json::Number(n, ..) => n.to_int64()
          _ => 0L
        }
      })
    _ => []
  }
}

///|
fn json_to_int(json : Json?) -> Int {
  match json {
    Some(Json::Number(n, ..)) => n.to_int()
    Some(Json::String(s)) => parse_int64_digits(s).to_int()
    _ => 0
  }
}

///|
fn json_str_array(json : Json?) -> Array[String] {
  match json {
    Some(Json::Array(items)) =>
      items.map(fn(v) {
        match v {
          Json::String(s) => s
          _ => ""
        }
      })
    _ => []
  }
}

///|
fn json_f64_array(json : Json?) -> FixedArray[Double] {
  match json {
    Some(Json::Array(items)) =>
      FixedArray::makei(items.length(), fn(i) {
        match items[i] {
          Json::Number(n, ..) => n
          // Tagged non-finite floats — JSON.stringify cannot carry them.
          Json::String("NaN") => @double.not_a_number
          Json::String("Inf") => @double.infinity
          Json::String("-Inf") => @double.neg_infinity
          Json::String(s) =>
            match
              (try @json.parse(s) |> Ok catch {
                e => Err(e)
              }) {
              Ok(Json::Number(n, ..)) => n
              _ => 0.0
            }
          _ => 0.0
        }
      })
    _ => []
  }
}

///|
fn json_bytes_array(json : Json?) -> FixedArray[Bytes] {
  match json {
    Some(Json::Array(items)) =>
      FixedArray::makei(items.length(), fn(i) {
        match items[i] {
          Json::Array(nums) => {
            let out : Array[Byte] = []
            for n in nums {
              out.push(json_to_int(Some(n)).to_byte())
            }
            Bytes::from_array(out)
          }
          _ => Default::default()
        }
      })
    _ => []
  }
}

///|
/// Rebuild one `VectorChunk` from a serialized pooled payload using the
/// production decoder `js_vector_from_pools`.
fn chunk_from_json(json : Json) -> VectorChunk {
  match json {
    Json::Object(obj) => {
      let columns = json_str_array(obj.get("columns"))
      let type_ids = FixedArray::from_array(json_int_array(obj.get("typeIds")))
      let kinds = FixedArray::from_array(json_int_array(obj.get("kinds")))
      let metas = FixedArray::from_array(json_int_array(obj.get("metas")))
      let offsets = FixedArray::from_array(json_int_array(obj.get("offsets")))
      let slot_rows = FixedArray::from_array(
        json_int_array(obj.get("slotRows")),
      )
      let null_flags = FixedArray::from_array(json_int_array(obj.get("flags")))
      let shapes = FixedArray::from_array(json_int_array(obj.get("shapes")))
      let shape_offsets = FixedArray::from_array(
        json_int_array(obj.get("shapeOffsets")),
      )
      let names = FixedArray::from_array(json_str_array(obj.get("names")))
      let name_offsets = FixedArray::from_array(
        json_int_array(obj.get("nameOffsets")),
      )
      let pool_i32 = FixedArray::from_array(json_int_array(obj.get("i32")))
      let pool_i64 = json_i64_array(obj.get("i64"))
      let pool_f64 = json_f64_array(obj.get("f64"))
      let pool_str = FixedArray::from_array(json_str_array(obj.get("str")))
      let pool_bytes = json_bytes_array(obj.get("bytes"))
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
      let ncols = columns.length()
      let vectors : Array[Vector] = []
      for slot in js_top_level_slots(pools) {
        vectors.push(js_vector_from_pools(pools, slot))
      }
      { columns, vectors, }
    }
    _ => { columns: [], vectors: [], }
  }
}

///|
test "js stream vectors: primitives stay typed" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT 42::INTEGER AS i, 7::BIGINT AS b, 1.5::DOUBLE AS d, true AS flag, 'hello' AS s",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      inspect(chunk.columns, content="[i, b, d, flag, s]")
      inspect(chunk.row_count(), content="1")
      match chunk.get_value(0, 0) {
        Some(Value::Int(42)) => ()
        _ => fail("expected Int(42)")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Int64(7L)) => ()
        _ => fail("expected Int64(7)")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Double(d)) => inspect(d, content="1.5")
        _ => fail("expected Double(1.5)")
      }
      match chunk.get_value(0, 3) {
        Some(Value::Bool(true)) => ()
        _ => fail("expected Bool(true)")
      }
      match chunk.get_value(0, 4) {
        Some(Value::String("hello")) => ()
        _ => fail("expected String(hello)")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: nulls via validity" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT * FROM (VALUES (1), (NULL), (3)) t(v) ORDER BY v IS NULL, v",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      inspect(chunk.row_count(), content="3")
      inspect(chunk.is_null(0, 0), content="false")
      inspect(chunk.is_null(1, 0), content="false")
      inspect(chunk.is_null(2, 0), content="true")
      match chunk.get_value(2, 0) {
        None => ()
        _ => fail("expected None for NULL cell")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: decimal and interval typed" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT 123.456::DECIMAL(10,3) AS price, INTERVAL '3 days' AS iv",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      match chunk.get_value(0, 0) {
        Some(Value::Decimal(dec)) => {
          inspect(dec.width, content="10")
          inspect(dec.scale, content="3")
          inspect(chunk.column(0).unwrap().string_at(0), content="123.456")
        }
        _ => fail("expected Decimal")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Interval(iv)) => inspect(iv.days, content="3")
        _ => fail("expected Interval")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: timestamps, date, time" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT DATE '2024-06-03' AS d, TIME '12:34:56.789' AS t, TIMESTAMP '2024-06-03 12:34:56.789' AS ts",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      inspect(chunk.column(0).unwrap().string_at(0), content="2024-06-03")
      inspect(chunk.column(1).unwrap().string_at(0), content="12:34:56.789")
      inspect(
        chunk.column(2).unwrap().string_at(0),
        content="2024-06-03 12:34:56.789",
      )
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: nested types decode without string round-trips" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT [1,2,3]::INTEGER[] AS xs, {'a': 1, 'b': 'x'} AS st, MAP {'x': 10, 'y': 20} AS m",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      // Real pooled variants, not the tagged-JSON Any fallback.
      match chunk.column(0).unwrap().data {
        VectorData::List(child~, offsets~, lengths~) => {
          inspect(Array::from_fixed_array(offsets), content="[0]")
          inspect(Array::from_fixed_array(lengths), content="[3]")
          inspect(child.data is VectorData::Int32(_), content="true")
        }
        _ => fail("expected List vector data")
      }
      match chunk.get_value(0, 0) {
        Some(Value::List(items)) =>
          inspect(items.map(fn(v) { v.to_string() }), content="[1, 2, 3]")
        _ => fail("expected List")
      }
      match chunk.column(1).unwrap().data {
        VectorData::Struct(fields~, children~) => {
          inspect(fields, content="[a, b]")
          inspect(children.length(), content="2")
          inspect(children[0].data is VectorData::Int32(_), content="true")
          inspect(children[1].data is VectorData::Varchar(_), content="true")
        }
        _ => fail("expected Struct vector data")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Struct(fields~, values~)) => {
          inspect(fields, content="[a, b]")
          inspect(values.map(fn(v) { v.to_string() }), content="[1, x]")
        }
        _ => fail("expected Struct")
      }
      match chunk.column(2).unwrap().data {
        VectorData::Map(offsets~, lengths~, keys~, values~) => {
          inspect(Array::from_fixed_array(offsets), content="[0]")
          inspect(Array::from_fixed_array(lengths), content="[2]")
          inspect(keys.data is VectorData::Varchar(_), content="true")
          inspect(values.data is VectorData::Int32(_), content="true")
        }
        _ => fail("expected Map vector data")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Map(keys~, values~)) => {
          inspect(keys.map(fn(v) { v.to_string() }), content="[x, y]")
          inspect(values.map(fn(v) { v.to_string() }), content="[10, 20]")
        }
        _ => fail("expected Map")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: nested lists across rows and null cells" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT * FROM (VALUES ([1,2]), ([3]), (NULL), ([4,NULL,6]), ([])) t(xs)",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      inspect(chunk.row_count(), content="5")
      match chunk.column(0).unwrap().data {
        VectorData::List(child~, offsets~, lengths~) => {
          inspect(Array::from_fixed_array(offsets), content="[0, 2, 3, 3, 6]")
          inspect(Array::from_fixed_array(lengths), content="[2, 1, 0, 3, 0]")
          inspect(child.len(), content="6")
          inspect(
            Array::from_fixed_array(child.validity),
            content="[true, true, true, true, false, true]",
          )
        }
        _ => fail("expected List vector data")
      }
      inspect(chunk.is_null(2, 0), content="true")
      match chunk.get_value(2, 0) {
        None => ()
        _ => fail("expected None for NULL list")
      }
      match chunk.get_value(3, 0) {
        Some(Value::List(items)) =>
          inspect(
            items.map(fn(v) { v.to_string() }),
            content=(
              #|[4, NULL, 6]
            ),
          )
        _ => fail("expected List with NULL element")
      }
      match chunk.get_value(4, 0) {
        Some(Value::List(items)) => inspect(items.length(), content="0")
        _ => fail("expected empty List")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: nested list across chunks" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT [i::INTEGER, i::INTEGER + 10] AS xs FROM RANGE(5000) t(i)",
    ) {
    Ok(chunks) => {
      inspect(chunks.length() > 1, content="true")
      inspect(
        chunks.fold(init=0, fn(acc, c) { acc + c.row_count() }),
        content="5000",
      )
      match chunks[0].get_value(0, 0) {
        Some(Value::List(items)) =>
          inspect(items.map(fn(v) { v.to_string() }), content="[0, 10]")
        _ => fail("expected first List")
      }
      let last = chunks[chunks.length() - 1]
      match last.get_value(last.row_count() - 1, 0) {
        Some(Value::List(items)) =>
          inspect(items.map(fn(v) { v.to_string() }), content="[4999, 5009]")
        _ => fail("expected last List")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: fixed-size array decodes as list with array type" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT * FROM (VALUES ([1,2,3]::INTEGER[3]), ([4,5,NULL]::INTEGER[3]), (NULL)) t(xs)",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      let vector = chunk.column(0).unwrap()
      inspect(vector.logical_type is ColumnType::Array, content="true")
      match vector.data {
        VectorData::List(child~, offsets~, lengths~) => {
          inspect(Array::from_fixed_array(offsets), content="[0, 3, 6]")
          inspect(Array::from_fixed_array(lengths), content="[3, 3, 0]")
          inspect(child.len(), content="6")
        }
        _ => fail("expected List vector data for ARRAY")
      }
      inspect(chunk.is_null(2, 0), content="true")
      match chunk.get_value(1, 0) {
        Some(Value::List(items)) =>
          inspect(
            items.map(fn(v) { v.to_string() }),
            content=(
              #|[4, 5, NULL]
            ),
          )
        _ => fail("expected List")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: nested-in-nested decodes recursively" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}] AS structs, {'list': [7, 8], 'map': MAP {'k': 9}} AS st",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      // LIST<STRUCT>
      match chunk.column(0).unwrap().data {
        VectorData::List(child~, ..) =>
          match child.data {
            VectorData::Struct(fields~, ..) => inspect(fields, content="[a, b]")
            _ => fail("expected Struct child vector")
          }
        _ => fail("expected List vector data")
      }
      match chunk.get_value(0, 0) {
        Some(Value::List(items)) =>
          inspect(
            items.map(fn(v) { v.to_string() }),
            content=(
              #|[{a: 1, b: x}, {a: 2, b: y}]
            ),
          )
        _ => fail("expected List of Struct")
      }
      // STRUCT with LIST and MAP fields: children must resolve to their
      // own slots — the parent descriptor is emitted after its children,
      // so interleaved emission decodes the wrong slot ids.
      match chunk.column(1).unwrap().data {
        VectorData::Struct(fields~, children~) => {
          inspect(fields, content="[list, map]")
          match children[0].data {
            VectorData::List(child~, offsets~, lengths~) => {
              inspect(Array::from_fixed_array(offsets), content="[0]")
              inspect(Array::from_fixed_array(lengths), content="[2]")
              inspect(child.len(), content="2")
            }
            _ => fail("expected List child vector")
          }
          match children[1].data {
            VectorData::Map(offsets~, lengths~, keys~, ..) => {
              inspect(Array::from_fixed_array(offsets), content="[0]")
              inspect(Array::from_fixed_array(lengths), content="[1]")
              inspect(keys.len(), content="1")
            }
            _ => fail("expected Map child vector")
          }
        }
        _ => fail("expected Struct vector data")
      }
      match chunk.get_value(0, 1) {
        Some(value) =>
          inspect(value.to_string(), content="{list: [7, 8], map: {k=9}}")
        _ => fail("expected Struct")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: struct-in-struct keeps field names aligned" {
  // Nested STRUCT field names interleave with the parent's names when
  // the parent's name segment is written before children are emitted —
  // fields would decode as ["inner", "x"] instead of ["inner", "b"].
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT {'inner': {'x': 1, 'y': 2}, 'b': 3} AS s",
    ) {
    Ok(chunks) => {
      let chunk = chunks[0]
      // Orphaned child slots must not surface as phantom top-level columns.
      inspect(chunk.column_count(), content="1")
      match chunk.column(0).unwrap().data {
        VectorData::Struct(fields~, children~) => {
          inspect(fields, content="[inner, b]")
          match children[0].data {
            VectorData::Struct(fields=inner_fields, ..) =>
              inspect(inner_fields, content="[x, y]")
            _ => fail("expected inner Struct child vector")
          }
        }
        _ => fail("expected Struct vector data")
      }
      match chunk.get_value(0, 0) {
        Some(value) =>
          inspect(value.to_string(), content="{inner: {x: 1, y: 2}, b: 3}")
        _ => fail("expected Struct")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: schema-only emits are EOS for public next_chunk" {
  // The public next_chunk/next path treats a 0-row emit as end-of-stream
  // (native parity: native next_chunk filters row_count() <= 0), while the
  // drain facade keeps it so query/execute still report the schema.
  let cols : FixedArray[String] = ["a", "b"]
  let empty : FixedArray[String] = []
  assert_true(js_chunk_is_eos(cols, 0, keep_schema=false))
  assert_true(!js_chunk_is_eos(cols, 0, keep_schema=true))
  assert_true(js_chunk_is_eos(empty, 0, keep_schema=false))
  assert_true(js_chunk_is_eos(empty, 0, keep_schema=true))
  assert_true(!js_chunk_is_eos(cols, 3, keep_schema=false))
  assert_true(!js_chunk_is_eos(cols, 3, keep_schema=true))
}

///|
test "js stream vectors: empty result keeps column schema" {
  // WHERE false: the drained chunks should still carry one schema-only
  // chunk so compat facades report real column names/types — and the same
  // emit carries rowCount 0, the marker that ends public streams.
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT 42 AS a, 'x'::VARCHAR AS b, {'k': [1]}::STRUCT(k INTEGER[]) AS s WHERE false",
    ) {
    Ok(chunks) => {
      inspect(chunks.length(), content="1")
      let chunk = chunks[0]
      inspect(chunk.row_count(), content="0")
      inspect(chunk.column_count(), content="3")
      let result = chunks_to_query_result(chunks)
      inspect(result.columns, content="[a, b, s]")
      inspect(result.column_types.length(), content="3")
      inspect(result.rows.length(), content="0")
      inspect(result.column_types[2] is ColumnType::Struct, content="true")
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

///|
test "js stream vectors: large numeric column stays pooled" {
  match
    js_stream_vector_chunks(
      JsBackend::Node,
      "SELECT i::BIGINT AS i FROM RANGE(5000) t(i)",
    ) {
    Ok(chunks) => {
      inspect(
        chunks.fold(init=0, fn(acc, c) { acc + c.row_count() }),
        content="5000",
      )
      let last = chunks[chunks.length() - 1]
      match last.get_value(last.row_count() - 1, 0) {
        Some(Value::Int64(4999L)) => ()
        _ => fail("expected Int64(4999)")
      }
      // Typed accessor: BIGINT column decoded as int64 pool.
      match chunks[0].column(0) {
        Some(vector) =>
          match vector.int64s() {
            Some(values) => inspect(values[0], content="0")
            None => fail("expected int64 pool")
          }
        None => fail("missing column")
      }
    }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node backend failed: \{message}")
      }
  }
}

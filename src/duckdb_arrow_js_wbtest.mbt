///|
// Tests for the JS Arrow-to-typed-vector path (issue #62). The extern
// bodies are exercised in a child node process (async APIs) by extracting
// them from the source file — same pattern as `duckdb_vector_js_wbtest.mbt`.

///|
/// Run `sql` via `js_query_arrow` + `js_arrow_to_stream` drained through
/// `js_stream_next_vectors_ffi` in a child node process, returning the
/// pooled chunk payloads plus the arrow metadata externs' outputs.
extern "js" fn js_run_arrow_vectors(backend : Int, sql : String) -> String =
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
  #|    "const js_close = eval(extract('src/duckdb_js.mbt', 'js_close'));",
  #|    "const js_stream_close = eval(extract('src/duckdb_js.mbt', 'js_stream_close'));",
  #|    "const js_stream_next_vectors_ffi = eval(extract('src/duckdb_vector_js.mbt', 'js_stream_next_vectors_ffi'));",
  #|    "const js_query_arrow = eval(extract('src/duckdb_arrow_js.mbt', 'js_query_arrow'));",
  #|    "const js_arrow_to_stream = eval(extract('src/duckdb_arrow_js.mbt', 'js_arrow_to_stream'));",
  #|    "const js_arrow_columns = eval(extract('src/duckdb_arrow_js.mbt', 'js_arrow_columns'));",
  #|    "const js_arrow_type_ids = eval(extract('src/duckdb_arrow_js.mbt', 'js_arrow_type_ids'));",
  #|    "const js_arrow_nullables = eval(extract('src/duckdb_arrow_js.mbt', 'js_arrow_nullables'));",
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
  #|    "  const handle = await new Promise((res, rej) => js_query_arrow(conn, sql, res, rej));",
  #|    "  const stream = js_arrow_to_stream(handle);",
  #|    "  const chunks = [];",
  #|    "  for (;;) {",
  #|    "    const chunk = await nextVectors(stream);",
  #|    "    if (chunk.rowCount <= 0 || chunk.columns.length === 0) break;",
  #|    "    chunks.push(chunk);",
  #|    "  }",
  #|    "  await new Promise((res, rej) => js_stream_close(stream, res, rej));",
  #|    "  const columns = js_arrow_columns(handle);",
  #|    "  const typeIds = js_arrow_type_ids(handle);",
  #|    "  const nullables = js_arrow_nullables(handle);",
  #|    "  await new Promise((res, rej) => js_close(conn, res, rej));",
  #|    "  return { chunks, columns, typeIds, nullables };",
  #|    "};",
  #|    "run()",
  #|    "  .then((out) => console.log(stringify({ ok: true, ...out })))",
  #|    "  .catch((err) => console.log(stringify({ ok: false, error: String(err && err.message ? err.message : err) })));",
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
/// The arrow result's chunks, schema columns, type ids, and nullability as
/// produced by the child process.
fn js_arrow_vector_chunks(
  backend : JsBackend,
  sql : String,
) -> Result[
  (Array[VectorChunk], Array[String], Array[Int], Array[Bool]),
  String,
] {
  let js_backend = match backend {
    JsBackend::Auto => 0
    JsBackend::Node => 1
    JsBackend::Wasm => 2
  }
  let payload = js_run_arrow_vectors(js_backend, sql)
  if payload.has_prefix("ERR:") {
    return Err(payload)
  }
  match (try? @json.parse(payload)) {
    Err(_) => Err("child output not json: " + payload)
    Ok(Json::Object(top)) =>
      match top.get("ok") {
        Some(Json::True) => {
          let chunks = match top.get("chunks") {
            Some(Json::Array(items)) => items.map(fn(c) { chunk_from_json(c) })
            _ => []
          }
          let columns = json_str_array(top.get("columns"))
          let type_ids = json_int_array(top.get("typeIds"))
          let nullables = match top.get("nullables") {
            Some(Json::Array(items)) =>
              items.map(fn(v) {
                match v {
                  Json::True => true
                  _ => false
                }
              })
            _ => []
          }
          Ok((chunks, columns, type_ids, nullables))
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
/// Skip cleanly when @duckdb/node-api is unavailable in this environment.
fn js_arrow_skip_if_missing(message : String) -> Unit raise {
  if message.contains("@duckdb/node-api") {
    ()
  } else {
    fail("node backend failed: \{message}")
  }
}

///|
test "js arrow vectors: primitives stay typed" {
  match
    js_arrow_vector_chunks(
      JsBackend::Node,
      "SELECT 42::INTEGER AS i, 7::BIGINT AS b, 1.5::DOUBLE AS d, true AS flag, 'hello' AS s",
    ) {
    Ok((chunks, columns, type_ids, nullables)) => {
      if columns != ["i", "b", "d", "flag", "s"] {
        fail("unexpected columns: \{columns}")
      }
      if type_ids.length() != 5 || nullables.length() != 5 {
        fail("expected 5 metadata entries")
      }
      let chunk = chunks[0]
      match chunk.get_value(0, 0) {
        Some(Value::Int(42)) => ()
        _ => fail("expected Int(42)")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Int64(7L)) => ()
        _ => fail("expected Int64(7)")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Double(d)) =>
          if d < 1.4 || d > 1.6 {
            fail("expected ~1.5, got \{d}")
          }
        _ => fail("expected Double")
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
    Err(message) => js_arrow_skip_if_missing(message)
  }
}

///|
test "js arrow vectors: bigint keeps precision (no JSON stringify)" {
  match
    js_arrow_vector_chunks(
      JsBackend::Node,
      "SELECT 4611686018427387904::BIGINT AS big, 18446744073709551615::UBIGINT AS max_u64",
    ) {
    Ok((chunks, _, _, _)) => {
      let chunk = chunks[0]
      match chunk.get_value(0, 0) {
        Some(Value::Int64(4611686018427387904L)) => ()
        _ => fail("int64 precision lost")
      }
      match chunk.get_value(0, 1) {
        Some(Value::UInt64(18446744073709551615UL)) => ()
        _ => fail("uint64 precision lost")
      }
    }
    Err(message) => js_arrow_skip_if_missing(message)
  }
}

///|
test "js arrow vectors: nulls via validity" {
  match
    js_arrow_vector_chunks(
      JsBackend::Node,
      "SELECT v FROM (VALUES (1), (NULL), (3)) t(v)",
    ) {
    Ok((chunks, _, _, _)) => {
      let vector = match chunks[0].column(0) {
        Some(v) => v
        None => fail("missing column 0")
      }
      if vector.len() != 3 {
        fail("expected 3 rows")
      } else if vector.is_null(0) || !vector.is_null(1) || vector.is_null(2) {
        fail("null mask wrong")
      }
      match chunk0_values(chunks) {
        [1, _, 3] => ()
        _ => fail("values wrong")
      }
    }
    Err(message) => js_arrow_skip_if_missing(message)
  }
}

///|
fn chunk0_values(chunks : Array[VectorChunk]) -> Array[Int] {
  let out : Array[Int] = []
  for chunk in chunks {
    match chunk.column(0) {
      Some(vector) =>
        match vector.ints() {
          Some(values) =>
            for v in values {
              out.push(v)
            } nobreak {
              ()
            }
          None => ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  out
}

///|
test "js arrow vectors: decimal column typed" {
  match
    js_arrow_vector_chunks(
      JsBackend::Node,
      "SELECT 12.34::DECIMAL(10, 2) AS amount",
    ) {
    Ok((chunks, _, type_ids, _)) => {
      if type_ids.length() != 1 || type_ids[0] != 19 {
        fail("expected decimal type id 19, got \{type_ids}")
      }
      match chunks[0].column(0) {
        Some(vector) =>
          if vector.string_at(0) != "12.34" {
            fail("expected '12.34', got '\{vector.string_at(0)}'")
          }
        None => fail("missing column 0")
      }
    }
    Err(message) => js_arrow_skip_if_missing(message)
  }
}

///|
test "js arrow vectors: list column decodes nested" {
  match
    js_arrow_vector_chunks(JsBackend::Node, "SELECT [1, 2, 3]::INTEGER[] AS xs") {
    Ok((chunks, _, _, _)) =>
      match chunks[0].column(0) {
        Some(vector) =>
          match vector.value_at(0) {
            Value::List(items) =>
              if items.length() != 3 {
                fail("expected 3 items")
              }
            _ => fail("expected Value::List")
          }
        None => fail("missing column 0")
      }
    Err(message) => js_arrow_skip_if_missing(message)
  }
}

///|
test "js arrow vectors: query error surfaces" {
  match
    js_arrow_vector_chunks(JsBackend::Node, "SELECT * FROM nonexistent_table") {
    Ok(_) => fail("query should have failed")
    Err(_) => () // Any error (missing module or query error) is acceptable here
  }
}

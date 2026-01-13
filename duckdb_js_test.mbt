///|
// Use a child process so async JS APIs can be exercised in sync tests.
extern "js" fn js_run_query(backend : JsBackend, sql : String) -> String =
  #|(backend, sql) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "const toCell = (value) => {",
  #|    "  if (value === null || value === undefined) { return ['', true]; }",
  #|    "  if (typeof value === 'string') { return [value, false]; }",
  #|    "  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  return [JSON.stringify(value), false];",
  #|    "};",
  #|    "const pushRow = (values, rows, nulls) => {",
  #|    "  const row = [];",
  #|    "  const rowNulls = [];",
  #|    "  for (const value of values) {",
  #|    "    const cell = toCell(value);",
  #|    "    row.push(cell[0]);",
  #|    "    rowNulls.push(cell[1]);",
  #|    "  }",
  #|    "  rows.push(row);",
  #|    "  nulls.push(rowNulls);",
  #|    "};",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    const result = await connection.run(sql);",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      const values = Array.isArray(row) ? row : columns.map((name) => row[name]);",
  #|    "      pushRow(values, rows, nulls);",
  #|    "    }",
  #|    "    if (connection && typeof connection.close === 'function') {",
  #|    "      await connection.close();",
  #|    "    }",
  #|    "    if (connection && typeof connection.closeSync === 'function') {",
  #|    "      connection.closeSync();",
  #|    "    }",
  #|    "    if (instance && typeof instance.close === 'function') {",
  #|    "      await instance.close();",
  #|    "    }",
  #|    "    return { columns, rows, nulls };",
  #|    "  }",
  #|    "  if (backend === 2) {",
  #|    "    if (typeof Worker === 'undefined') {",
  #|    "      throw new Error('duckdb-wasm requires Worker support');",
  #|    "    }",
  #|    "    const duckdb = await import('@duckdb/duckdb-wasm');",
  #|    "    const bundles = duckdb.getJsDelivrBundles();",
  #|    "    const bundle = await duckdb.selectBundle(bundles);",
  #|    "    const logger = new duckdb.ConsoleLogger();",
  #|    "    const worker = new Worker(bundle.mainWorker);",
  #|    "    const db = new duckdb.AsyncDuckDB(logger, worker);",
  #|    "    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);",
  #|    "    const conn = await db.connect();",
  #|    "    const arrowResult = await conn.query(sql);",
  #|    "    let columns = [];",
  #|    "    if (arrowResult && arrowResult.schema && arrowResult.schema.fields) {",
  #|    "      columns = arrowResult.schema.fields.map((field) => field.name);",
  #|    "    }",
  #|    "    const rowObjects = arrowResult.toArray().map((row) => row.toJSON());",
  #|    "    if (columns.length === 0 && rowObjects.length > 0) {",
  #|    "      columns = Object.keys(rowObjects[0]);",
  #|    "    }",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowObjects) {",
  #|    "      const values = columns.map((name) => row[name]);",
  #|    "      pushRow(values, rows, nulls);",
  #|    "    }",
  #|    "    if (conn && typeof conn.close === 'function') {",
  #|    "      await conn.close();",
  #|    "    }",
  #|    "    if (db && typeof db.terminate === 'function') {",
  #|    "      await db.terminate();",
  #|    "    }",
  #|    "    if (worker && typeof worker.terminate === 'function') {",
  #|    "      worker.terminate();",
  #|    "    }",
  #|    "    return { columns, rows, nulls };",
  #|    "  }",
  #|    "  throw new Error('unknown backend');",
  #|    "};",
  #|    "run()",
  #|    "  .then((result) => {",
  #|    "    console.log(JSON.stringify({ ok: true, columns: result.columns, rows: result.rows, nulls: result.nulls }));",
  #|    "  })",
  #|    "  .catch((err) => {",
  #|    "    console.log(JSON.stringify({ ok: false, error: toError(err) }));",
  #|    "  });"
  #|  ].join("\\n");
  #|  try {
  #|    const output = execFileSync(process.execPath, ["-e", script], {
  #|      encoding: "utf8",
  #|      stdio: ["ignore", "pipe", "pipe"],
  #|    });
  #|    const lines = output.split(/\r?\n/).filter((line) => line.trim().length > 0);
  #|    return lines.length ? lines[lines.length - 1] : "";
  #|  } catch (err) {
  #|    const message = err && err.stderr
  #|      ? err.stderr.toString()
  #|      : (err && err.message ? err.message : String(err));
  #|    return JSON.stringify({ ok: false, error: message });
  #|  }
  #|}

///|
fn decode_string_array(json : Json) -> Result[Array[String], String] {
  match json {
    Json::Array(items) => {
      let out : Array[String] = []
      for item in items {
        match item {
          Json::String(value) => out.push(value)
          _ => return Err("expected string array")
        }
      } else {
        ()
      }
      Ok(out)
    }
    _ => Err("expected string array")
  }
}

///|
fn decode_bool_array(json : Json) -> Result[Array[Bool], String] {
  match json {
    Json::Array(items) => {
      let out : Array[Bool] = []
      for item in items {
        match item {
          Json::True => out.push(true)
          Json::False => out.push(false)
          _ => return Err("expected bool array")
        }
      } else {
        ()
      }
      Ok(out)
    }
    _ => Err("expected bool array")
  }
}

///|
fn decode_string_matrix(json : Json) -> Result[Array[Array[String]], String] {
  match json {
    Json::Array(items) => {
      let out : Array[Array[String]] = []
      for item in items {
        match decode_string_array(item) {
          Ok(row) => out.push(row)
          Err(message) => return Err(message)
        }
      } else {
        ()
      }
      Ok(out)
    }
    _ => Err("expected string matrix")
  }
}

///|
fn decode_bool_matrix(json : Json) -> Result[Array[Array[Bool]], String] {
  match json {
    Json::Array(items) => {
      let out : Array[Array[Bool]] = []
      for item in items {
        match decode_bool_array(item) {
          Ok(row) => out.push(row)
          Err(message) => return Err(message)
        }
      } else {
        ()
      }
      Ok(out)
    }
    _ => Err("expected bool matrix")
  }
}

///|
fn decode_query_result(
  payload : String,
) -> Result[(Array[String], Array[Array[String]], Array[Array[Bool]]), String] {
  let json = @json.parse(payload) catch {
    err => return Err("parse failed: \{err.to_string()}")
  }
  match json {
    Json::Object(obj) =>
      match obj["ok"] {
        Json::True => {
          let columns = match decode_string_array(obj["columns"]) {
            Ok(values) => values
            Err(message) => return Err(message)
          }
          let rows = match decode_string_matrix(obj["rows"]) {
            Ok(values) => values
            Err(message) => return Err(message)
          }
          let nulls = match decode_bool_matrix(obj["nulls"]) {
            Ok(values) => values
            Err(message) => return Err(message)
          }
          Ok((columns, rows, nulls))
        }
        Json::False =>
          match obj["error"] {
            Json::String(message) => Err(message)
            _ => Err("expected error string")
          }
        _ => Err("unexpected ok field")
      }
    _ => Err("unexpected json payload")
  }
}

///|
fn run_js_query(
  backend : JsBackend,
  sql : String,
) -> Result[(Array[String], Array[Array[String]], Array[Array[Bool]]), String] {
  let payload = js_run_query(backend, sql)
  if payload is "" {
    Err("no output from js runner")
  } else {
    decode_query_result(payload)
  }
}

///|
fn cell_from_data(
  rows : Array[Array[String]],
  nulls : Array[Array[Bool]],
  row : Int,
  col : Int,
) -> String? {
  if nulls[row][col] {
    None
  } else {
    Some(rows[row][col])
  }
}

///|
test "js node query" {
  let sql = "select 1 as a, NULL as b, 'duck' as c"
  let result = run_js_query(JsBackend::Node, sql)
  match result {
    Ok((columns, rows, nulls)) => {
      inspect(columns, content="[\"a\", \"b\", \"c\"]")
      inspect(rows, content="[[\"1\", \"\", \"duck\"]]")
      inspect(nulls, content="[[false, true, false]]")
      inspect(cell_from_data(rows, nulls, 0, 1), content="None")
    }
    Err(message) => {
      guard message.contains("@duckdb/node-api") else {
        fail("node backend failed: \{message}")
      }
    }
  }
}

///|
test "js wasm query" {
  let sql = "select 1 as a, NULL as b, 'duck' as c"
  let result = run_js_query(JsBackend::Wasm, sql)
  match result {
    Ok((columns, rows, nulls)) => {
      inspect(columns, content="[\"a\", \"b\", \"c\"]")
      inspect(rows, content="[[\"1\", \"\", \"duck\"]]")
      inspect(nulls, content="[[false, true, false]]")
      inspect(cell_from_data(rows, nulls, 0, 1), content="None")
    }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm backend failed: \{message}")
      }
  }
}

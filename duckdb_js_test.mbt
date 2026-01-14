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
test "js node fixtures" {
  let missing_ref : Ref[Bool] = Ref::new(false)
  for case in fixture_cases {
    if missing_ref.val {
      ()
    } else {
      let result = run_js_query(JsBackend::Node, case.sql)
      match result {
        Ok((columns, rows, nulls)) =>
          expect_fixture_case(case, columns, rows, nulls)
        Err(message) =>
          if message.contains("@duckdb/node-api") {
            missing_ref.val = true
          } else {
            fail("node backend failed for '\{case.name}': \{message}")
          }
      }
    }
  } else {
    ()
  }
}

///|
test "js wasm fixture" {
  guard fixture_cases.length() > 0 else { fail("no fixtures defined") }
  let case = fixture_cases[0]
  let result = run_js_query(JsBackend::Wasm, case.sql)
  match result {
    Ok((columns, rows, nulls)) =>
      expect_fixture_case(case, columns, rows, nulls)
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm backend failed: \{message}")
      }
  }
}

// ============================================================================
// Prepared Statement Tests
// ============================================================================

///|
extern "js" fn js_run_prepare_query(
  backend : JsBackend,
  sql : String,
  binds : Array[String],
  bind_types : Array[String],
) -> String =
  #|(backend, sql, binds, bind_types) => {
  #|  const { execFileSync } = require("child_process");
  #|  const bindScript = binds.map((v, i) => {
  #|    const t = bind_types[i];
  #|    if (t === "null") return `stmt.bindNull(${i + 1});`;
  #|    if (t === "int") return `stmt.bindInteger(${i + 1}, ${v});`;
  #|    if (t === "bigint") return `stmt.bindBigInt(${i + 1}, BigInt(${v}));`;
  #|    if (t === "double") return `stmt.bindDouble(${i + 1}, ${v});`;
  #|    if (t === "varchar") return `stmt.bindVarchar(${i + 1}, ${JSON.stringify(v)});`;
  #|    if (t === "bool") return `stmt.bindBoolean(${i + 1}, ${v});`;
  #|    return `stmt.bindNull(${i + 1});`;
  #|  }).join("\\n");
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
  #|    "    const stmt = connection.createStatement(sql);",
  #|    bindScript,
  #|    "    const result = await stmt.run();",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      pushRow(Array.isArray(row) ? row : columns.map(n => row[n]), rows, nulls);",
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
  #|    "    // For WASM, use real prepared statements",
  #|    "    const stmt = await conn.prepare(sql);",
  #|    "    const params = [];",
  #|    "    const binds = " + JSON.stringify(binds) + ";",
  #|    "    const bindTypes = " + JSON.stringify(bind_types) + ";",
  #|    "    for (let i = 0; i < binds.length; i++) {",
  #|    "      const t = bindTypes[i];",
  #|    "      const v = binds[i];",
  #|    "      if (t === 'null') {",
  #|    "        params.push(null);",
  #|    "      } else if (t === 'bigint') {",
  #|    "        params.push(BigInt(v));",
  #|    "      } else if (t === 'varchar') {",
  #|    "        params.push(v);",
  #|    "      } else if (t === 'int' || t === 'double' || t === 'bool') {",
  #|    "        params.push(eval(v));",
  #|    "      } else {",
  #|    "        params.push(v);",
  #|    "      }",
  #|    "    }",
  #|    "    const arrowResult = await stmt.query(...params);",
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
fn run_js_prepare_query(
  backend : JsBackend,
  sql : String,
  binds : Array[(String, String)],
) -> Result[(Array[String], Array[Array[String]], Array[Array[Bool]]), String] {
  let bind_values : Array[String] = []
  let bind_types : Array[String] = []
  for bind in binds {
    match bind {
      (value, typ) => {
        bind_values.push(value)
        bind_types.push(typ)
      }
    }
  } else {
    ()
  }
  let payload = js_run_prepare_query(backend, sql, bind_values, bind_types)
  if payload is "" {
    Err("no output from js prepare runner")
  } else {
    decode_query_result(payload)
  }
}

///|
test "js node prepare bind int" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? * 2 AS x", [("21", "int")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "42" {
        fail("expected '42', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node prepare bind int failed: \{message}")
      }
  }
}

///|
test "js node prepare bind varchar" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT CONCAT(?, 'suffix') AS x", [("prefix", "varchar")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "prefixsuffix" {
        fail("expected 'prefixsuffix', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node prepare bind varchar failed: \{message}")
      }
  }
}

///|
test "js node prepare bind null" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? IS NULL AS x", [("", "null")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value for IS NULL check")
      } else if rows[0][0] != "true" {
        fail("expected 'true', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node prepare bind null failed: \{message}")
      }
  }
}

///|
test "js node prepare multiple params" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? + ? + ? AS x", [("10", "int"), ("20", "int"), ("30", "int")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "60" {
        fail("expected '60', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node prepare multiple params failed: \{message}")
      }
  }
}

///|
test "js wasm prepare bind int" {
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? * 2 AS x", [("21", "int")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "42" {
        fail("expected '42', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm prepare bind int failed: \{message}")
      }
  }
}

///|
test "js wasm prepare bind varchar" {
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT CONCAT(?, 'suffix') AS x", [("prefix", "varchar")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "prefixsuffix" {
        fail("expected 'prefixsuffix', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm prepare bind varchar failed: \{message}")
      }
  }
}

// ============================================================================
// Error Propagation Tests
// ============================================================================

///|
test "js node prepare bind chain with ? operator" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? + ? AS x", [("10", "int"), ("20", "int")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "30" {
        fail("expected '30', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node bind chain failed: \{message}")
      }
  }
}

///|
test "js wasm prepare multiple params" {
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? + ? + ? AS x", [("10", "int"), ("20", "int"), ("30", "int")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "60" {
        fail("expected '60', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm prepare multiple params failed: \{message}")
      }
  }
}

///|
test "js wasm prepare with quotes" {
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? AS x", [("it's a test", "varchar")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if nulls[0][0] {
        fail("expected non-null value")
      } else if rows[0][0] != "it's a test" {
        fail("expected \"it's a test\", got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm prepare with quotes failed: \{message}")
      }
  }
}

///|
test "js wasm prepare sql injection safe" {
  let malicious = "'; DROP TABLE users; --"
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? AS x", [(malicious, "varchar")])
  match result {
    Ok((columns, rows, nulls)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if rows[0][0] != malicious {
        fail("SQL injection vulnerability detected! expected '\{malicious}', got '\{rows[0][0]}'")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm prepare sql injection test failed: \{message}")
      }
  }
}

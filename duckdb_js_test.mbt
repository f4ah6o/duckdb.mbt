///|
// Use a child process so async JS APIs can be exercised in sync tests.
extern "js" fn js_run_query(backend : JsBackend, sql : String) -> String =
  #|(backend, sql) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "let floatTypeId = null;",
  #|    "let doubleTypeId = null;",
  #|    "const toCell = (value, typeId) => {",
  #|    "  if (value === null || value === undefined) { return ['', true]; }",
  #|    "  if (typeof value === 'string') {",
  #|    "    if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {",
  #|    "      if (value === 'NaN') { return ['nan', false]; }",
  #|    "      if (value === 'Infinity') { return ['inf', false]; }",
  #|    "      if (value === '-Infinity') { return ['-inf', false]; }",
  #|    "    }",
  #|    "    return [value, false];",
  #|    "  }",
  #|    "  if (typeof value === 'number') {",
  #|    "    if (Number.isNaN(value)) { return ['nan', false]; }",
  #|    "    if (value === Infinity) { return ['inf', false]; }",
  #|    "    if (value === -Infinity) { return ['-inf', false]; }",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  if (typeof value === 'boolean' || typeof value === 'bigint') {",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  return [JSON.stringify(value), false];",
  #|    "};",
  #|    "const pushRow = (values, rows, nulls, typeIds) => {",
  #|    "  const row = [];",
  #|    "  const rowNulls = [];",
  #|    "  for (let i = 0; i < values.length; i++) {",
  #|    "    const cell = toCell(values[i], typeIds ? typeIds[i] : null);",
  #|    "    row.push(cell[0]);",
  #|    "    rowNulls.push(cell[1]);",
  #|    "  }",
  #|    "  rows.push(row);",
  #|    "  nulls.push(rowNulls);",
  #|    "};",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    floatTypeId = api.DuckDBTypeId.FLOAT;",
  #|    "    doubleTypeId = api.DuckDBTypeId.DOUBLE;",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    const result = await connection.run(sql);",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      const values = Array.isArray(row) ? row : columns.map((name) => row[name]);",
  #|    "      pushRow(values, rows, nulls, typeIds);",
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
  #|    "      pushRow(values, rows, nulls, null);",
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
  #|  ].join("\n");
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
extern "js" fn js_run_stream_count(backend : JsBackend, sql : String) -> String =
  #|(backend, sql) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    const result = await connection.stream(sql);",
  #|    "    const columns = result.columnNames();",
  #|    "    let count = 0;",
  #|    "    while (true) {",
  #|    "      const chunk = await result.fetchChunk();",
  #|    "      if (!chunk || chunk.rowCount <= 0) break;",
  #|    "      count += chunk.rowCount;",
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
  #|    "    return { columns, count };",
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
  #|    "    const reader = await conn.send(sql, true);",
  #|    "    let columns = [];",
  #|    "    if (reader && reader.schema && reader.schema.fields) {",
  #|    "      columns = reader.schema.fields.map((field) => field.name);",
  #|    "    }",
  #|    "    let count = 0;",
  #|    "    for await (const batch of reader) {",
  #|    "      if (batch && batch.numRows !== undefined) {",
  #|    "        count += batch.numRows;",
  #|    "      }",
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
  #|    "    return { columns, count };",
  #|    "  }",
  #|    "  throw new Error('unknown backend');",
  #|    "};",
  #|    "run()",
  #|    "  .then((result) => {",
  #|    "    console.log(JSON.stringify({ ok: true, columns: result.columns, count: result.count }));",
  #|    "  })",
  #|    "  .catch((err) => {",
  #|    "    console.log(JSON.stringify({ ok: false, error: toError(err) }));",
  #|    "  });"
  #|  ].join(\"\\n\");
  #|  try {
  #|    const output = execFileSync(process.execPath, [\"-e\", script], {
  #|      encoding: \"utf8\",
  #|      stdio: [\"ignore\", \"pipe\", \"pipe\"],
  #|    });
  #|    const lines = output.split(/\\r?\\n/).filter((line) => line.trim().length > 0);
  #|    return lines.length ? lines[lines.length - 1] : \"\";
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
fn decode_int(json : Json) -> Result[Int, String] {
  match json {
    Json::Number(n, ..) => Ok(parse_int(n.to_string()))
    Json::String(s) => Ok(parse_int(s))
    _ => Err("expected int")
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
fn decode_stream_result(
  payload : String,
) -> Result[(Array[String], Int), String] {
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
          let count = match decode_int(obj["count"]) {
            Ok(value) => value
            Err(message) => return Err(message)
          }
          Ok((columns, count))
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
fn run_js_stream_count(
  backend : JsBackend,
  sql : String,
) -> Result[(Array[String], Int), String] {
  let payload = js_run_stream_count(backend, sql)
  if payload is "" {
    Err("no output from js runner")
  } else {
    decode_stream_result(payload)
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

///|
test "js node stream count" {
  let result = run_js_stream_count(
    JsBackend::Node,
    "SELECT i FROM RANGE(10000) tbl(i)",
  )
  match result {
    Ok((columns, count)) =>
      if columns.length() != 1 || columns[0] != "i" {
        fail("unexpected columns: \{columns}")
      } else if count != 10000 {
        fail("expected 10000 rows, got \{count}")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node stream failed: \{message}")
      }
  }
}

///|
test "js wasm stream count" {
  let result = run_js_stream_count(
    JsBackend::Wasm,
    "SELECT i FROM RANGE(1000) tbl(i)",
  )
  match result {
    Ok((columns, count)) =>
      if columns.length() != 1 || columns[0] != "i" {
        fail("unexpected columns: \{columns}")
      } else if count != 1000 {
        fail("expected 1000 rows, got \{count}")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/duckdb-wasm") ||
        message.contains("duckdb-wasm requires Worker support") {
        ()
      } else {
        fail("wasm stream failed: \{message}")
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
  #|  }).join("\n");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "let floatTypeId = null;",
  #|    "let doubleTypeId = null;",
  #|    "const toCell = (value, typeId) => {",
  #|    "  if (value === null || value === undefined) { return ['', true]; }",
  #|    "  if (typeof value === 'string') {",
  #|    "    if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {",
  #|    "      if (value === 'NaN') { return ['nan', false]; }",
  #|    "      if (value === 'Infinity') { return ['inf', false]; }",
  #|    "      if (value === '-Infinity') { return ['-inf', false]; }",
  #|    "    }",
  #|    "    return [value, false];",
  #|    "  }",
  #|    "  if (typeof value === 'number') {",
  #|    "    if (Number.isNaN(value)) { return ['nan', false]; }",
  #|    "    if (value === Infinity) { return ['inf', false]; }",
  #|    "    if (value === -Infinity) { return ['-inf', false]; }",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  if (typeof value === 'boolean' || typeof value === 'bigint') {",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  return [JSON.stringify(value), false];",
  #|    "};",
  #|    "const pushRow = (values, rows, nulls, typeIds) => {",
  #|    "  const row = [];",
  #|    "  const rowNulls = [];",
  #|    "  for (let i = 0; i < values.length; i++) {",
  #|    "    const cell = toCell(values[i], typeIds ? typeIds[i] : null);",
  #|    "    row.push(cell[0]);",
  #|    "    rowNulls.push(cell[1]);",
  #|    "  }",
  #|    "  rows.push(row);",
  #|    "  nulls.push(rowNulls);",
  #|    "};",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    floatTypeId = api.DuckDBTypeId.FLOAT;",
  #|    "    doubleTypeId = api.DuckDBTypeId.DOUBLE;",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    const stmt = await connection.prepare(sql);",
  #|    bindScript,
  #|    "    const result = await stmt.run();",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      pushRow(Array.isArray(row) ? row : columns.map(n => row[n]), rows, nulls, typeIds);",
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
  #|    "      pushRow(values, rows, nulls, null);",
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
  #|  ].join("\n");
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
extern "js" fn js_run_wrapper_prepare_query(
  backend : JsBackend,
  sql : String,
  binds : Array[String],
  bind_types : Array[String],
) -> String =
  #|(backend, sql, binds, bind_types) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const binds = " + JSON.stringify(binds) + ";",
  #|    "const bindTypes = " + JSON.stringify(bind_types) + ";",
  #|    "const fs = require('fs');",
  #|    "const path = require('path');",
  #|    "const source = fs.readFileSync(path.join(process.cwd(), 'duckdb_js.mbt'), 'utf8');",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "const extract = (name) => {",
  #|    "  const pattern = 'extern \"js\" fn ' + name + '[\\\\s\\\\S]*?=\\\\n((?:\\\\s*#\\\\|.*\\\\n)+)';",
  #|    "  const match = source.match(new RegExp(pattern));",
  #|    "  if (!match) throw new Error('missing js ffi: ' + name);",
  #|    "  return match[1]",
  #|    "    .split(/\\r?\\n/)",
  #|    "    .filter((line) => line.trim().startsWith('#|'))",
  #|    "    .map((line) => line.replace(/^\\s*#\\|\\s?/, ''))",
  #|    "    .join('\\n');",
  #|    "};",
  #|    "const js_connect = eval(extract('js_connect'));",
  #|    "const js_prepare = eval(extract('js_prepare'));",
  #|    "const js_execute_prepared = eval(extract('js_execute_prepared'));",
  #|    "const js_close_prepared = eval(extract('js_close_prepared'));",
  #|    "const js_close = eval(extract('js_close'));",
  #|    "const js_bind_int = eval(extract('js_bind_int'));",
  #|    "const js_bind_varchar = eval(extract('js_bind_varchar'));",
  #|    "const js_bind_null = eval(extract('js_bind_null'));",
  #|    "const run = async () => {",
  #|    "  const connect = () => new Promise((resolve, reject) => js_connect(':memory:', backend, resolve, reject));",
  #|    "  const prepare = (conn) => new Promise((resolve, reject) => js_prepare(conn, sql, resolve, reject));",
  #|    "  const execute = (stmt) => new Promise((resolve, reject) => {",
  #|    "    js_execute_prepared(stmt, (columns, rows, nulls) => resolve({ columns, rows, nulls }), reject);",
  #|    "  });",
  #|    "  const closeStmt = (stmt) => new Promise((resolve, reject) => js_close_prepared(stmt, resolve, reject));",
  #|    "  const closeConn = (conn) => new Promise((resolve, reject) => js_close(conn, resolve, reject));",
  #|    "  const conn = await connect();",
  #|    "  const stmt = await prepare(conn);",
  #|    "  for (let i = 0; i < binds.length; i++) {",
  #|    "    const idx = i + 1;",
  #|    "    const value = binds[i];",
  #|    "    const type = bindTypes[i];",
  #|    "    let result;",
  #|    "    if (type === 'int') result = js_bind_int(stmt, idx, Number(value));",
  #|    "    else if (type === 'varchar') result = js_bind_varchar(stmt, idx, value);",
  #|    "    else if (type === 'null') result = js_bind_null(stmt, idx);",
  #|    "    else throw new Error('unsupported bind type: ' + type);",
  #|    "    if (!result || !result.ok) {",
  #|    "      throw new Error(result && result.error ? result.error : 'bind failed');",
  #|    "    }",
  #|    "  }",
  #|    "  const result = await execute(stmt);",
  #|    "  await closeStmt(stmt);",
  #|    "  await closeConn(conn);",
  #|    "  return result;",
  #|    "};",
  #|    "run()",
  #|    "  .then((result) => {",
  #|    "    console.log(JSON.stringify({ ok: true, columns: result.columns, rows: result.rows, nulls: result.nulls }));",
  #|    "  })",
  #|    "  .catch((err) => {",
  #|    "    console.log(JSON.stringify({ ok: false, error: toError(err) }));",
  #|    "  });"
  #|  ].join("\n");
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
fn run_js_wrapper_prepare_query(
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
  let payload = js_run_wrapper_prepare_query(
    backend, sql, bind_values, bind_types,
  )
  if payload is "" {
    Err("no output from js wrapper prepare runner")
  } else {
    decode_query_result(payload)
  }
}

///|
test "js node prepare bind int" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? * 2 AS x", [
    ("21", "int"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(
    JsBackend::Node,
    "SELECT CONCAT(?, 'suffix') AS x",
    [("prefix", "varchar")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? IS NULL AS x", [
    ("", "null"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? + ? + ? AS x", [
    ("10", "int"),
    ("20", "int"),
    ("30", "int"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? * 2 AS x", [
    ("21", "int"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(
    JsBackend::Wasm,
    "SELECT CONCAT(?, 'suffix') AS x",
    [("prefix", "varchar")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
// MoonBit Wrapper Prepared Statement Tests
// ============================================================================

///|
test "js wrapper node prepare bind int" {
  let result = run_js_wrapper_prepare_query(
    JsBackend::Node,
    "SELECT ? * 2 AS x",
    [("21", "int")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
        fail("node wrapper bind int failed: \{message}")
      }
  }
}

///|
test "js wrapper node prepare bind varchar" {
  let result = run_js_wrapper_prepare_query(
    JsBackend::Node,
    "SELECT CONCAT(?, 'suffix') AS x",
    [("prefix", "varchar")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
        fail("node wrapper bind varchar failed: \{message}")
      }
  }
}

///|
test "js wrapper wasm prepare bind int" {
  let result = run_js_wrapper_prepare_query(
    JsBackend::Wasm,
    "SELECT ? * 2 AS x",
    [("21", "int")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
        fail("wasm wrapper bind int failed: \{message}")
      }
  }
}

///|
test "js wrapper wasm prepare multiple params" {
  let result = run_js_wrapper_prepare_query(
    JsBackend::Wasm,
    "SELECT ? + ? + ? AS x",
    [("10", "int"), ("20", "int"), ("30", "int")],
  )
  match result {
    Ok((_, rows, nulls)) =>
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
        fail("wasm wrapper multiple params failed: \{message}")
      }
  }
}

// ============================================================================
// Error Propagation Tests
// ============================================================================

///|
test "js node prepare bind chain with ? operator" {
  let result = run_js_prepare_query(JsBackend::Node, "SELECT ? + ? AS x", [
    ("10", "int"),
    ("20", "int"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? + ? + ? AS x", [
    ("10", "int"),
    ("20", "int"),
    ("30", "int"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? AS x", [
    ("it's a test", "varchar"),
  ])
  match result {
    Ok((_, rows, nulls)) =>
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
  let result = run_js_prepare_query(JsBackend::Wasm, "SELECT ? AS x", [
    (malicious, "varchar"),
  ])
  match result {
    Ok((_, rows, _)) =>
      if rows.length() != 1 || rows[0].length() != 1 {
        fail("expected 1 row, 1 column")
      } else if rows[0][0] != malicious {
        fail(
          "SQL injection vulnerability detected! expected '\{malicious}', got '\{rows[0][0]}'",
        )
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

// ============================================================================
// Appender Tests
// ============================================================================

///|
extern "js" fn js_run_appender_test(
  backend : JsBackend,
  setup_sql : String,
  table : String,
  operations : String,
) -> String =
  #|(backend, setup_sql, table, operations) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const setupSql = " + JSON.stringify(setup_sql) + ";",
  #|    "const table = " + JSON.stringify(table) + ";",
  #|    "const operations = " + operations + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "let floatTypeId = null;",
  #|    "let doubleTypeId = null;",
  #|    "const toCell = (value, typeId) => {",
  #|    "  if (value === null || value === undefined) { return ['', true]; }",
  #|    "  if (typeof value === 'string') {",
  #|    "    if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {",
  #|    "      if (value === 'NaN') { return ['nan', false]; }",
  #|    "      if (value === 'Infinity') { return ['inf', false]; }",
  #|    "      if (value === '-Infinity') { return ['-inf', false]; }",
  #|    "    }",
  #|    "    return [value, false];",
  #|    "  }",
  #|    "  if (typeof value === 'number') {",
  #|    "    if (Number.isNaN(value)) { return ['nan', false]; }",
  #|    "    if (value === Infinity) { return ['inf', false]; }",
  #|    "    if (value === -Infinity) { return ['-inf', false]; }",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  if (typeof value === 'boolean' || typeof value === 'bigint') {",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  return [JSON.stringify(value), false];",
  #|    "};",
  #|    "const pushRow = (values, rows, nulls, typeIds) => {",
  #|    "  const row = [];",
  #|    "  const rowNulls = [];",
  #|    "  for (let i = 0; i < values.length; i++) {",
  #|    "    const cell = toCell(values[i], typeIds ? typeIds[i] : null);",
  #|    "    row.push(cell[0]);",
  #|    "    rowNulls.push(cell[1]);",
  #|    "  }",
  #|    "  rows.push(row);",
  #|    "  nulls.push(rowNulls);",
  #|    "};",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    floatTypeId = api.DuckDBTypeId.FLOAT;",
  #|    "    doubleTypeId = api.DuckDBTypeId.DOUBLE;",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    await connection.run(setupSql);",
  #|    "    const appender = await connection.createAppender(table);",
  #|    "    for (const op of operations) {",
  #|    "      switch (op.type) {",
  #|    "        case 'begin':",
  #|    "          // No-op in Node API",
  #|    "          break;",
  #|    "        case 'append_int':",
  #|    "          appender.appendInteger(op.value);",
  #|    "          break;",
  #|    "        case 'append_bigint':",
  #|    "          appender.appendBigInt(BigInt(op.value));",
  #|    "          break;",
  #|    "        case 'append_double':",
  #|    "          appender.appendDouble(op.value);",
  #|    "          break;",
  #|    "        case 'append_varchar':",
  #|    "          appender.appendVarchar(String(op.value));",
  #|    "          break;",
  #|    "        case 'append_bool':",
  #|    "          appender.appendBoolean(op.value);",
  #|    "          break;",
  #|    "        case 'append_null':",
  #|    "          appender.appendNull();",
  #|    "          break;",
  #|    "        case 'end_row':",
  #|    "          appender.endRow();",
  #|    "          break;",
  #|    "        case 'flush':",
  #|    "          appender.flushSync();",
  #|    "          break;",
  #|    "        case 'close':",
  #|    "          appender.closeSync();",
  #|    "          break;",
  #|    "      }",
  #|    "    }",
  #|    "    const result = await connection.run('SELECT * FROM ' + table + ' ORDER BY i');",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      pushRow(Array.isArray(row) ? row : columns.map(n => row[n]), rows, nulls, typeIds);",
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
  #|    "    throw new Error('Appender is not supported for WASM backend');",
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
  #|  ].join("\n");
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
fn run_appender_test(
  backend : JsBackend,
  setup_sql : String,
  table : String,
  operations : Array[(String, Int)],
) -> Result[(Array[String], Array[Array[String]], Array[Array[Bool]]), String] {
  // Convert operations to JSON
  let mut ops_json = "["
  for op in operations {
    let (op_type, value) = op
    let op_str = "{\"type\":\"" +
      op_type +
      "\",\"value\":" +
      value.to_string() +
      "}"
    ops_json = ops_json + op_str
    if op != operations[operations.length() - 1] {
      ops_json = ops_json + ","
    }
  }
  ops_json = ops_json + "]"
  let payload = js_run_appender_test(backend, setup_sql, table, ops_json)
  if payload is "" {
    Err("no output from appender test runner")
  } else {
    decode_query_result(payload)
  }
}

///|
test "js node appender basic" {
  let setup_sql = "CREATE TABLE test_appender (i INTEGER, v VARCHAR)"
  let operations = [
    ("append_int", 1),
    ("append_varchar", 101),
    ("end_row", 0),
    ("append_int", 2),
    ("append_varchar", 102),
    ("end_row", 0),
    ("flush", 0),
  ]
  let result = run_appender_test(
    JsBackend::Node,
    setup_sql,
    "test_appender",
    operations,
  )
  match result {
    Ok((_, rows, nulls)) =>
      if rows.length() != 2 {
        fail("expected 2 rows, got \{rows.length()}")
      } else if nulls[0][0] || nulls[0][1] {
        fail("expected non-null values in row 0")
      } else if rows[0][0] != "1" || rows[0][1] != "101" {
        fail(
          "expected ['1', '101'] in row 0, got ['\{rows[0][0]}', '\{rows[0][1]}']",
        )
      } else if rows[1][0] != "2" || rows[1][1] != "102" {
        fail(
          "expected ['2', '102'] in row 1, got ['\{rows[1][0]}', '\{rows[1][1]}']",
        )
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node appender basic test failed: \{message}")
      }
  }
}

///|
test "js node appender with null" {
  let setup_sql = "CREATE TABLE test_appender_null (i INTEGER, v VARCHAR)"
  let operations = [
    ("append_int", 1),
    ("append_varchar", 101),
    ("end_row", 0),
    ("append_int", 2),
    ("append_null", 0),
    ("end_row", 0),
    ("append_int", 3),
    ("append_varchar", 103),
    ("end_row", 0),
    ("flush", 0),
  ]
  let result = run_appender_test(
    JsBackend::Node,
    setup_sql,
    "test_appender_null",
    operations,
  )
  match result {
    Ok((_, rows, nulls)) =>
      if rows.length() != 3 {
        fail("expected 3 rows, got \{rows.length()}")
      } else if !nulls[1][1] {
        fail("expected null in row 1 column 1")
      } else {
        ()
      }
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node appender null test failed: \{message}")
      }
  }
}

///|
test "js wasm appender not supported" {
  let setup_sql = "CREATE TABLE test_appender (i INTEGER, v VARCHAR)"
  let operations = []
  let result = run_appender_test(
    JsBackend::Wasm,
    setup_sql,
    "test_appender",
    operations,
  )
  match result {
    Ok(_) => fail("wasm appender should not be supported")
    Err(message) =>
      if message.contains("Appender is not supported for WASM") {
        ()
      } else {
        fail("wasm appender should return not supported error, got: \{message}")
      }
  }
}

// ============================================================================
// Advanced Type Bind Tests
// ============================================================================

///|
extern "js" fn js_run_advanced_bind_test(
  backend : JsBackend,
  setup_sql : String,
  sql : String,
  bind_type : String,
  bind_value : String,
) -> String =
  #|(backend, setup_sql, sql, bind_type, bind_value) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const backend = " + backend + ";",
  #|    "const setupSql = " + JSON.stringify(setup_sql) + ";",
  #|    "const sql = " + JSON.stringify(sql) + ";",
  #|    "const bindType = " + JSON.stringify(bind_type) + ";",
  #|    "const bindValue = " + bind_value + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "let floatTypeId = null;",
  #|    "let doubleTypeId = null;",
  #|    "const toCell = (value, typeId) => {",
  #|    "  if (value === null || value === undefined) { return ['', true]; }",
  #|    "  if (typeof value === 'string') {",
  #|    "    if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {",
  #|    "      if (value === 'NaN') { return ['nan', false]; }",
  #|    "      if (value === 'Infinity') { return ['inf', false]; }",
  #|    "      if (value === '-Infinity') { return ['-inf', false]; }",
  #|    "    }",
  #|    "    return [value, false];",
  #|    "  }",
  #|    "  if (typeof value === 'number') {",
  #|    "    if (Number.isNaN(value)) { return ['nan', false]; }",
  #|    "    if (value === Infinity) { return ['inf', false]; }",
  #|    "    if (value === -Infinity) { return ['-inf', false]; }",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  if (typeof value === 'boolean' || typeof value === 'bigint') {",
  #|    "    return [String(value), false];",
  #|    "  }",
  #|    "  return [JSON.stringify(value), false];",
  #|    "};",
  #|    "const pushRow = (values, rows, nulls, typeIds) => {",
  #|    "  const row = [];",
  #|    "  const rowNulls = [];",
  #|    "  for (let i = 0; i < values.length; i++) {",
  #|    "    const cell = toCell(values[i], typeIds ? typeIds[i] : null);",
  #|    "    row.push(cell[0]);",
  #|    "    rowNulls.push(cell[1]);",
  #|    "  }",
  #|    "  rows.push(row);",
  #|    "  nulls.push(rowNulls);",
  #|    "};",
  #|    "const run = async () => {",
  #|    "  if (backend === 1) {",
  #|    "    const api = await import('@duckdb/node-api');",
  #|    "    floatTypeId = api.DuckDBTypeId.FLOAT;",
  #|    "    doubleTypeId = api.DuckDBTypeId.DOUBLE;",
  #|    "    const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "    const connection = await instance.connect();",
  #|    "    await connection.run(setupSql);",
  #|    "    const stmt = await connection.prepare(sql);",
  #|    "    if (bindType === 'decimal') {",
  #| "      stmt.bindDecimal(1, api.decimalValue(BigInt(bindValue.value), bindValue.width, bindValue.scale));",
  #|    "    } else if (bindType === 'interval') {",
  #|    "      stmt.bindInterval(1, api.intervalValue(bindValue.months, bindValue.days, BigInt(bindValue.micros)));",
  #|    "    } else if (bindType === 'list') {",
  #|    "      stmt.bindList(1, api.listValue(bindValue.items));",
  #|    "    } else if (bindType === 'struct') {",
  #|    "      const entries = bindValue.fields.map((name, i) => ({ name, value: bindValue.values[i] }));",
  #|    "      stmt.bindStruct(1, api.structValue(entries));",
  #|    "    } else if (bindType === 'map') {",
  #|    "      const entries = bindValue.keys.map((key, i) => ({ key, value: bindValue.values[i] }));",
  #|    "      stmt.bindMap(1, api.mapValue(entries));",
  #|    "    } else {",
  #|    "      throw new Error('unknown bind type: ' + bindType);",
  #|    "    }",
  #|    "    const result = await stmt.run();",
  #|    "    const columns = result.columnNames();",
  #|    "    const rowsJson = await result.getRowsJson();",
  #|    "    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));",
  #|    "    const rows = [];",
  #|    "    const nulls = [];",
  #|    "    for (const row of rowsJson) {",
  #|    "      pushRow(Array.isArray(row) ? row : columns.map(n => row[n]), rows, nulls, typeIds);",
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
  #|    "  throw new Error('unknown backend');",
  #|    "};",
  #|    "run()",
  #|    "  .then((result) => {",
  #|    "    console.log(JSON.stringify({ ok: true, columns: result.columns, rows: result.rows, nulls: result.nulls }));",
  #|    "  })",
  #|    "  .catch((err) => {",
  #|    "    console.log(JSON.stringify({ ok: false, error: toError(err) }));",
  #|    "  });"
  #|  ].join("\n");
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
test "js node bind decimal" {
  let setup_sql = "CREATE TABLE test_decimal (id INTEGER, value DECIMAL(10,2))"
  let sql = "SELECT value FROM test_decimal WHERE id = ?"
  let bind_value = "{ \"width\": 10, \"scale\": 2, \"value\": 12345 }"
  let payload = js_run_advanced_bind_test(
    JsBackend::Node,
    setup_sql,
    sql,
    "decimal",
    bind_value,
  )
  let result = decode_query_result(payload)
  match result {
    Ok(_) => ()
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node bind decimal test failed: \{message}")
      }
  }
}

///|
test "js node bind interval" {
  let setup_sql = "CREATE TABLE test_interval (id INTEGER, value INTERVAL)"
  let sql = "SELECT value FROM test_interval WHERE id = ?"
  let bind_value = "{ \"months\": 1, \"days\": 2, \"micros\": 3000000 }"
  let payload = js_run_advanced_bind_test(
    JsBackend::Node,
    setup_sql,
    sql,
    "interval",
    bind_value,
  )
  let result = decode_query_result(payload)
  match result {
    Ok(_) => ()
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node bind interval test failed: \{message}")
      }
  }
}

///|
test "js node bind list" {
  let setup_sql = "CREATE TABLE test_list (id INTEGER, values VARCHAR[])"
  let sql = "SELECT values FROM test_list WHERE id = ?"
  let bind_value = "{ \"items\": [\"a\", \"b\", \"c\"] }"
  let payload = js_run_advanced_bind_test(
    JsBackend::Node,
    setup_sql,
    sql,
    "list",
    bind_value,
  )
  let result = decode_query_result(payload)
  match result {
    Ok(_) => ()
    Err(message) =>
      if message.contains("@duckdb/node-api") {
        ()
      } else {
        fail("node bind list test failed: \{message}")
      }
  }
}

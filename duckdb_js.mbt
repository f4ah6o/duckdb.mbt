///|
extern "js" fn js_connect(
  path : String,
  backend : JsBackend,
  on_ok : (Connection) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(path, backend, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const isNode = typeof process !== "undefined" && !!(process.versions && process.versions.node);
  #|  let mode = backend | 0;
  #|  if (mode === 0) {
  #|    mode = isNode ? 1 : 2;
  #|  }
  #|  const connectNode = async () => {
  #|    const api = await import("@duckdb/node-api");
  #|    const instance = await api.DuckDBInstance.create(path || ":memory:");
  #|    const connection = await instance.connect();
  #|    on_ok({ kind: "node", instance, connection });
  #|  };
  #|  const connectWasm = async () => {
  #|    if (typeof Worker === "undefined") {
  #|      throw new Error("duckdb-wasm requires Worker support");
  #|    }
  #|    const duckdb = await import("@duckdb/duckdb-wasm");
  #|    const bundles = duckdb.getJsDelivrBundles();
  #|    const bundle = await duckdb.selectBundle(bundles);
  #|    const logger = new duckdb.ConsoleLogger();
  #|    const worker = new Worker(bundle.mainWorker);
  #|    const db = new duckdb.AsyncDuckDB(logger, worker);
  #|    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  #|    const conn = await db.connect();
  #|    on_ok({ kind: "wasm", db, conn, worker });
  #|  };
  #|  const run = async () => {
  #|    if (mode === 1) {
  #|      await connectNode();
  #|      return;
  #|    }
  #|    if (mode === 2) {
  #|      await connectWasm();
  #|      return;
  #|    }
  #|    throw new Error("unknown JsBackend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
extern "js" fn js_query(
  conn : Connection,
  sql : String,
  on_ok : (Array[String], Array[Array[String]], Array[Array[Bool]]) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const toCell = (value) => {
  #|    if (value === null || value === undefined) {
  #|      return ["", true];
  #|    }
  #|    if (typeof value === "string") {
  #|      return [value, false];
  #|    }
  #|    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
  #|      return [String(value), false];
  #|    }
  #|    return [JSON.stringify(value), false];
  #|  };
  #|  const pushRow = (values, rows, nulls) => {
  #|    const row = [];
  #|    const rowNulls = [];
  #|    for (const value of values) {
  #|      const cell = toCell(value);
  #|      row.push(cell[0]);
  #|      rowNulls.push(cell[1]);
  #|    }
  #|    rows.push(row);
  #|    nulls.push(rowNulls);
  #|  };
  #|  const runNode = async () => {
  #|    const result = await conn.connection.run(sql);
  #|    const columns = result.columnNames();
  #|    const rowsJson = await result.getRowsJson();
  #|    const rows = [];
  #|    const nulls = [];
  #|    for (const row of rowsJson) {
  #|      pushRow(row, rows, nulls);
  #|    }
  #|    on_ok(columns, rows, nulls);
  #|  };
  #|  const runWasm = async () => {
  #|    const arrowResult = await conn.conn.query(sql);
  #|    let columns = [];
  #|    if (arrowResult && arrowResult.schema && arrowResult.schema.fields) {
  #|      columns = arrowResult.schema.fields.map((field) => field.name);
  #|    }
  #|    const rowObjects = arrowResult.toArray().map((row) => row.toJSON());
  #|    if (columns.length === 0 && rowObjects.length > 0) {
  #|      columns = Object.keys(rowObjects[0]);
  #|    }
  #|    const rows = [];
  #|    const nulls = [];
  #|    for (const row of rowObjects) {
  #|      const values = columns.map((name) => row[name]);
  #|      pushRow(values, rows, nulls);
  #|    }
  #|    on_ok(columns, rows, nulls);
  #|  };
  #|  const run = async () => {
  #|    if (conn && conn.kind === "node") {
  #|      await runNode();
  #|      return;
  #|    }
  #|    if (conn && conn.kind === "wasm") {
  #|      await runWasm();
  #|      return;
  #|    }
  #|    throw new Error("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
extern "js" fn js_close(
  conn : Connection,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const run = async () => {
  #|    if (conn && conn.kind === "node") {
  #|      if (conn.connection && typeof conn.connection.closeSync === "function") {
  #|        conn.connection.closeSync();
  #|      }
  #|      if (conn.instance && typeof conn.instance.close === "function") {
  #|        await conn.instance.close();
  #|      }
  #|      on_ok();
  #|      return;
  #|    }
  #|    if (conn && conn.kind === "wasm") {
  #|      if (conn.conn && typeof conn.conn.close === "function") {
  #|        await conn.conn.close();
  #|      }
  #|      if (conn.db && typeof conn.db.terminate === "function") {
  #|        await conn.db.terminate();
  #|      }
  #|      if (conn.worker && typeof conn.worker.terminate === "function") {
  #|        conn.worker.terminate();
  #|      }
  #|      on_ok();
  #|      return;
  #|    }
  #|    throw new Error("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
pub fn connect(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  touch_public_types(backend)
  js_connect(path, backend, fn(conn) { on_ready(Ok(conn)) }, fn(message) {
    on_ready(Err(DuckDBError::Message(message)))
  })
}

///|
pub fn Connection::close(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  js_close(self, fn() { on_done(Ok(())) }, fn(message) {
    on_done(Err(DuckDBError::Message(message)))
  })
}

///|
pub fn Connection::query(
  self : Connection,
  sql : String,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  js_query(
    self,
    sql,
    fn(columns, rows, nulls) { on_done(Ok({ columns, rows, nulls })) },
    fn(message) { on_done(Err(DuckDBError::Message(message))) },
  )
}

// ============================================================================
// Prepared Statement JS FFI
// ============================================================================

extern "js" fn js_prepare(
  conn : Connection,
  sql : String,
  on_ok : (PreparedStatement) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const run = async () => {
  #|    if (conn && conn.kind === "node") {
  #|      try {
  #|        const statement = await conn.connection.createStatement(sql);
  #|        on_ok({ kind: "prepared", connection: conn, statement });
  #|        return;
  #|      } catch (e) {
  #|        on_err(toError(e));
  #|        return;
  #|      }
  #|    }
  #|    if (conn && conn.kind === "wasm") {
  #|      try {
  #|        // WASM doesn't have true prepared statements, use query with parameters
  #|        on_ok({ kind: "prepared", connection: conn, sql, params: {} });
  #|        return;
  #|      } catch (e) {
  #|        on_err(toError(e));
  #|        return;
  #|      }
  #|    }
  #|    throw new Error("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

extern "js" fn js_bind_int(
  stmt : PreparedStatement,
  index : Int,
  value : Int,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, value, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindInteger(index, value);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      // WASM: store parameters for later use in execute
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "int", value };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_bigint(
  stmt : PreparedStatement,
  index : Int,
  value : Int,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, value, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindBigInt(index, BigInt(value));
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "bigint", value };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_double(
  stmt : PreparedStatement,
  index : Int,
  value : Double,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, value, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindDouble(index, value);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "double", value };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_varchar(
  stmt : PreparedStatement,
  index : Int,
  value : String,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, value, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindVarchar(index, value);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "varchar", value };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_bool(
  stmt : PreparedStatement,
  index : Int,
  value : Bool,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, value, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindBoolean(index, value);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "bool", value };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_null(
  stmt : PreparedStatement,
  index : Int,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindNull(index);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "null", value: null };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_clear_bindings(
  stmt : PreparedStatement,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.clearBindings();
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      stmt.params = {};
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_date(
  stmt : PreparedStatement,
  index : Int,
  date_str : String,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, date_str, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindDate(index, date_str);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "date", value: date_str };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_bind_timestamp(
  stmt : PreparedStatement,
  index : Int,
  timestamp_str : String,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, index, timestamp_str, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        stmt.statement.bindTimestamp(index, timestamp_str);
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      if (!stmt.params) stmt.params = {};
  #|      stmt.params[index] = { type: "timestamp", value: timestamp_str };
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_execute_prepared(
  stmt : PreparedStatement,
  on_ok : (Array[String], Array[Array[String]], Array[Array[Bool]]) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  const toCell = (value) => {
  #|    if (value === null || value === undefined) {
  #|      return ["", true];
  #|    }
  #|    if (typeof value === "string") {
  #|      return [value, false];
  #|    }
  #|    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
  #|      return [String(value), false];
  #|    }
  #|    return [JSON.stringify(value), false];
  #|  };
  #|  const pushRow = (values, rows, nulls) => {
  #|    const row = [];
  #|    const rowNulls = [];
  #|    for (const value of values) {
  #|      const cell = toCell(value);
  #|      row.push(cell[0]);
  #|      rowNulls.push(cell[1]);
  #|    }
  #|    rows.push(row);
  #|    nulls.push(rowNulls);
  #|  };
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        const result = await stmt.statement.run();
  #|        const columns = result.columnNames();
  #|        const rowsJson = await result.getRowsJson();
  #|        const rows = [];
  #|        const nulls = [];
  #|        for (const row of rowsJson) {
  #|          pushRow(row, rows, nulls);
  #|        }
  #|        on_ok(columns, rows, nulls);
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.sql) {
  #|      try {
  #|        // For WASM, construct the query with parameters
  #|        let sql = stmt.sql;
  #|        if (stmt.params) {
  #|          const params = Object.keys(stmt.params).map(Number).sort((a, b) => a - b);
  #|          for (const index of params) {
  #|            const param = stmt.params[index];
  #|            if (param.type === "null") {
  #|              sql = sql.replace(/\?/, "NULL");
  #|            } else if (param.type === "varchar") {
  #|              sql = sql.replace(/\?/, "'" + String(param.value).replace(/'/g, "''") + "'");
  #|            } else {
  #|              sql = sql.replace(/\?/, String(param.value));
  #|            }
  #|          }
  #|        }
  #|        const arrowResult = await stmt.connection.conn.query(sql);
  #|        let columns = [];
  #|        if (arrowResult && arrowResult.schema && arrowResult.schema.fields) {
  #|          columns = arrowResult.schema.fields.map((field) => field.name);
  #|        }
  #|        const rowObjects = arrowResult.toArray().map((row) => row.toJSON());
  #|        if (columns.length === 0 && rowObjects.length > 0) {
  #|          columns = Object.keys(rowObjects[0]);
  #|        }
  #|        const rows = [];
  #|        const nulls = [];
  #|        for (const row of rowObjects) {
  #|          const values = columns.map((name) => row[name]);
  #|          pushRow(values, rows, nulls);
  #|        }
  #|        on_ok(columns, rows, nulls);
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

extern "js" fn js_close_prepared(
  stmt : PreparedStatement,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        // Node-api statements are automatically cleaned up
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (stmt && stmt.kind === "prepared" && stmt.params !== undefined) {
  #|      // WASM: just clear params
  #|      stmt.params = {};
  #|      on_ok();
  #|      return;
  #|    }
  #|    on_err("unknown statement backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

// ============================================================================
// Prepared Statement API Implementation
// ============================================================================

///|
pub fn Connection::prepare(
  self : Connection,
  sql : String,
  on_done~ : (Result[PreparedStatement, DuckDBError]) -> Unit,
) -> Unit {
  js_prepare(self, sql, fn(stmt) { on_done(Ok(stmt)) }, fn(message) {
    on_done(Err(DuckDBError::Message(message)))
  })
}

///|
pub fn PreparedStatement::bind_int(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  // Note: JS bind functions use callbacks, but the MoonBit API is synchronous
  // This is a known limitation - the bind happens asynchronously but errors are silently ignored
  js_bind_int(self, index, value, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_bigint(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  js_bind_bigint(self, index, value, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_double(
  self : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, DuckDBError] {
  js_bind_double(self, index, value, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_varchar(
  self : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, DuckDBError] {
  js_bind_varchar(self, index, value, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_bool(
  self : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  js_bind_bool(self, index, value, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_null(
  self : PreparedStatement,
  index : Int,
) -> Result[Unit, DuckDBError] {
  js_bind_null(self, index, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::clear_bindings(
  self : PreparedStatement,
) -> Result[Unit, DuckDBError] {
  js_clear_bindings(self, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

// ============================================================================
// Date/Timestamp API Implementation (JS Backend)
// ============================================================================

///|
pub fn PreparedStatement::bind_date(
  self : PreparedStatement,
  index : Int,
  days : Int,
) -> Result[Unit, DuckDBError] {
  // Convert days to ISO date string for JS
  let (year, month, day) = date_to_ymd(days)
  let date_str = "\\{year}-\\{String::pad_left(month.to_string(), 2, '0')}-\\{String::pad_left(day.to_string(), 2, '0')}"
  js_bind_date(self, index, date_str, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn PreparedStatement::bind_timestamp(
  self : PreparedStatement,
  index : Int,
  micros : Int,
) -> Result[Unit, DuckDBError] {
  // Convert microseconds to ISO timestamp string for JS
  let (year, month, day, hour, minute, second) = timestamp_to_ymd_hms(micros)
  let date_str = "\\{year}-\\{String::pad_left(month.to_string(), 2, '0')}-\\{String::pad_left(day.to_string(), 2, '0')}"
  let time_str = "\\{String::pad_left(hour.to_string(), 2, '0')}:\\{String::pad_left(minute.to_string(), 2, '0')}:\\{String::pad_left(second.to_string(), 2, '0')}"
  let timestamp_str = "\\{date_str} \\{time_str}"
  js_bind_timestamp(self, index, timestamp_str, fn() { () }, fn(message) {
    let _ = message
  })
  Ok(())
}

///|
pub fn Appender::append_date(self : Appender, days : Int) -> Result[Unit, DuckDBError] {
  let _ = days
  // Appender not supported in JS backend
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_timestamp(self : Appender, micros : Int) -> Result[Unit, DuckDBError] {
  let _ = micros
  // Appender not supported in JS backend
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
/// Check if a year is a leap year in the Gregorian calendar.
fn is_leap_year(year : Int) -> Bool {
  (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

///|
/// Cumulative days before each month (for non-leap years).
fn days_before_month(m : Int) -> Int {
  match m {
    1 => 0
    2 => 31
    3 => 59
    4 => 90
    5 => 120
    6 => 151
    7 => 181
    8 => 212
    9 => 243
    10 => 273
    11 => 304
    12 => 334
    _ => 0
  }
}

///|
/// Count leap years between year 1 and the given year (exclusive).
fn count_leap_years_before(year : Int) -> Int {
  let y = year - 1
  (y / 4) - (y / 100) + (y / 400)
}

///|
/// Convert year, month, day to days since 1970-01-01 (Unix epoch).
/// Uses accurate Gregorian calendar calculation with leap year support.
pub fn date_from_ymd(year : Int, month : Int, day : Int) -> Int {
  // Days from 1970 to the beginning of the given year
  let year_delta = year - 1970
  let leap_years = count_leap_years_before(year) - count_leap_years_before(1970)
  let year_days = year_delta * 365 + leap_years

  // Days from beginning of year to beginning of month
  let month_days = days_before_month(month)

  // Add leap day if past February in a leap year
  let leap_day = if month > 2 && is_leap_year(year) { 1 } else { 0 }

  // Day of month (0-indexed)
  let day_days = day - 1

  year_days + month_days + leap_day + day_days
}

///|
/// Convert days since 1970-01-01 to year, month, day.
/// Uses accurate Gregorian calendar calculation with leap year support.
pub fn date_to_ymd(total_days : Int) -> (Int, Int, Int) {
  // Use a simpler algorithm: count years from 1970
  let mut days = total_days
  let mut year = 1970

  // Count years forward
  while days >= 365 {
    let leap = if is_leap_year(year) { 1 } else { 0 }
    let days_in_year = 365 + leap
    if days >= days_in_year {
      days = days - days_in_year
      year = year + 1
    } else {
      break
    }
  }

  // Find month
  let is_leap = is_leap_year(year)
  let mut month = 1
  let mut remaining = days

  // Cumulative days for each month
  while month <= 12 {
    let dim = match month {
      2 => if is_leap { 29 } else { 28 }
      4 | 6 | 9 | 11 => 30
      _ => 31
    }
    if remaining < dim {
      break
    }
    remaining = remaining - dim
    month = month + 1
  }

  let day = remaining + 1
  (year, month, day)
}

///|
/// Convert date components to a timestamp (microseconds since 1970-01-01).
/// Note: This is a simplified calculation. For production use, use a proper date library.
pub fn timestamp_from_ymd_hms(
  year : Int,
  month : Int,
  day : Int,
  hour : Int,
  minute : Int,
  second : Int,
) -> Int {
  let date_days = date_from_ymd(year, month, day)
  let seconds_per_day = 86400
  let date_seconds = date_days * seconds_per_day
  let time_seconds = hour * 3600 + minute * 60 + second
  (date_seconds + time_seconds) * 1000000
}

///|
/// Convert timestamp (microseconds since 1970-01-01) to date components.
/// Note: This is a simplified calculation. For production use, use a proper date library.
pub fn timestamp_to_ymd_hms(micros : Int) -> (Int, Int, Int, Int, Int, Int) {
  let total_seconds = micros / 1000000
  let days = total_seconds / 86400
  let seconds_in_day = total_seconds % 86400
  let hour = seconds_in_day / 3600
  let remaining_seconds = seconds_in_day % 3600
  let minute = remaining_seconds / 60
  let second = remaining_seconds % 60
  let (year, month, day) = date_to_ymd(days)
  (year, month, day, hour, minute, second)
}

///|
pub fn PreparedStatement::execute(
  self : PreparedStatement,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  js_execute_prepared(
    self,
    fn(columns, rows, nulls) { on_done(Ok({ columns, rows, nulls })) },
    fn(message) { on_done(Err(DuckDBError::Message(message))) },
  )
}

///|
pub fn PreparedStatement::close(
  self : PreparedStatement,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  js_close_prepared(self, fn() { on_done(Ok(())) }, fn(message) {
    on_done(Err(DuckDBError::Message(message)))
  })
}

// ============================================================================
// Configuration API Implementation
// ============================================================================

extern "js" fn js_config_create() -> Config =
  #|() => {
  #|  return { kind: "config", options: {} };
  #|}

extern "js" fn js_config_set(
  cfg : Config,
  key : String,
  value : String,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(cfg, key, value, on_ok, on_err) => {
  #|  try {
  #|    cfg.options[key] = value;
  #|    on_ok();
  #|  } catch (e) {
  #|    on_err(e.message || String(e));
  #|  }
  #|}

extern "js" fn js_connect_with_config(
  path : String,
  config : Config,
  backend : JsBackend,
  on_ok : (Connection) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(path, config, backend, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const isNode = typeof process !== "undefined" && !!(process.versions && process.versions.node);
  #|  let mode = backend | 0;
  #|  if (mode === 0) {
  #|    mode = isNode ? 1 : 2;
  #|  }
  #|
  #|  const configOptions = config.options || {};
  #|
  #|  const connectNode = async () => {
  #|    const api = await import("@duckdb/node-api");
  #|    const instance = await api.DuckDBInstance.create(path || ":memory:", configOptions);
  #|    const connection = await instance.connect();
  #|    on_ok({ kind: "node", instance, connection });
  #|  };
  #|
  #|  const connectWasm = async () => {
  #|    if (typeof Worker === "undefined") {
  #|      throw new Error("duckdb-wasm requires Worker support");
  #|    }
  #|    const duckdb = await import("@duckdb/duckdb-wasm");
  #|    const bundles = duckdb.getJsDelivrBundles();
  #|    const bundle = await duckdb.selectBundle(bundles);
  #|    const logger = new duckdb.ConsoleLogger();
  #|    const worker = new Worker(bundle.mainWorker);
  #|    const db = new duckdb.AsyncDuckDB(logger, worker);
  #|    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  #|    const conn = await db.connect();
  #|    on_ok({ kind: "wasm", db, conn, worker });
  #|  };
  #|
  #|  const run = async () => {
  #|    if (mode === 1) {
  #|      await connectNode();
  #|      return;
  #|    }
  #|    if (mode === 2) {
  #|      await connectWasm();
  #|      return;
  #|    }
  #|    throw new Error("unknown JsBackend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
pub fn Config::create() -> Config {
  js_config_create()
}

///|
pub fn Config::set(
  self : Config,
  key : String,
  value : String,
) -> Result[Unit, DuckDBError] {
  // JS uses callbacks, ignore errors silently for now
  js_config_set(self, key, value, fn() { () }, fn(msg) {
    let _ = msg
  })
  Ok(())
}

///|
pub fn connect_with_config(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  config : Config?,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  touch_public_types(backend)
  match config {
    Some(cfg) => {
      js_connect_with_config(path, cfg, backend, fn(conn) { on_ready(Ok(conn)) }, fn(message) {
        on_ready(Err(DuckDBError::Message(message)))
      })
    }
    None => {
      // Call the original connect function - need to create a wrapper
      js_connect(path, backend, fn(conn) { on_ready(Ok(conn)) }, fn(message) {
        on_ready(Err(DuckDBError::Message(message)))
      })
    }
  }
}

// ============================================================================
// Appender API Implementation (Basic - for future expansion)
// ============================================================================

///|
pub fn Connection::create_appender(
  self : Connection,
  schema : String,
  table : String,
  on_done~ : (Result[Appender, DuckDBError]) -> Unit,
) -> Unit {
  // JS Appender is complex and requires table schema knowledge
  // For now, return not supported error
  on_done(Err(DuckDBError::Message("Appender not yet supported for JS backend")))
}

///|
pub fn Appender::begin_row(self : Appender) -> Result[Unit, DuckDBError] {
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_int(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_bigint(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_double(self : Appender, value : Double) -> Result[Unit, DuckDBError] {
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_varchar(self : Appender, value : String) -> Result[Unit, DuckDBError] {
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_bool(self : Appender, value : Bool) -> Result[Unit, DuckDBError] {
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_null(self : Appender) -> Result[Unit, DuckDBError] {
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::end_row(self : Appender) -> Result[Unit, DuckDBError] {
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::flush(self : Appender) -> Result[Unit, DuckDBError] {
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::close(self : Appender, on_done~ : (Result[Unit, DuckDBError]) -> Unit) -> Unit {
  on_done(Err(DuckDBError::Message("Appender not yet supported for JS backend")))
}

// ============================================================================
// Advanced Data Types for JS Backend
// ============================================================================

// ----------------------------------------------------------------------------
// Blob Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_blob(
  self : PreparedStatement,
  index : Int,
  value : Bytes,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("bind_blob not yet supported for JS backend"))
}

///|
pub fn Appender::append_blob(self : Appender, value : Bytes) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

// ----------------------------------------------------------------------------
// Decimal Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_decimal(
  self : PreparedStatement,
  index : Int,
  value : Decimal,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("bind_decimal not yet supported for JS backend"))
}

///|
pub fn Appender::append_decimal(self : Appender, value : Decimal) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

// Helper function for power of 10
fn int_pow10(exp : Int) -> Int {
  let mut result = 1
  let mut i = 0
  while i < exp {
    result = result * 10
    i = i + 1
  }
  result
}

///|
pub fn decimal_from_double(value : Double, width : Int, scale : Int) -> Decimal {
  let multiplier = int_pow10(scale)
  let scaled = (value * Double::from_int(multiplier)).to_int()
  { width, scale, value: scaled }
}

///|
pub fn decimal_to_double(decimal : Decimal) -> Double {
  let divisor = Double::from_int(int_pow10(decimal.scale))
  Double::from_int(decimal.value) / divisor
}

///|
pub fn decimal_from_parts(whole : Int, fractional : Int, scale : Int) -> Decimal {
  let value = whole * int_pow10(scale) + fractional
  let width = if whole == 0 {
    String::length(fractional.to_string())
  } else {
    String::length(whole.to_string()) + scale
  }
  { width: width.max(1), scale, value }
}

///|
pub fn decimal_to_parts(decimal : Decimal) -> (Int, Int) {
  let divisor = int_pow10(decimal.scale)
  let whole = decimal.value / divisor
  let fractional = Int::abs(decimal.value % divisor)
  (whole, fractional)
}

// ----------------------------------------------------------------------------
// Interval Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_interval(
  self : PreparedStatement,
  index : Int,
  value : Interval,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("bind_interval not yet supported for JS backend"))
}

///|
pub fn Appender::append_interval(self : Appender, value : Interval) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn interval_from_parts(months : Int, days : Int, micros : Int) -> Interval {
  { months, days, micros }
}

///|
pub fn interval_from_days(days : Int) -> Interval {
  { months: 0, days, micros: 0 }
}

///|
pub fn interval_from_hours(hours : Int) -> Interval {
  let micros = hours * 3600 * 1000000
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_minutes(minutes : Int) -> Interval {
  let micros = minutes * 60 * 1000000
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_seconds(seconds : Int) -> Interval {
  let micros = seconds * 1000000
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_months(months : Int) -> Interval {
  { months, days: 0, micros: 0 }
}

///|
pub fn interval_to_micros(interval : Interval) -> Int {
  let days_micros = interval.days * 86400 * 1000000
  interval.micros + days_micros
}

// ----------------------------------------------------------------------------
// List Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_list_varchar(
  self : PreparedStatement,
  index : Int,
  values : Array[String],
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = values
  Err(DuckDBError::Message("bind_list_varchar not yet supported for JS backend"))
}

///|
pub fn list_from_strings(elements : Array[String]) -> List {
  { elements }
}

///|
pub fn list_length(list : List) -> Int {
  list.elements.length()
}

///|
pub fn list_get(list : List, index : Int) -> String? {
  if index >= 0 && index < list.elements.length() {
    Some(list.elements[index])
  } else {
    None
  }
}

// ----------------------------------------------------------------------------
// Struct Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_struct(
  self : PreparedStatement,
  index : Int,
  value : Struct,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("bind_struct not yet supported for JS backend"))
}

///|
pub fn struct_from_arrays(fields : Array[String], values : Array[String]) -> Struct {
  { fields, values }
}

///|
pub fn struct_from_pairs(pairs : Array[(String, String)]) -> Struct {
  let fields = pairs.map(fn(p) { p.0 })
  let values = pairs.map(fn(p) { p.1 })
  { fields, values }
}

// Helper function to find index in array
fn array_index_of(arr : Array[String], val : String) -> Option[Int] {
  let mut i = 0
  while i < arr.length() {
    if arr[i] == val {
      return Some(i)
    }
    i = i + 1
  }
  None
}

///|
pub fn struct_get(s : Struct, field_name : String) -> String? {
  let idx = array_index_of(s.fields, field_name)
  match idx {
    Some(i) => {
      if i < s.values.length() {
        Some(s.values[i])
      } else {
        None
      }
    }
    None => None
  }
}

///|
pub fn struct_field_count(s : Struct) -> Int {
  s.fields.length()
}

// ----------------------------------------------------------------------------
// Map Type
// ----------------------------------------------------------------------------

///|
pub fn PreparedStatement::bind_map(
  self : PreparedStatement,
  index : Int,
  map : Map,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = map
  Err(DuckDBError::Message("bind_map not yet supported for JS backend"))
}

///|
pub fn map_from_arrays(keys : Array[String], values : Array[String]) -> Map {
  { keys, values }
}

///|
pub fn map_from_pairs(pairs : Array[(String, String)]) -> Map {
  let keys = pairs.map(fn(p) { p.0 })
  let values = pairs.map(fn(p) { p.1 })
  { keys, values }
}

///|
pub fn map_get(m : Map, key : String) -> String? {
  let idx = array_index_of(m.keys, key)
  match idx {
    Some(i) => {
      if i < m.values.length() {
        Some(m.values[i])
      } else {
        None
      }
    }
    None => None
  }
}

///|
pub fn map_size(m : Map) -> Int {
  m.keys.length()
}

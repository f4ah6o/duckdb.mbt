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

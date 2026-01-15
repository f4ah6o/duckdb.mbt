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
  on_ok : (Array[String], Array[Array[String]], Array[Array[Bool]], Array[Int]) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  let floatTypeId = null;
  #|  let doubleTypeId = null;
  #|  const toCell = (value, typeId) => {
  #|    if (value === null || value === undefined) {
  #|      return ["", true];
  #|    }
  #|    if (typeof value === "string") {
  #|      if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {
  #|        if (value === "NaN") {
  #|          return ["nan", false];
  #|        }
  #|        if (value === "Infinity") {
  #|          return ["inf", false];
  #|        }
  #|        if (value === "-Infinity") {
  #|          return ["-inf", false];
  #|        }
  #|      }
  #|      return [value, false];
  #|    }
  #|    if (typeof value === "number") {
  #|      if (Number.isNaN(value)) {
  #|        return ["nan", false];
  #|      }
  #|      if (value === Infinity) {
  #|        return ["inf", false];
  #|      }
  #|      if (value === -Infinity) {
  #|        return ["-inf", false];
  #|      }
  #|      return [String(value), false];
  #|    }
  #|    if (typeof value === "boolean" || typeof value === "bigint") {
  #|      return [String(value), false];
  #|    }
  #|    return [JSON.stringify(value), false];
  #|  };
  #|  const pushRow = (values, rows, nulls, typeIds) => {
  #|    const row = [];
  #|    const rowNulls = [];
  #|    for (let i = 0; i < values.length; i++) {
  #|      const cell = toCell(values[i], typeIds ? typeIds[i] : null);
  #|      row.push(cell[0]);
  #|      rowNulls.push(cell[1]);
  #|    }
  #|    rows.push(row);
  #|    nulls.push(rowNulls);
  #|  };
  #|  const runNode = async () => {
  #|    const api = await import('@duckdb/node-api');
  #|    floatTypeId = api.DuckDBTypeId.FLOAT;
  #|    doubleTypeId = api.DuckDBTypeId.DOUBLE;
  #|    const result = await conn.connection.run(sql);
  #|    const columns = result.columnNames();
  #|    const rowsJson = await result.getRowsJson();
  #|    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));
  #|    const rows = [];
  #|    const nulls = [];
  #|    for (const row of rowsJson) {
  #|      const values = Array.isArray(row) ? row : columns.map((name) => row[name]);
  #|      pushRow(values, rows, nulls, typeIds);
  #|    }
  #|    on_ok(columns, rows, nulls, typeIds);
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
  #|      pushRow(values, rows, nulls, null);
  #|    }
  #|    const typeIds = [];
  #|    on_ok(columns, rows, nulls, typeIds);
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
fn column_types_from_ids(
  columns : Array[String],
  type_ids : Array[Int],
) -> Array[ColumnType] {
  let column_types : Array[ColumnType] = []
  let count = columns.length()
  for col = 0; col < count; col = col + 1 {
    if col < type_ids.length() {
      column_types.push(column_type_from_id(type_ids[col]))
    } else {
      column_types.push(ColumnType::Unknown(-1))
    }
  } else {
    ()
  }
  column_types
}

///|
extern "js" fn js_query_stream(
  conn : Connection,
  sql : String,
  on_ok : (ResultStream) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const runNode = async () => {
  #|    const api = await import('@duckdb/node-api');
  #|    const result = await conn.connection.stream(sql);
  #|    const columns = result.columnNames();
  #|    const typeIds = columns.map((_, idx) => result.columnTypeId(idx));
  #|    on_ok({
  #|      kind: "node",
  #|      result,
  #|      columns,
  #|      typeIds,
  #|      floatTypeId: api.DuckDBTypeId.FLOAT,
  #|      doubleTypeId: api.DuckDBTypeId.DOUBLE,
  #|    });
  #|  };
  #|  const runWasm = async () => {
  #|    const reader = await conn.conn.send(sql, true);
  #|    let columns = [];
  #|    if (reader && reader.schema && reader.schema.fields) {
  #|      columns = reader.schema.fields.map((field) => field.name);
  #|    }
  #|    on_ok({ kind: "wasm", reader, columns, done: false });
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
extern "js" fn js_stream_columns(stream : ResultStream) -> Array[String] =
  #|(stream) => {
  #|  if (stream && Array.isArray(stream.columns)) {
  #|    return stream.columns;
  #|  }
  #|  return [];
  #|}

///|
extern "js" fn js_stream_column_count(stream : ResultStream) -> Int =
  #|(stream) => {
  #|  if (stream && Array.isArray(stream.columns)) {
  #|    return stream.columns.length;
  #|  }
  #|  return 0;
  #|}

///|
extern "js" fn js_stream_next(
  stream : ResultStream,
  on_ok : (Array[Array[String]], Array[Array[Bool]]) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stream, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const toCell = (value, typeId) => {
  #|    const floatTypeId = stream.floatTypeId;
  #|    const doubleTypeId = stream.doubleTypeId;
  #|    if (value === null || value === undefined) {
  #|      return ["", true];
  #|    }
  #|    if (typeof value === "string") {
  #|      if (typeId !== null && typeId !== undefined &&
  #|          (typeId === floatTypeId || typeId === doubleTypeId)) {
  #|        if (value === "NaN") return ["nan", false];
  #|        if (value === "Infinity") return ["inf", false];
  #|        if (value === "-Infinity") return ["-inf", false];
  #|      }
  #|      return [value, false];
  #|    }
  #|    if (typeof value === "number") {
  #|      if (Number.isNaN(value)) return ["nan", false];
  #|      if (value === Infinity) return ["inf", false];
  #|      if (value === -Infinity) return ["-inf", false];
  #|      return [String(value), false];
  #|    }
  #|    if (typeof value === "boolean" || typeof value === "bigint") {
  #|      return [String(value), false];
  #|    }
  #|    return [JSON.stringify(value), false];
  #|  };
  #|  const pushRow = (values, rows, nulls, typeIds) => {
  #|    const row = [];
  #|    const rowNulls = [];
  #|    for (let i = 0; i < values.length; i++) {
  #|      const cell = toCell(values[i], typeIds ? typeIds[i] : null);
  #|      row.push(cell[0]);
  #|      rowNulls.push(cell[1]);
  #|    }
  #|    rows.push(row);
  #|    nulls.push(rowNulls);
  #|  };
  #|  const runNode = async () => {
  #|    const chunk = await stream.result.fetchChunk();
  #|    if (!chunk) {
  #|      throw new Error("fetchChunk failed");
  #|    }
  #|    if (chunk.rowCount <= 0) {
  #|      on_ok([], []);
  #|      return;
  #|    }
  #|    const rows = [];
  #|    const nulls = [];
  #|    const values = chunk.getRows();
  #|    for (const row of values) {
  #|      pushRow(row, rows, nulls, stream.typeIds || null);
  #|    }
  #|    on_ok(rows, nulls);
  #|  };
  #|  const runWasm = async () => {
  #|    if (stream.done) {
  #|      on_ok([], []);
  #|      return;
  #|    }
  #|    const next = await stream.reader.next();
  #|    if (!next || next.done || !next.value) {
  #|      stream.done = true;
  #|      on_ok([], []);
  #|      return;
  #|    }
  #|    const batch = next.value;
  #|    const rowCount = batch.numRows ?? batch.length ?? 0;
  #|    let columnCount = batch.numCols ?? 0;
  #|    if (!columnCount && batch.schema && batch.schema.fields) {
  #|      columnCount = batch.schema.fields.length;
  #|    }
  #|    if ((!stream.columns || stream.columns.length === 0) &&
  #|        batch.schema && batch.schema.fields) {
  #|      stream.columns = batch.schema.fields.map((field) => field.name);
  #|    }
  #|    const rows = [];
  #|    const nulls = [];
  #|    for (let r = 0; r < rowCount; r++) {
  #|      const row = [];
  #|      const rowNulls = [];
  #|      for (let c = 0; c < columnCount; c++) {
  #|        const vector = batch.getChildAt ? batch.getChildAt(c) : batch.getChild(c);
  #|        const value = vector ? vector.get(r) : null;
  #|        const cell = toCell(value, null);
  #|        row.push(cell[0]);
  #|        rowNulls.push(cell[1]);
  #|      }
  #|      rows.push(row);
  #|      nulls.push(rowNulls);
  #|    }
  #|    on_ok(rows, nulls);
  #|  };
  #|  const run = async () => {
  #|    if (stream && stream.kind === "node") {
  #|      await runNode();
  #|      return;
  #|    }
  #|    if (stream && stream.kind === "wasm") {
  #|      await runWasm();
  #|      return;
  #|    }
  #|    throw new Error("unknown stream backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
extern "js" fn js_stream_close(
  stream : ResultStream,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stream, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (stream && stream.kind === "wasm") {
  #|      if (stream.reader && typeof stream.reader.cancel === "function") {
  #|        await stream.reader.cancel();
  #|      }
  #|      stream.done = true;
  #|    }
  #|    on_ok();
  #|  };
  #|  run().catch((err) => on_err(err && err.message ? err.message : String(err)));
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
    fn(columns, rows, nulls, type_ids) {
      let column_types = column_types_from_ids(columns, type_ids)
      on_done(Ok({ columns, column_types, rows, nulls }))
    },
    fn(message) { on_done(Err(DuckDBError::Message(message))) },
  )
}

///|
pub fn Connection::query_stream(
  self : Connection,
  sql : String,
  on_done~ : (Result[ResultStream, DuckDBError]) -> Unit,
) -> Unit {
  js_query_stream(self, sql, fn(stream) { on_done(Ok(stream)) }, fn(message) {
    on_done(Err(DuckDBError::Message(message)))
  })
}

///|
pub fn ResultStream::columns(self : ResultStream) -> Array[String] {
  js_stream_columns(self)
}

///|
pub fn ResultStream::column_count(self : ResultStream) -> Int {
  js_stream_column_count(self)
}

///|
pub fn ResultStream::next(
  self : ResultStream,
  on_done~ : (Result[DataChunk?, DuckDBError]) -> Unit,
) -> Unit {
  js_stream_next(
    self,
    fn(rows, nulls) {
      if rows.length() == 0 {
        on_done(Ok(None))
      } else {
        on_done(Ok(Some({ columns: self.columns(), rows, nulls })))
      }
    },
    fn(message) { on_done(Err(DuckDBError::Message(message))) },
  )
}

///|
pub fn ResultStream::close(
  self : ResultStream,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  js_stream_close(self, fn() { on_done(Ok(())) }, fn(message) {
    on_done(Err(DuckDBError::Message(message)))
  })
}

// ============================================================================
// Prepared Statement JS FFI
// ============================================================================

///|
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
  #|        const statement = await conn.connection.prepare(sql);
  #|        on_ok({ kind: "prepared", connection: conn, backend: conn.kind, statement });
  #|        return;
  #|      } catch (e) {
  #|        on_err(toError(e));
  #|        return;
  #|      }
  #|    }
  #|    if (conn && conn.kind === "wasm") {
  #|      try {
  #|        // Use real duckdb-wasm prepared statements
  #|        const statement = await conn.conn.prepare(sql);
  #|        on_ok({
  #|          kind: "prepared",
  #|          connection: conn,
  #|          backend: conn.kind,
  #|          statement,
  #|          sql,
  #|          params: {},
  #|        });
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

///|
extern "js" fn js_bind_int(
  stmt : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, String] =
  #|(stmt, index, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindInteger(index, value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    // WASM: store parameters for later use in execute
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "int", value };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_bigint(
  stmt : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, String] =
  #|(stmt, index, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindBigInt(index, BigInt(value));
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "bigint", value };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_double(
  stmt : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, String] =
  #|(stmt, index, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindDouble(index, value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "double", value };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_varchar(
  stmt : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, String] =
  #|(stmt, index, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindVarchar(index, value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "varchar", value };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_bool(
  stmt : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, String] =
  #|(stmt, index, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindBoolean(index, value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "bool", value };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_null(
  stmt : PreparedStatement,
  index : Int,
) -> Result[Unit, String] =
  #|(stmt, index) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindNull(index);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "null", value: null };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_clear_bindings(
  stmt : PreparedStatement,
) -> Result[Unit, String] =
  #|(stmt) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.clearBindings();
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    stmt.params = {};
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_date(
  stmt : PreparedStatement,
  index : Int,
  date_str : String,
) -> Result[Unit, String] =
  #|(stmt, index, date_str) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindDate(index, date_str);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "date", value: date_str };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

///|
extern "js" fn js_bind_timestamp(
  stmt : PreparedStatement,
  index : Int,
  timestamp_str : String,
) -> Result[Unit, String] =
  #|(stmt, index, timestamp_str) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindTimestamp(index, timestamp_str);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    if (!stmt.params) stmt.params = {};
  #|    stmt.params[index] = { type: "timestamp", value: timestamp_str };
  #|    return { ok: true };
  #|  }
  #|  return { ok: false, error: "unknown statement backend" };
  #|}

// ============================================================================
// Advanced Type Bindings (Node API only)
// ============================================================================

///|
extern "js" fn js_bind_blob(
  stmt : PreparedStatement,
  index : Int,
  data : Bytes,
) -> Result[Unit, String] =
  #|(stmt, index, data) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      const uint8Array = new Uint8Array(data.byteLength);
  #|      for (let i = 0; i < data.byteLength; i++) {
  #|        uint8Array[i] = data.readUint8(i);
  #|      }
  #|      stmt.statement.bindBlob(index, { bytes: uint8Array });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_blob is not supported for WASM backend. Use INSERT statements with CAST or hex encoding instead." };
  #|  }
  #|  return { ok: false, error: "bind_blob is only supported for Node backend" };
  #|}

///|
extern "js" fn js_bind_decimal(
  stmt : PreparedStatement,
  index : Int,
  width : Int,
  scale : Int,
  value : Int,
) -> Result[Unit, String] =
  #|(stmt, index, width, scale, value) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindDecimal(index, { width, scale, value: BigInt(value) });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_decimal is not supported for WASM backend. Use INSERT statements with DECIMAL literals instead." };
  #|  }
  #|  return { ok: false, error: "bind_decimal is only supported for Node backend" };
  #|}

///|
extern "js" fn js_bind_interval(
  stmt : PreparedStatement,
  index : Int,
  months : Int,
  days : Int,
  micros : Int,
) -> Result[Unit, String] =
  #|(stmt, index, months, days, micros) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bindInterval(index, { months, days, micros: BigInt(micros) });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_interval is not supported for WASM backend. Use INSERT statements with INTERVAL literals instead." };
  #|  }
  #|  return { ok: false, error: "bind_interval is only supported for Node backend" };
  #|}

///|
extern "js" fn js_bind_list_varchar(
  stmt : PreparedStatement,
  index : Int,
  values : Array[String],
) -> Result[Unit, String] =
  #|(stmt, index, values) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      stmt.statement.bind(index, { items: values }, { typeId: 24 }); // LIST type
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_list_varchar is not supported for WASM backend. Use INSERT statements with LIST literals instead." };
  #|  }
  #|  return { ok: false, error: "bind_list_varchar is only supported for Node backend" };
  #|}

///|
extern "js" fn js_bind_struct(
  stmt : PreparedStatement,
  index : Int,
  fields : Array[String],
  values : Array[String],
) -> Result[Unit, String] =
  #|(stmt, index, fields, values) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      const entries = fields.map((name, i) => ({ name, value: values[i] }));
  #|      stmt.statement.bindStruct(index, { entries });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_struct is not supported for WASM backend. Use INSERT statements with STRUCT literals instead." };
  #|  }
  #|  return { ok: false, error: "bind_struct is only supported for Node backend" };
  #|}

///|
extern "js" fn js_bind_map(
  stmt : PreparedStatement,
  index : Int,
  keys : Array[String],
  values : Array[String],
) -> Result[Unit, String] =
  #|(stmt, index, keys, values) => {
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "node" && stmt.statement) {
  #|    try {
  #|      const entries = keys.map((key, i) => ({ key, value: values[i] }));
  #|      stmt.statement.bindMap(index, { entries });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  if (stmt && stmt.kind === "prepared" && stmt.connection && stmt.connection.kind === "wasm") {
  #|    return { ok: false, error: "bind_map is not supported for WASM backend. Use INSERT statements with MAP literals instead." };
  #|  }
  #|  return { ok: false, error: "bind_map is only supported for Node backend" };
  #|}

// ============================================================================
// Appender API (Node API only)
// ============================================================================

///|
extern "js" fn js_create_appender(
  conn : Connection,
  table : String,
  on_ok : (Appender) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, table, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const run = async () => {
  #|    if (conn && conn.kind === "node") {
  #|      try {
  #|        const appender = await conn.connection.createAppender(table);
  #|        on_ok({ kind: "appender", connection: conn, appender });
  #|        return;
  #|      } catch (e) {
  #|        on_err(toError(e));
  #|        return;
  #|      }
  #|    }
  #|    if (conn && conn.kind === "wasm") {
  #|      on_err("Appender is not supported for WASM backend. Use INSERT statements, insertCSVFromPath(), insertJSONFromPath(), or insertArrowTable() instead.");
  #|      return;
  #|    }
  #|    on_err("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
extern "js" fn js_appender_begin_row(
  appender : Appender,
) -> Result[Unit, String] =
  #|(appender) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      // No-op in Node API, but kept for API compatibility
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_int(
  appender : Appender,
  value : Int,
) -> Result[Unit, String] =
  #|(appender, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendInteger(value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_bigint(
  appender : Appender,
  value : Int,
) -> Result[Unit, String] =
  #|(appender, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendBigInt(BigInt(value));
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_double(
  appender : Appender,
  value : Double,
) -> Result[Unit, String] =
  #|(appender, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendDouble(value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_varchar(
  appender : Appender,
  value : String,
) -> Result[Unit, String] =
  #|(appender, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendVarchar(value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_bool(
  appender : Appender,
  value : Bool,
) -> Result[Unit, String] =
  #|(appender, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendBoolean(value);
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_null(
  appender : Appender,
) -> Result[Unit, String] =
  #|(appender) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendNull();
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_blob(
  appender : Appender,
  data : Bytes,
) -> Result[Unit, String] =
  #|(appender, data) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      const uint8Array = new Uint8Array(data.byteLength);
  #|      for (let i = 0; i < data.byteLength; i++) {
  #|        uint8Array[i] = data.readUint8(i);
  #|      }
  #|      appender.appender.appendBlob({ bytes: uint8Array });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_decimal(
  appender : Appender,
  width : Int,
  scale : Int,
  value : Int,
) -> Result[Unit, String] =
  #|(appender, width, scale, value) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendDecimal({ width, scale, value: BigInt(value) });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_append_interval(
  appender : Appender,
  months : Int,
  days : Int,
  micros : Int,
) -> Result[Unit, String] =
  #|(appender, months, days, micros) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.appendInterval({ months, days, micros: BigInt(micros) });
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_end_row(appender : Appender) -> Result[Unit, String] =
  #|(appender) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.endRow();
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_flush(appender : Appender) -> Result[Unit, String] =
  #|(appender) => {
  #|  if (appender && appender.kind === "appender" && appender.appender) {
  #|    try {
  #|      appender.appender.flushSync();
  #|      return { ok: true };
  #|    } catch (e) {
  #|      return { ok: false, error: e.message || String(e) };
  #|    }
  #|  }
  #|  return { ok: false, error: "invalid appender" };
  #|}

///|
extern "js" fn js_appender_close(
  appender : Appender,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(appender, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const run = async () => {
  #|    if (appender && appender.kind === "appender" && appender.appender) {
  #|      try {
  #|        appender.appender.closeSync();
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(toError(e));
  #|        return;
  #|      }
  #|    }
  #|    on_err("invalid appender");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
extern "js" fn js_execute_prepared(
  stmt : PreparedStatement,
  on_ok : (Array[String], Array[Array[String]], Array[Array[Bool]], Array[Int]) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  let floatTypeId = null;
  #|  let doubleTypeId = null;
  #|  const toCell = (value, typeId) => {
  #|    if (value === null || value === undefined) {
  #|      return ["", true];
  #|    }
  #|    if (typeof value === "string") {
  #|      if (typeId !== null && (typeId === floatTypeId || typeId === doubleTypeId)) {
  #|        if (value === "NaN") {
  #|          return ["nan", false];
  #|        }
  #|        if (value === "Infinity") {
  #|          return ["inf", false];
  #|        }
  #|        if (value === "-Infinity") {
  #|          return ["-inf", false];
  #|        }
  #|      }
  #|      return [value, false];
  #|    }
  #|    if (typeof value === "number") {
  #|      if (Number.isNaN(value)) {
  #|        return ["nan", false];
  #|      }
  #|      if (value === Infinity) {
  #|        return ["inf", false];
  #|      }
  #|      if (value === -Infinity) {
  #|        return ["-inf", false];
  #|      }
  #|      return [String(value), false];
  #|    }
  #|    if (typeof value === "boolean" || typeof value === "bigint") {
  #|      return [String(value), false];
  #|    }
  #|    return [JSON.stringify(value), false];
  #|  };
  #|  const pushRow = (values, rows, nulls, typeIds) => {
  #|    const row = [];
  #|    const rowNulls = [];
  #|    for (let i = 0; i < values.length; i++) {
  #|      const cell = toCell(values[i], typeIds ? typeIds[i] : null);
  #|      row.push(cell[0]);
  #|      rowNulls.push(cell[1]);
  #|    }
  #|    rows.push(row);
  #|    nulls.push(rowNulls);
  #|  };
  #|  const run = async () => {
  #|    const backend = stmt && (stmt.backend || (stmt.connection && stmt.connection.kind));
  #|    const inferred = backend || (stmt && stmt.statement && typeof stmt.statement.query === "function" ? "wasm" : null);
  #|    const kind = inferred || (stmt && stmt.statement && typeof stmt.statement.run === "function" ? "node" : null);
  #|    if (kind === "node" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        const api = await import('@duckdb/node-api');
  #|        floatTypeId = api.DuckDBTypeId.FLOAT;
  #|        doubleTypeId = api.DuckDBTypeId.DOUBLE;
  #|        const result = await stmt.statement.run();
  #|        const columns = result.columnNames();
  #|        const rowsJson = await result.getRowsJson();
  #|        const typeIds = columns.map((_, idx) => result.columnTypeId(idx));
  #|        const rows = [];
  #|        const nulls = [];
  #|        for (const row of rowsJson) {
  #|          const values = Array.isArray(row) ? row : columns.map((name) => row[name]);
  #|          pushRow(values, rows, nulls, typeIds);
  #|        }
  #|        on_ok(columns, rows, nulls, typeIds);
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (kind === "wasm" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        // For WASM, use prepared statement with parameters
  #|        const params = [];
  #|        if (stmt.params) {
  #|          const indices = Object.keys(stmt.params).map(Number).sort((a, b) => a - b);
  #|          for (const index of indices) {
  #|            const param = stmt.params[index];
  #|            // Convert typed parameters to values for duckdb-wasm
  #|            if (param.type === "null") {
  #|              params.push(null);
  #|            } else if (param.type === "bigint") {
  #|              params.push(BigInt(param.value));
  #|            } else if (param.type === "date" || param.type === "timestamp") {
  #|              params.push(param.value);  // ISO string format
  #|            } else {
  #|              params.push(param.value);
  #|            }
  #|          }
  #|        }
  #|        const arrowResult = await stmt.statement.query(...params);
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
  #|          pushRow(values, rows, nulls, null);
  #|        }
  #|        const typeIds = [];
  #|        on_ok(columns, rows, nulls, typeIds);
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

///|
extern "js" fn js_execute_prepared_stream(
  stmt : PreparedStatement,
  on_ok : (ResultStream) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  const run = async () => {
  #|    const backend = stmt && (stmt.backend || (stmt.connection && stmt.connection.kind));
  #|    const inferred = backend || (stmt && stmt.statement && typeof stmt.statement.query === "function" ? "wasm" : null);
  #|    const kind = inferred || (stmt && stmt.statement && typeof stmt.statement.run === "function" ? "node" : null);
  #|    if (kind === "node" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        const api = await import('@duckdb/node-api');
  #|        const result = await stmt.statement.stream();
  #|        const columns = result.columnNames();
  #|        const typeIds = columns.map((_, idx) => result.columnTypeId(idx));
  #|        on_ok({
  #|          kind: "node",
  #|          result,
  #|          columns,
  #|          typeIds,
  #|          floatTypeId: api.DuckDBTypeId.FLOAT,
  #|          doubleTypeId: api.DuckDBTypeId.DOUBLE,
  #|        });
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (kind === "wasm" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        const params = [];
  #|        if (stmt.params) {
  #|          const indices = Object.keys(stmt.params).map(Number).sort((a, b) => a - b);
  #|          for (const index of indices) {
  #|            const param = stmt.params[index];
  #|            if (param.type === "null") {
  #|              params.push(null);
  #|            } else if (param.type === "bigint") {
  #|              params.push(BigInt(param.value));
  #|            } else if (param.type === "date" || param.type === "timestamp") {
  #|              params.push(param.value);
  #|            } else {
  #|              params.push(param.value);
  #|            }
  #|          }
  #|        }
  #|        const reader = await stmt.statement.send(...params);
  #|        let columns = [];
  #|        if (reader && reader.schema && reader.schema.fields) {
  #|          columns = reader.schema.fields.map((field) => field.name);
  #|        }
  #|        on_ok({ kind: "wasm", reader, columns, done: false });
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

///|
extern "js" fn js_close_prepared(
  stmt : PreparedStatement,
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(stmt, on_ok, on_err) => {
  #|  const run = async () => {
  #|    const backend = stmt && (stmt.backend || (stmt.connection && stmt.connection.kind));
  #|    const inferred = backend || (stmt && stmt.statement && typeof stmt.statement.query === "function" ? "wasm" : null);
  #|    const kind = inferred || (stmt && stmt.statement && typeof stmt.statement.run === "function" ? "node" : null);
  #|    if (kind === "node" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        // Node-api statements are automatically cleaned up
  #|        on_ok();
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (kind === "wasm" && stmt && stmt.kind === "prepared" && stmt.statement) {
  #|      try {
  #|        if (typeof stmt.statement.close === "function") {
  #|          await stmt.statement.close();
  #|        }
  #|        stmt.params = {};
  #|        on_ok();
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
  match js_bind_int(self, index, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_bigint(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  match js_bind_bigint(self, index, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_double(
  self : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, DuckDBError] {
  match js_bind_double(self, index, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_varchar(
  self : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, DuckDBError] {
  match js_bind_varchar(self, index, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_bool(
  self : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  match js_bind_bool(self, index, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_null(
  self : PreparedStatement,
  index : Int,
) -> Result[Unit, DuckDBError] {
  match js_bind_null(self, index) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::clear_bindings(
  self : PreparedStatement,
) -> Result[Unit, DuckDBError] {
  match js_clear_bindings(self) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
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
  let date_str = "\{year}-\{String::pad_start(month.to_string(), 2, '0')}-\{String::pad_start(day.to_string(), 2, '0')}"
  match js_bind_date(self, index, date_str) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn PreparedStatement::bind_timestamp(
  self : PreparedStatement,
  index : Int,
  micros : Int,
) -> Result[Unit, DuckDBError] {
  // Convert microseconds to ISO timestamp string for JS
  let (year, month, day, hour, minute, second) = timestamp_to_ymd_hms(micros)
  let date_str = "\{year}-\{String::pad_start(month.to_string(), 2, '0')}-\{String::pad_start(day.to_string(), 2, '0')}"
  let time_str = "\{String::pad_start(hour.to_string(), 2, '0')}:\{String::pad_start(minute.to_string(), 2, '0')}:\{String::pad_start(second.to_string(), 2, '0')}"
  let timestamp_str = "\{date_str} \{time_str}"
  match js_bind_timestamp(self, index, timestamp_str) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
}

///|
pub fn Appender::append_date(
  self : Appender,
  days : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = days
  // Appender not supported in JS backend
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
}

///|
pub fn Appender::append_timestamp(
  self : Appender,
  micros : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = micros
  // Appender not supported in JS backend
  Err(DuckDBError::Message("Appender not yet supported for JS backend"))
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
  y / 4 - y / 100 + y / 400
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
    fn(columns, rows, nulls, type_ids) {
      let column_types = column_types_from_ids(columns, type_ids)
      on_done(Ok({ columns, column_types, rows, nulls }))
    },
    fn(message) { on_done(Err(DuckDBError::Message(message))) },
  )
}

///|
pub fn PreparedStatement::execute_stream(
  self : PreparedStatement,
  on_done~ : (Result[ResultStream, DuckDBError]) -> Unit,
) -> Unit {
  js_execute_prepared_stream(self, fn(stream) { on_done(Ok(stream)) }, fn(
    message,
  ) {
    on_done(Err(DuckDBError::Message(message)))
  })
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

///|
extern "js" fn js_config_create() -> Config =
  #|() => {
  #|  return { kind: "config", options: {} };
  #|}

///|
extern "js" fn js_config_set(
  cfg : Config,
  key : String,
  value : String,
) -> Result[Unit, String] =
  #|(cfg, key, value) => {
  #|  try {
  #|    cfg.options[key] = value;
  #|    return { ok: true };
  #|  } catch (e) {
  #|    return { ok: false, error: e.message || String(e) };
  #|  }
  #|}

///|
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
  match js_config_set(self, key, value) {
    Ok(_) => Ok(())
    Err(message) => Err(DuckDBError::Message(message))
  }
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
    Some(cfg) =>
      js_connect_with_config(
        path,
        cfg,
        backend,
        fn(conn) { on_ready(Ok(conn)) },
        fn(message) { on_ready(Err(DuckDBError::Message(message))) },
      )
    None =>
      // Call the original connect function - need to create a wrapper
      js_connect(path, backend, fn(conn) { on_ready(Ok(conn)) }, fn(message) {
        on_ready(Err(DuckDBError::Message(message)))
      })
  }
}

// ============================================================================
// Appender API Implementation (Node backend only)
// ============================================================================

///|
pub fn Connection::create_appender(
  self : Connection,
  schema : String,
  table : String,
  on_done~ : (Result[Appender, DuckDBError]) -> Unit,
) -> Unit {
  // Note: schema parameter is ignored for Node API (uses table name only)
  let _ = schema
  js_create_appender(self, table, fn(a) { on_done(Ok(a)) }, fn(e) {
    on_done(Err(DuckDBError::Message(e)))
  })
}

///|
pub fn Appender::begin_row(self : Appender) -> Result[Unit, DuckDBError] {
  match js_appender_begin_row(self) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_int(
  self : Appender,
  value : Int,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_int(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_bigint(
  self : Appender,
  value : Int,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_bigint(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_double(
  self : Appender,
  value : Double,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_double(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_varchar(
  self : Appender,
  value : String,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_varchar(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_bool(
  self : Appender,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_bool(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_null(self : Appender) -> Result[Unit, DuckDBError] {
  match js_appender_append_null(self) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::end_row(self : Appender) -> Result[Unit, DuckDBError] {
  match js_appender_end_row(self) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::flush(self : Appender) -> Result[Unit, DuckDBError] {
  match js_appender_flush(self) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::close(
  self : Appender,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  js_appender_close(self, fn() { on_done(Ok(())) }, fn(e) {
    on_done(Err(DuckDBError::Message(e)))
  })
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
  match js_bind_blob(self, index, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_blob(
  self : Appender,
  value : Bytes,
) -> Result[Unit, DuckDBError] {
  match js_appender_append_blob(self, value) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
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
  // For JS Node backend, convert to BigInt from lower/upper
  // Note: This loses true 128-bit precision, but provides best effort
  let bigint_value = if value.upper < 0 {
    // Negative: construct two's complement
    BigInt::from_int(value.lower) - (BigInt::from_int(1) << 64)
  } else {
    // Positive or zero
    BigInt::from_int(value.lower) + (BigInt::from_int(value.upper) << 64)
  }
  // Use bind_decimal with the BigInt value
  match
    js_bind_decimal(
      self,
      index,
      value.width,
      value.scale,
      bigint_value.to_int(),
    ) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_decimal(
  self : Appender,
  value : Decimal,
) -> Result[Unit, DuckDBError] {
  // For JS Node backend, convert to BigInt from lower/upper
  let bigint_value = if value.upper < 0 {
    // Negative: construct two's complement
    BigInt::from_int(value.lower) - (BigInt::from_int(1) << 64)
  } else {
    // Positive or zero
    BigInt::from_int(value.lower) + (BigInt::from_int(value.upper) << 64)
  }
  match
    js_appender_append_decimal(
      self,
      value.width,
      value.scale,
      bigint_value.to_int(),
    ) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn decimal_from_double(value : Double, width : Int, scale : Int) -> Decimal {
  let multiplier = int_pow10(scale)
  let scaled = (value * Double::from_int(multiplier)).to_int()
  let upper = if scaled >= 0 { 0 } else { -1 }
  { width, scale, lower: scaled, upper }
}

///|
pub fn decimal_to_double(decimal : Decimal) -> Double {
  let divisor = Double::from_int(int_pow10(decimal.scale))
  Double::from_int(decimal.lower) / divisor
}

///|
pub fn decimal_from_parts(
  whole : Int,
  fractional : Int,
  scale : Int,
) -> Decimal {
  let value = whole * int_pow10(scale) + fractional
  let width = if whole == 0 {
    String::length(fractional.to_string())
  } else {
    String::length(whole.to_string()) + scale
  }
  let upper = if value >= 0 { 0 } else { -1 }
  { width: width.max(1), scale, lower: value, upper }
}

///|
pub fn decimal_to_parts(decimal : Decimal) -> (Int, Int) {
  let divisor = int_pow10(decimal.scale)
  let whole = decimal.lower / divisor
  let fractional = Int::abs(decimal.lower % divisor)
  (whole, fractional)
}

///|
/// Create a decimal from 128-bit parts.
pub fn decimal_from_hugeint(
  lower : Int,
  upper : Int,
  width : Int,
  scale : Int,
) -> Decimal {
  { width, scale, lower, upper }
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
  match js_bind_interval(self, index, value.months, value.days, value.micros) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn Appender::append_interval(
  self : Appender,
  value : Interval,
) -> Result[Unit, DuckDBError] {
  match
    js_appender_append_interval(self, value.months, value.days, value.micros) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
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
  match js_bind_list_varchar(self, index, values) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn list_from_strings(elements : Array[String]) -> List {
  { elements, }
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
  match js_bind_struct(self, index, value.fields, value.values) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
}

///|
pub fn struct_from_arrays(
  fields : Array[String],
  values : Array[String],
) -> Struct {
  { fields, values }
}

///|
pub fn struct_from_pairs(pairs : Array[(String, String)]) -> Struct {
  let fields = pairs.map(fn(p) { p.0 })
  let values = pairs.map(fn(p) { p.1 })
  { fields, values }
}

// Helper function to find index in array

///|
fn array_index_of(arr : Array[String], val : String) -> Int? {
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
    Some(i) => if i < s.values.length() { Some(s.values[i]) } else { None }
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
  match js_bind_map(self, index, map.keys, map.values) {
    Ok(_) => Ok(())
    Err(e) => Err(DuckDBError::Message(e))
  }
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
    Some(i) => if i < m.values.length() { Some(m.values[i]) } else { None }
    None => None
  }
}

///|
pub fn map_size(m : Map) -> Int {
  m.keys.length()
}

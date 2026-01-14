///|
#external
pub type ArrowResult

// ============================================================================
// Arrow FFI Declarations (JavaScript Backend)
// ============================================================================

///|
extern "js" fn js_query_arrow(
  conn : Connection,
  sql : String,
  on_ok : (ArrowResult) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (conn && conn.kind === "wasm") {
  #|      try {
  #|        const arrowResult = await conn.conn.query(sql, { returnType: "arrow" });
  #|        on_ok({ kind: "arrow", result: arrowResult });
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (conn && conn.kind === "node") {
  #|      // Node API - Arrow support
  #|      try {
  #|        const result = await conn.connection.run(sql);
  #|        if (result && result.arrow) {
  #|          on_ok({ kind: "arrow", result: result.arrow() });
  #|          return;
  #|        }
  #|        // Fallback to regular result
  #|        on_ok({ kind: "arrow", result: null });
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    on_err("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

///|
extern "js" fn js_arrow_column_count(result : ArrowResult) -> Int =
  #|(result) => {
  #|  if (result && result.kind === "arrow" && result.result) {
  #|    const arrow = result.result;
  #|    if (arrow.numCols) {
  #|      return arrow.numCols;
  #|    }
  #|    if (arrow.schema && arrow.schema.fields) {
  #|      return arrow.schema.fields.length;
  #|    }
  #|  }
  #|  return 0;
  #|}

///|
extern "js" fn js_arrow_row_count(result : ArrowResult) -> Int =
  #|(result) => {
  #|  if (result && result.kind === "arrow" && result.result) {
  #|    const arrow = result.result;
  #|    if (arrow.numRows !== undefined) {
  #|      return arrow.numRows;
  #|    }
  #|    if (arrow.length !== undefined) {
  #|      return arrow.length;
  #|    }
  #|  }
  #|  return 0;
  #|}

///|
extern "js" fn js_arrow_destroy(result : ArrowResult) -> Unit =
  #|(result) => {
  #|  // Arrow result will be garbage collected
  #|  if (result && result.result && result.result.close) {
  #|    result.result.close();
  #|  }
  #|}

// ============================================================================
// Public API (JavaScript Backend)
// ============================================================================

///|
pub struct ArrowField {
  name : String
  nullable : Bool
  type_id : String
}

///|
pub struct ArrowSchemaInfo {
  fields : Array[ArrowField]
}

///|
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  js_query_arrow(self, sql,
    fn(result) { on_done(Ok(result)) },
    fn(err) { on_done(Err(DuckDBError::Message(err))) }
  )
}

///|
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  js_arrow_column_count(self)
}

///|
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  js_arrow_row_count(self)
}

///|
pub fn ArrowResult::get_schema(self : ArrowResult) -> Result[ArrowSchemaInfo, DuckDBError] {
  // For JS backend, schema extraction requires Arrow schema traversal
  Ok(ArrowSchemaInfo::{ fields: [] })
}

///|
pub fn ArrowResult::get_column_int32(self : ArrowResult, col : Int) -> Array[Int] {
  // TODO: Extract from Arrow data
  []
}

///|
pub fn ArrowResult::get_column_int64(self : ArrowResult, col : Int) -> Array[Int] {
  []
}

///|
pub fn ArrowResult::get_column_double(self : ArrowResult, col : Int) -> Array[Double] {
  []
}

///|
pub fn ArrowResult::get_column_string(self : ArrowResult, col : Int) -> Array[String] {
  []
}

///|
pub fn ArrowResult::get_column_bool(self : ArrowResult, col : Int) -> Array[Bool] {
  []
}

///|
pub fn ArrowResult::close(self : ArrowResult, on_done~ : (Result[Unit, DuckDBError]) -> Unit) -> Unit {
  js_arrow_destroy(self)
  on_done(Ok(()))
}

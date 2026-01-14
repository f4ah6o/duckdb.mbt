///|
#external
pub type ArrowResult

// ============================================================================
// Arrow FFI Declarations
// ============================================================================

///|
#borrow(conn, sql)
extern "C" fn native_query_arrow(conn : Connection, sql : Bytes) -> ArrowResult = "duckdb_mb_query_arrow"

///|
#borrow(result)
extern "C" fn native_arrow_destroy(result : ArrowResult) = "duckdb_mb_arrow_destroy"

///|
#borrow(result)
extern "C" fn native_arrow_column_count(result : ArrowResult) -> Int = "duckdb_mb_arrow_column_count"

///|
#borrow(result)
extern "C" fn native_arrow_row_count(result : ArrowResult) -> Int = "duckdb_mb_arrow_row_count"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int32(result : ArrowResult, col : Int) -> Bytes = "duckdb_mb_arrow_get_column_int32"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int64(result : ArrowResult, col : Int) -> Bytes = "duckdb_mb_arrow_get_column_int64"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_double(result : ArrowResult, col : Int) -> Bytes = "duckdb_mb_arrow_get_column_double"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_string(result : ArrowResult, col : Int) -> Bytes = "duckdb_mb_arrow_get_column_string"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_bool(result : ArrowResult, col : Int) -> Bytes = "duckdb_mb_arrow_get_column_bool"

///|
extern "C" fn native_is_null_arrow_result(result : ArrowResult) -> Bool = "duckdb_mb_is_null_arrow_result"

// ============================================================================
// Public API
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
  let sql_bytes = @encoding/utf8.encode(sql)
  let result = native_query_arrow(self, sql_bytes)
  if native_is_null_arrow_result(result) {
    on_done(Err(DuckDBError::Message(last_error("arrow query failed"))))
  } else {
    on_done(Ok(result))
  }
}

///|
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  native_arrow_column_count(self)
}

///|
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  native_arrow_row_count(self)
}

///|
pub fn ArrowResult::get_schema(self : ArrowResult) -> Result[ArrowSchemaInfo, DuckDBError] {
  // Parse schema from Arrow result
  // For now, return empty schema
  Ok(ArrowSchemaInfo::{ fields: [] })
}

///|
pub fn ArrowResult::get_column_int32(self : ArrowResult, col : Int) -> Array[Int] {
  let _data = native_arrow_get_column_int32(self, col)
  // TODO: Decode packed int32 array from bytes
  []
}

///|
pub fn ArrowResult::get_column_int64(self : ArrowResult, col : Int) -> Array[Int] {
  let _data = native_arrow_get_column_int64(self, col)
  []
}

///|
pub fn ArrowResult::get_column_double(self : ArrowResult, col : Int) -> Array[Double] {
  let _data = native_arrow_get_column_double(self, col)
  []
}

///|
pub fn ArrowResult::get_column_string(self : ArrowResult, col : Int) -> Array[String] {
  let _data = native_arrow_get_column_string(self, col)
  []
}

///|
pub fn ArrowResult::get_column_bool(self : ArrowResult, col : Int) -> Array[Bool] {
  let _data = native_arrow_get_column_bool(self, col)
  []
}

///|
pub fn ArrowResult::close(self : ArrowResult, on_done~ : (Result[Unit, DuckDBError]) -> Unit) -> Unit {
  native_arrow_destroy(self)
  on_done(Ok(()))
}

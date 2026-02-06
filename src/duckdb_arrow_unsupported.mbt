///|
#external
pub type ArrowResult

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

// ============================================================================
// Arrow Stubs (for unsupported targets)
// ============================================================================

///|
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("arrow integration is not available for this target"),
    ),
  )
}

///|
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  let _ = self
  0
}

///|
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  let _ = self
  0
}

///|
pub fn ArrowResult::get_schema(
  self : ArrowResult,
) -> Result[ArrowSchemaInfo, DuckDBError] {
  let _ = self
  let _ = ArrowSchemaInfo::{
    fields: [
      ArrowField::{ name: "", nullable: false, type_id: "" },
    ],
  }
  Err(
    DuckDBError::Message("arrow integration is not available for this target"),
  )
}

///|
pub fn ArrowResult::get_column_int32(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let _ = self
  let _ = col
  []
}

///|
pub fn ArrowResult::get_column_int64(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let _ = self
  let _ = col
  []
}

///|
pub fn ArrowResult::get_column_double(
  self : ArrowResult,
  col : Int,
) -> Array[Double] {
  let _ = self
  let _ = col
  []
}

///|
pub fn ArrowResult::get_column_string(
  self : ArrowResult,
  col : Int,
) -> Array[String] {
  let _ = self
  let _ = col
  []
}

///|
pub fn ArrowResult::get_column_bool(
  self : ArrowResult,
  col : Int,
) -> Array[Bool] {
  let _ = self
  let _ = col
  []
}

///|
pub fn ArrowResult::close(
  self : ArrowResult,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("arrow integration is not available for this target"),
    ),
  )
}

///|
#external
pub type Connection

///|
#external
pub type PreparedStatement

///|
#external
pub type Config

///|
#external
pub type Appender

///|
/// JS backend selection for `connect`.
pub(all) enum JsBackend {
  Auto
  Node
  Wasm
}

///|
/// DuckDB error wrapper.
pub suberror DuckDBError {
  Message(String)
}

///|
/// Query result data is represented as strings plus a null mask.
pub struct QueryResult {
  columns : Array[String]
  rows : Array[Array[String]]
  nulls : Array[Array[Bool]]
}

///|
// Ensure public constructors are referenced in all target builds.
fn touch_public_types(backend : JsBackend) -> Unit {
  let _ = backend
  let _ = JsBackend::Node
  let _ = JsBackend::Wasm
  let _ : QueryResult = { columns: [], rows: [], nulls: [] }
  let _ : Decimal = { width: 0, scale: 0, value: 0 }
  let _ : Interval = { months: 0, days: 0, micros: 0 }
  let _ : List = { elements: [] }
  let _ : Struct = { fields: [], values: [] }
  let _ : Map = { keys: [], values: [] }
}

// ============================================================================
// Advanced Data Types
// ============================================================================

///|
/// Fixed-point decimal type for financial calculations.
/// Uses 128-bit integer representation internally.
pub struct Decimal {
  width : Int  // Total number of digits
  scale : Int  // Digits after decimal point
  value : Int  // Scaled integer value (e.g., 123.45 with scale 2 stores as 12345)
}

///|
/// Date/time interval type.
/// Represents a span of time in months, days, and microseconds.
pub struct Interval {
  months : Int    // Months
  days : Int      // Days
  micros : Int    // Microseconds
}

///|
/// List/array type. Elements are stored as strings and converted on demand.
pub struct List {
  elements : Array[String]
}

///|
/// Composite type with named fields.
/// Represents a DuckDB STRUCT type.
pub struct Struct {
  fields : Array[String]  // Field names
  values : Array[String]  // Field values as strings
}

///|
/// Key-value pair map type.
/// DuckDB maps are implemented as lists of key-value structs.
pub struct Map {
  keys : Array[String]
  values : Array[String]
}

///|
pub fn QueryResult::row_count(self : QueryResult) -> Int {
  self.rows.length()
}

///|
pub fn QueryResult::column_count(self : QueryResult) -> Int {
  self.columns.length()
}

///|
pub fn QueryResult::cell(self : QueryResult, row : Int, col : Int) -> String? {
  if self.nulls[row][col] {
    None
  } else {
    Some(self.rows[row][col])
  }
}


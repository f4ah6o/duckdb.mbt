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
#external
pub type ResultStream

///|
#external
pub type LogicalType

///|
#external
pub type Vector

///|
#external
pub type NativeDataChunk

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
  column_types : Array[ColumnType]
  rows : Array[Array[String]]
  nulls : Array[Array[Bool]]
}

///|
/// Chunked query data with column metadata.
pub struct DataChunk {
  columns : Array[String]
  rows : Array[Array[String]]
  nulls : Array[Array[Bool]]
}

///|
/// DuckDB column type identifiers.
pub enum ColumnType {
  Invalid
  Boolean
  TinyInt
  SmallInt
  Integer
  BigInt
  UTinyInt
  USmallInt
  UInteger
  UBigInt
  Float
  Double
  Timestamp
  Date
  Time
  Interval
  HugeInt
  UHugeInt
  Varchar
  Blob
  Decimal
  TimestampS
  TimestampMs
  TimestampNs
  Enum
  List
  Struct
  Map
  Array
  Uuid
  Union
  Bit
  TimeTz
  TimestampTz
  Any
  Bignum
  SqlNull
  StringLiteral
  IntegerLiteral
  TimeNs
  Unknown(Int)
}

///|
// Ensure public constructors are referenced in all target builds.
fn touch_public_types(backend : JsBackend) -> Unit {
  let _ = backend
  let _ = JsBackend::Node
  let _ = JsBackend::Wasm
  let _ : QueryResult = { columns: [], column_types: [], rows: [], nulls: [] }
  let _ : DataChunk = { columns: [], rows: [], nulls: [] }
  let _ : Decimal = { width: 0, scale: 0, lower: 0, upper: 0 }
  let _ : Interval = { months: 0, days: 0, micros: 0 }
  let _ : List = { elements: [] }
  let _ : Struct = { fields: [], values: [] }
  let _ : Map = { keys: [], values: [] }
  let _ : ColumnType = ColumnType::Unknown(-1)
  let _ = column_type_from_id(0)
  // Typed result API types
  let _ : Value = Value::Decimal({ width: 0, scale: 0, lower: 0, upper: 0 })
  let _ : Value = Value::Blob(Bytes::default())
  let _ : Value = Value::Null
  let _ : TypedQueryResult = { columns: [], data: [] }

}

// ============================================================================
// Advanced Data Types
// ============================================================================

///|
/// Fixed-point decimal type for financial calculations.
/// Uses 128-bit integer representation (lower/upper parts).
pub struct Decimal {
  width : Int // Total number of digits
  scale : Int // Digits after decimal point
  lower : Int // Lower 64 bits of the 128-bit value
  upper : Int // Upper 64 bits (sign extension for negative values)
}

///|
/// Date/time interval type.
/// Represents a span of time in months, days, and microseconds.
pub struct Interval {
  months : Int // Months
  days : Int // Days
  micros : Int64 // Microseconds (Int64 to avoid overflow)
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
  fields : Array[String] // Field names
  values : Array[String] // Field values as strings
}

///|
/// Key-value pair map type.
/// DuckDB maps are implemented as lists of key-value structs.
pub struct Map {
  keys : Array[String]
  values : Array[String]
}

// ============================================================================
// Typed Result API
// ============================================================================

///|
/// Typed value representing a single DuckDB cell.
pub enum Value {
  Int(Int)
  Double(Double)
  Bool(Bool)
  String(String)
  Date(Int) // Days since 1970-01-01
  Timestamp(Int64) // Microseconds since 1970-01-01
  Decimal(Decimal)
  Blob(Bytes)
  Null
}

///|
/// Type-safe query result with typed value access.
/// Stores data column-wise for efficient columnar access.
pub struct TypedQueryResult {
  columns : Array[String]
  data : Array[Array[Value]] // Column-major storage
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

///|
pub fn DataChunk::row_count(self : DataChunk) -> Int {
  self.rows.length()
}

///|
pub fn DataChunk::column_count(self : DataChunk) -> Int {
  self.columns.length()
}

///|
pub fn DataChunk::cell(self : DataChunk, row : Int, col : Int) -> String? {
  if self.nulls[row][col] {
    None
  } else {
    Some(self.rows[row][col])
  }
}

// ============================================================================
// QueryResult Direct Typed Access
// ============================================================================

///|
/// Get the typed value at the specified row and column.
/// Returns the Value directly without requiring to_typed() conversion.
pub fn QueryResult::get_value(
  self : QueryResult,
  row : Int,
  col : Int,
) -> Value? {
  if row < 0 ||
    row >= self.rows.length() ||
    col < 0 ||
    col >= self.columns.length() {
    None
  } else if self.nulls[row][col] {
    Some(Value::Null)
  } else {
    let column_type = if col < self.column_types.length() {
      self.column_types[col]
    } else {
      ColumnType::Unknown(-1)
    }
    Some(parse_value_with_type(self.rows[row][col], column_type))
  }
}

///|
/// Get the integer value at the specified row and column.
/// Returns None if the value is null or not an integer.
pub fn QueryResult::get_int(self : QueryResult, row : Int, col : Int) -> Int? {
  match self.get_value(row, col) {
    Some(Int(n)) => Some(n)
    _ => None
  }
}

///|
/// Get the double value at the specified row and column.
/// Returns None if the value is null or not a double.
pub fn QueryResult::get_double(
  self : QueryResult,
  row : Int,
  col : Int,
) -> Double? {
  match self.get_value(row, col) {
    Some(Double(d)) => Some(d)
    _ => None
  }
}

///|
/// Get the boolean value at the specified row and column.
/// Returns None if the value is null or not a boolean.
pub fn QueryResult::get_bool(self : QueryResult, row : Int, col : Int) -> Bool? {
  match self.get_value(row, col) {
    Some(Bool(b)) => Some(b)
    _ => None
  }
}

///|
/// Get the string value at the specified row and column.
/// Returns None if the value is null.
pub fn QueryResult::get_string(
  self : QueryResult,
  row : Int,
  col : Int,
) -> String? {
  match self.get_value(row, col) {
    Some(String(s)) => Some(s)
    Some(Value::Null) => None
    Some(other) => Some(other.to_string())
    None => None
  }
}

///|
/// Get the date value at the specified row and column.
/// Returns None if the value is null or not a date.
pub fn QueryResult::get_date(self : QueryResult, row : Int, col : Int) -> Int? {
  match self.get_value(row, col) {
    Some(Date(d)) => Some(d)
    _ => None
  }
}

///|
/// Get the timestamp value at the specified row and column.
/// Returns None if the value is null or not a timestamp.
pub fn QueryResult::get_timestamp(
  self : QueryResult,
  row : Int,
  col : Int,
) -> Int64? {
  match self.get_value(row, col) {
    Some(Timestamp(t)) => Some(t)
    _ => None
  }
}

///|
/// Get the decimal value at the specified row and column.
/// Returns None if the value is null or not a decimal.
pub fn QueryResult::get_decimal(
  self : QueryResult,
  row : Int,
  col : Int,
) -> Decimal? {
  match self.get_value(row, col) {
    Some(Decimal(d)) => Some(d)
    _ => None
  }
}

///|
/// Get the blob value at the specified row and column.
/// Returns None if the value is null or not a blob.
pub fn QueryResult::get_blob(
  self : QueryResult,
  row : Int,
  col : Int,
) -> Bytes? {
  match self.get_value(row, col) {
    Some(Blob(b)) => Some(b)
    _ => None
  }
}

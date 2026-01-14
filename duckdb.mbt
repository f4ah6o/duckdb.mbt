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

// ============================================================================
// Typed Value API
// ============================================================================

///|
/// Represents a typed DuckDB value.
pub enum Value {
  Null
  IntValue(Int)
  DoubleValue(Double)
  BoolValue(Bool)
  StringValue(String)
  DateValue(Int)        // days since 1970-01-01
  TimestampValue(Int)   // microseconds since 1970-01-01
  DecimalValue(Decimal)
  BlobValue(Bytes)
  ListValue(List)
  StructValue(Struct)
  MapValue(Map)
  IntervalValue(Interval)
}

///|
/// Get the integer value if present, None otherwise.
pub fn Value::as_int(self : Value) -> Int? {
  match self {
    IntValue(n) => Some(n)
    _ => None
  }
}

///|
/// Get the double value if present, None otherwise.
pub fn Value::as_double(self : Value) -> Double? {
  match self {
    DoubleValue(d) => Some(d)
    _ => None
  }
}

///|
/// Get the boolean value if present, None otherwise.
pub fn Value::as_bool(self : Value) -> Bool? {
  match self {
    BoolValue(b) => Some(b)
    _ => None
  }
}

///|
/// Get the string value if present, None otherwise.
pub fn Value::as_string(self : Value) -> String? {
  match self {
    StringValue(s) => Some(s)
    _ => None
  }
}

///|
/// Get the date value if present, None otherwise.
pub fn Value::as_date(self : Value) -> Int? {
  match self {
    DateValue(d) => Some(d)
    _ => None
  }
}

///|
/// Get the timestamp value if present, None otherwise.
pub fn Value::as_timestamp(self : Value) -> Int? {
  match self {
    TimestampValue(t) => Some(t)
    _ => None
  }
}

///|
/// Get the decimal value if present, None otherwise.
pub fn Value::as_decimal(self : Value) -> Decimal? {
  match self {
    DecimalValue(d) => Some(d)
    _ => None
  }
}

///|
/// Check if the value is null.
pub fn Value::is_null(self : Value) -> Bool {
  match self {
    Null => true
    _ => false
  }
}

///|
/// Get the type name of this value.
pub fn Value::type_name(self : Value) -> String {
  match self {
    Null => "null"
    IntValue(_) => "int"
    DoubleValue(_) => "double"
    BoolValue(_) => "bool"
    StringValue(_) => "string"
    DateValue(_) => "date"
    TimestampValue(_) => "timestamp"
    DecimalValue(_) => "decimal"
    BlobValue(_) => "blob"
    ListValue(_) => "list"
    StructValue(_) => "struct"
    MapValue(_) => "map"
    IntervalValue(_) => "interval"
  }
}

// ============================================================================
// Type Inference and Parsing
// ============================================================================

///|
/// Infer the type from a string value and convert to a Value.
fn infer_value(raw : String) -> Value {
  // Try boolean first
  if raw == "true" { Value::BoolValue(true) }
  else if raw == "false" { Value::BoolValue(false) }
  // Try double (must check before int since "1.0" contains digits)
  else if is_double_format(raw) {
    try_parse_double(raw)
  }
  // Try integer
  else if is_int_format(raw) {
    try_parse_int(raw)
  }
  // Default to string
  else { Value::StringValue(raw) }
}

///|
/// Try to parse a string as an integer, returning StringValue on failure.
fn try_parse_int(s : String) -> Value {
  try {
    let n = @strconv.parse_int(s)
    Value::IntValue(n)
  } catch {
    _ => Value::StringValue(s)
  }
}

///|
/// Try to parse a string as a double, returning StringValue on failure.
fn try_parse_double(s : String) -> Value {
  try {
    let d = @strconv.parse_double(s)
    Value::DoubleValue(d)
  } catch {
    _ => Value::StringValue(s)
  }
}

///|
/// Check if a string represents an integer value.
fn is_int_format(s : String) -> Bool {
  if s.length() == 0 { false }
  else {
    let mut i = 0
    // Allow leading sign
    let first = String::unsafe_char_at(s, 0)
    if (first == '+' || first == '-') {
      i = 1
    }
    // Need at least one digit
    if i >= s.length() { false }
    else {
      // Check all remaining characters are digits
      while i < s.length() {
        let c = String::unsafe_char_at(s, i)
        if !is_digit(c) { return false }
        i = i + 1
      }
      true
    }
  }
}

///|
/// Check if a character is a digit.
fn is_digit(c : Char) -> Bool {
  c >= '0' && c <= '9'
}

///|
/// Check if a string represents a double value.
fn is_double_format(s : String) -> Bool {
  String::contains(s, ".") || String::contains(s, "e") || String::contains(s, "E") ||
  s == "nan" || s == "NaN" ||
  s == "inf" || s == "Inf" || s == "-inf" || s == "-Inf"
}

// ============================================================================
// QueryResult Typed API
// ============================================================================

///|
/// Get the typed value at the specified row and column.
/// Uses type inference for non-null values.
pub fn QueryResult::cell_value(self : QueryResult, row : Int, col : Int) -> Value {
  if row < 0 || row >= self.rows.length() {
    Value::Null
  } else if col < 0 || col >= self.columns.length() {
    Value::Null
  } else {
    let is_null = self.nulls[row][col]
    let raw = self.rows[row][col]
    if is_null {
      Value::Null
    } else {
      infer_value(raw)
    }
  }
}

///|
/// Get the integer value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as an integer.
pub fn QueryResult::cell_int(self : QueryResult, row : Int, col : Int) -> Int? {
  match self.cell_value(row, col) {
    Value::IntValue(n) => Some(n)
    _ => None
  }
}

///|
/// Get the double value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as a double.
pub fn QueryResult::cell_double(self : QueryResult, row : Int, col : Int) -> Double? {
  match self.cell_value(row, col) {
    Value::DoubleValue(d) => Some(d)
    _ => None
  }
}

///|
/// Get the boolean value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as a boolean.
pub fn QueryResult::cell_bool(self : QueryResult, row : Int, col : Int) -> Bool? {
  match self.cell_value(row, col) {
    Value::BoolValue(b) => Some(b)
    _ => None
  }
}

///|
/// Get the string value at the specified row and column.
/// Returns None if the value is null.
pub fn QueryResult::cell_string(self : QueryResult, row : Int, col : Int) -> String? {
  match self.cell_value(row, col) {
    Value::StringValue(s) => Some(s)
    Value::Null => None
    other => Some(value_to_string(other))
  }
}

///|
/// Convert a Value to its string representation.
fn value_to_string(v : Value) -> String {
  match v {
    Null => ""
    IntValue(n) => n.to_string()
    DoubleValue(d) => d.to_string()
    BoolValue(b) => if b { "true" } else { "false" }
    StringValue(s) => s
    DateValue(_) => "<date>"
    TimestampValue(_) => "<timestamp>"
    DecimalValue(_) => "<decimal>"
    BlobValue(_) => "<blob>"
    ListValue(_) => "<list>"
    StructValue(_) => "<struct>"
    MapValue(_) => "<map>"
    IntervalValue(_) => "<interval>"
  }
}

///|
/// Get the date value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as a date.
pub fn QueryResult::cell_date(self : QueryResult, row : Int, col : Int) -> Int? {
  match self.cell_value(row, col) {
    Value::DateValue(d) => Some(d)
    _ => None
  }
}

///|
/// Get the timestamp value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as a timestamp.
pub fn QueryResult::cell_timestamp(self : QueryResult, row : Int, col : Int) -> Int? {
  match self.cell_value(row, col) {
    Value::TimestampValue(t) => Some(t)
    _ => None
  }
}

///|
/// Get the decimal value at the specified row and column.
/// Returns None if the value is null or cannot be parsed as a decimal.
pub fn QueryResult::cell_decimal(self : QueryResult, row : Int, col : Int) -> Decimal? {
  match self.cell_value(row, col) {
    Value::DecimalValue(d) => Some(d)
    _ => None
  }
}


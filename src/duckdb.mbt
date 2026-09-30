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
/// A borrowed DuckDB logical type handle (native backend only).
#external
pub type NativeLogicalType

///|
/// A borrowed DuckDB vector handle (native backend only). Only valid while
/// the chunk it was taken from is alive; the canonical, owned column
/// representation is `Vector`.
#external
pub type NativeVector

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
/// Structured error raised by the duckdb bindings.
///
/// Match on the variants to branch on the error category without parsing
/// strings. Every variant keeps the original diagnostic text available via
/// `DuckDBError::message`.
///
/// Categories:
/// - `Query` / `Prepare` / `Bind` / `Append`: the binding detected a failure
///   at that stage and the DuckDB engine did not supply a classified error
///   type (allocation failures, invalid handles, host runtime errors, ...).
/// - `DuckDB`: a failure reported by the DuckDB engine itself; `error_type`
///   is the engine's own classification normalized to snake_case
///   ("parser", "catalog", "invalid_input", ...) or "unknown".
/// - `Backend`: a backend/runtime failure outside the query path (connect,
///   close, configuration, host exceptions).
/// - `Unsupported`: the feature exists but this backend cannot provide it.
///   `backend` names the backend ("native", "node", "wasm", "unsupported");
///   designed to compose with the BackendCapabilities work in issue #65.
/// - `Closed`: the operation was attempted on a closed handle.
/// - `InvalidArgument`: a caller-supplied argument failed validation.
pub suberror DuckDBError {
  /// Query/streaming stage failure without an engine classification.
  Query(String)
  /// Prepare stage failure without an engine classification.
  Prepare(String)
  /// Bind stage failure without an engine classification.
  Bind(String)
  /// Appender stage failure without an engine classification.
  Append(String)
  /// `feature` is not supported by `backend`.
  Unsupported(feature~ : String, backend~ : String)
  /// The operation was attempted on a closed `resource`
  /// ("connection", "statement", "stream", "appender").
  Closed(resource~ : String)
  /// `argument` failed validation (`reason` explains why).
  InvalidArgument(argument~ : String, reason~ : String)
  /// Backend/runtime failure that did not come from the DuckDB engine.
  Backend(backend~ : String, message~ : String)
  /// Error reported by the DuckDB engine itself. `error_type` is the
  /// engine's classification in snake_case or "unknown"; `message` is the
  /// full original diagnostic text.
  DuckDB(error_type~ : String, message~ : String)
}

///|
/// The diagnostic message carried by this error.
///
/// For errors reported by DuckDB or a backend this is the original text
/// verbatim; for the binding-side categories (`Unsupported`, `Closed`,
/// `InvalidArgument`) it is synthesized from the structured fields.
///
/// Migration note: this replaces matching on the removed
/// `DuckDBError::Message(String)` variant.
pub fn DuckDBError::message(self : DuckDBError) -> String {
  match self {
    Query(msg) | Prepare(msg) | Bind(msg) | Append(msg) => msg
    Backend(message~, ..) | DuckDB(message~, ..) => message
    Unsupported(feature~, backend~) =>
      "\{feature} is not supported on the \{backend} backend"
    Closed(resource~) => "\{resource} is closed"
    InvalidArgument(argument~, reason~) =>
      "invalid argument \{argument}: \{reason}"
  }
}

///|
/// The DuckDB engine's error classification when one is available (the
/// `DuckDB` variant), e.g. "parser", "catalog", "invalid_input" or
/// "unknown". Returns `None` for binding-side categories.
pub fn DuckDBError::error_type(self : DuckDBError) -> String? {
  match self {
    DuckDB(error_type~, ..) => Some(error_type)
    _ => None
  }
}

///|
/// Sentinel prefix emitted by the JS/WASM FFI shims when an operation is
/// attempted on a closed handle. Not part of the public API.
let duckdb_error_closed_sentinel = "ERR_CLOSED:"

///|
/// Normalize a DuckDB engine error type name ("Parser Error", "Invalid
/// Input Error", ...) to snake_case without the "error" suffix
/// ("parser", "invalid_input", ...).
fn normalize_error_type(name : String) -> String {
  let sb = StringBuilder::new()
  for c in name {
    if c == ' ' || c == '-' {
      ignore(sb.write_char('_'))
    } else {
      ignore(sb.write_char(c.to_ascii_lowercase()))
    }
  }
  let normalized = sb.to_string()
  if normalized.has_suffix("_error") {
    normalized[:normalized.length() - 6].to_owned()
  } else {
    normalized
  }
}

///|
/// Extract the engine error type from a diagnostic of the form
/// `"<Words> Error: <detail>"` — the prefix DuckDB puts on user-facing
/// errors. Returns `Some` normalized type, `Some("unknown")` for a bare
/// "Error:" prefix, or `None` when the message is not engine-classified.
fn duckdb_error_type_from_message(message : String) -> String? {
  let mut colon = -1
  for i, c in message {
    if c == ':' {
      colon = i
      break
    }
    if i >= 64 {
      return None
    }
  }
  if colon <= 0 {
    return None
  }
  let prefix = message[:colon].trim().to_owned()
  if prefix == "Error" {
    return Some("unknown")
  }
  if !prefix.has_suffix(" Error") {
    return None
  }
  let name = prefix[:prefix.length() - 6].to_owned()
  if name.length() == 0 {
    return Some("unknown")
  }
  for c in name {
    if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == ' ') {
      return None
    }
  }
  Some(normalize_error_type(name))
}

///|
/// Split "<feature> is (not yet )?supported for <backend> backend" style
/// binding-layer diagnostics into a `Unsupported` variant. Returns `None`
/// for messages that are not unsupported-feature reports.
fn duckdb_unsupported_from_message(message : String) -> DuckDBError? {
  if message.contains("requires Worker support") {
    return Some(Unsupported(feature="duckdb-wasm", backend="wasm"))
  }
  if message.contains("unsupported column type") {
    return Some(
      Unsupported(
        feature="streamed query results with this column type",
        backend="native",
      ),
    )
  }
  let wasm_no_support = "WASM backend does not support "
  if message.has_prefix(wasm_no_support) {
    let feature = message[wasm_no_support.length():]
      .trim()
      .to_owned()
      .to_lower()
    if feature.length() > 0 {
      return Some(Unsupported(feature~, backend="wasm"))
    }
    return None
  }
  if message.has_prefix("Appender is not supported for WASM backend") {
    return Some(Unsupported(feature="appender", backend="wasm"))
  }
  if message.has_prefix("Appender not yet supported for JS backend") {
    return Some(Unsupported(feature="appender", backend="js"))
  }
  None
}

///|
/// Classify a backend diagnostic message into a structured `DuckDBError`:
///
/// - `ERR_CLOSED:<resource>` sentinel → `Closed`
/// - known unsupported-feature diagnostics → `Unsupported`
/// - `"<Words> Error: ..."` engine messages → `DuckDB` (engine error type)
/// - anything else → `fallback(message)` (usually the stage variant for the
///   call site, or `Backend` outside the query path)
fn classify_duckdb_error(
  message : String,
  fallback~ : (String) -> DuckDBError,
) -> DuckDBError {
  if message.has_prefix(duckdb_error_closed_sentinel) {
    let resource = message[duckdb_error_closed_sentinel.length():]
      .trim()
      .to_owned()
    return if resource.length() > 0 {
      Closed(resource~)
    } else {
      Closed(resource="handle")
    }
  }
  // Native stub sentinels for null/invalid handles.
  match message {
    "connection is null" => return Closed(resource="connection")
    "statement is null" => return Closed(resource="statement")
    "stream is null" => return Closed(resource="stream")
    "appender is null" => return Closed(resource="appender")
    _ => ()
  }
  match duckdb_unsupported_from_message(message) {
    Some(err) => return err
    None => ()
  }
  match duckdb_error_type_from_message(message) {
    Some(error_type) => DuckDB(error_type~, message~)
    None => fallback(message)
  }
}

///|
/// A query/stream failure reported by the DuckDB engine (classified by the
/// `"<Type> Error:"` message prefix) or a plain `Query` binding error.
fn query_error(message : String) -> DuckDBError {
  classify_duckdb_error(message, fallback=fn(m) { Query(m) })
}

///|
/// A prepare failure, classified like `query_error`.
fn prepare_error(message : String) -> DuckDBError {
  classify_duckdb_error(message, fallback=fn(m) { Prepare(m) })
}

///|
/// A bind failure, classified like `query_error`.
fn bind_error(message : String) -> DuckDBError {
  classify_duckdb_error(message, fallback=fn(m) { Bind(m) })
}

///|
/// An appender failure, classified like `query_error`.
fn append_error(message : String) -> DuckDBError {
  classify_duckdb_error(message, fallback=fn(m) { Append(m) })
}

///|
/// A backend/runtime failure outside the query path, classified like
/// `query_error` but falling back to `Backend`.
fn backend_error(backend : String, message : String) -> DuckDBError {
  classify_duckdb_error(message, fallback=fn(m) { Backend(backend~, message=m) })
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
} derive(Eq)

///|
pub extend ColumnType with Eq::{not_equal, equal}

///|
// Ensure public constructors are referenced in all target builds.
fn touch_public_types(backend : JsBackend) -> Unit {
  let _ = backend
  let _ = JsBackend::Node
  let _ = JsBackend::Wasm
  let _ : QueryResult = { columns: [], column_types: [], rows: [], nulls: [], }
  let _ : DataChunk = { columns: [], rows: [], nulls: [], }
  let _ : Decimal = { width: 0, scale: 0, lower: 0, upper: 0, }
  let _ : Interval = { months: 0, days: 0, micros: 0, }
  let _ : List = { elements: [], }
  let _ : Struct = { fields: [], values: [], }
  let _ : Map = { keys: [], values: [], }
  let _ : ColumnType = ColumnType::Unknown(-1)
  let _ = column_type_from_id(0)
  // Typed result API types
  let _ : Value = Value::Decimal({ width: 0, scale: 0, lower: 0, upper: 0, })
  let _ : Value = Value::Int64(0L)
  let _ : Value = Value::UInt64(0UL)
  let _ : Value = Value::Interval({ months: 0, days: 0, micros: 0, })
  let _ : Value = Value::HugeInt(lower=0UL, upper=0L)
  let _ : Value = Value::List([])
  let _ : Value = Value::Struct(fields=[], values=[])
  let _ : Value = Value::Map(keys=[], values=[])
  let _ : Value = Value::Blob(Bytes::default())
  let _ : Value = Value::Null
  let _ : VectorChunk = { columns: [], vectors: [], }
  let _ : Vector = {
    logical_type: ColumnType::Unknown(-1),
    data: VectorData::Any([]),
    validity: FixedArray::make(0, false),
  }
  let _ : TypedQueryResult = { columns: [], data: [], }
}

// ============================================================================
// Advanced Data Types
// ============================================================================

///|
/// Fixed-point decimal type for financial calculations.
/// Carries the full DuckDB 128-bit decimal payload as two 64-bit halves:
/// `upper` is the signed high 64 bits, `lower` the unsigned low 64 bits of the
/// scaled two's-complement integer (value * 10^scale).
pub struct Decimal {
  width : Int // Total number of digits
  scale : Int // Digits after decimal point
  lower : UInt64 // Lower 64 bits of the 128-bit value
  upper : Int64 // Upper 64 bits (sign extension for negative values)
}

///|
/// The full signed 128-bit scaled payload of a Decimal as a BigInt:
/// `upper << 64 | lower`, i.e. value * 10^scale.
fn decimal_scaled_bigint(decimal : Decimal) -> BigInt {
  (BigInt::from_int64(decimal.upper) << 64) | BigInt::from_uint64(decimal.lower)
}

///|
/// 10^n as a BigInt (n <= 0 yields 1).
fn pow10_bigint(n : Int) -> BigInt {
  if n <= 0 {
    BigInt::from_int(1)
  } else {
    BigInt::from_int(10).pow(BigInt::from_int(n))
  }
}

///|
/// Build a Decimal's halves from a signed scaled BigInt value, keeping the
/// low 128 bits in two's-complement form.
fn decimal_from_scaled_bigint(
  value : BigInt,
  width : Int,
  scale : Int,
) -> Decimal {
  let mask = (BigInt::from_int(1) << 64) - BigInt::from_int(1)
  {
    width,
    scale,
    lower: (value & mask).to_uint64(),
    upper: (value >> 64).to_int64(),
  }
}

///|
/// Render a decimal as `whole.fraction` using the full 128-bit payload.
fn decimal_render(decimal : Decimal) -> String {
  let value = decimal_scaled_bigint(decimal)
  let negative = value.compare_int(0) < 0
  let magnitude = if negative { -value } else { value }
  let divisor = pow10_bigint(decimal.scale)
  let sign = if negative { "-" } else { "" }
  if decimal.scale == 0 {
    return sign + (magnitude / divisor).to_string()
  }
  let frac = pad_int((magnitude % divisor).to_string(), decimal.scale)
  sign + (magnitude / divisor).to_string() + "." + frac
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
pub(all) enum Value {
  Int(Int)
  Int64(Int64)
  UInt64(UInt64)
  Double(Double)
  Bool(Bool)
  String(String)
  Date(Int) // Days since 1970-01-01
  Timestamp(Int64) // Microseconds since 1970-01-01
  Decimal(Decimal)
  Interval(Interval)
  HugeInt(lower~ : UInt64, upper~ : Int64)
  Blob(Bytes)
  List(Array[Value])
  Struct(fields~ : Array[String], values~ : Array[Value])
  Map(keys~ : Array[Value], values~ : Array[Value])
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
/// The string value at (`row`, `col`), or `None` when the cell is NULL or
/// the indices are out of bounds (bounds-checked, never panics).
pub fn QueryResult::cell(self : QueryResult, row : Int, col : Int) -> String? {
  if row < 0 ||
    row >= self.rows.length() ||
    col < 0 ||
    col >= self.columns.length() ||
    col >= self.rows[row].length() ||
    col >= self.nulls[row].length() {
    None
  } else if self.nulls[row][col] {
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
/// The string value at (`row`, `col`), or `None` when the cell is NULL or
/// the indices are out of bounds (bounds-checked, never panics).
pub fn DataChunk::cell(self : DataChunk, row : Int, col : Int) -> String? {
  if row < 0 ||
    row >= self.rows.length() ||
    col < 0 ||
    col >= self.columns.length() ||
    col >= self.rows[row].length() ||
    col >= self.nulls[row].length() {
    None
  } else if self.nulls[row][col] {
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

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
  micros : Int // Microseconds
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
// Type Parsing
// ============================================================================

///|
/// Map DuckDB type id to ColumnType.
pub fn column_type_from_id(id : Int) -> ColumnType {
  match id {
    0 => ColumnType::Invalid
    1 => ColumnType::Boolean
    2 => ColumnType::TinyInt
    3 => ColumnType::SmallInt
    4 => ColumnType::Integer
    5 => ColumnType::BigInt
    6 => ColumnType::UTinyInt
    7 => ColumnType::USmallInt
    8 => ColumnType::UInteger
    9 => ColumnType::UBigInt
    10 => ColumnType::Float
    11 => ColumnType::Double
    12 => ColumnType::Timestamp
    13 => ColumnType::Date
    14 => ColumnType::Time
    15 => ColumnType::Interval
    16 => ColumnType::HugeInt
    17 => ColumnType::Varchar
    18 => ColumnType::Blob
    19 => ColumnType::Decimal
    20 => ColumnType::TimestampS
    21 => ColumnType::TimestampMs
    22 => ColumnType::TimestampNs
    23 => ColumnType::Enum
    24 => ColumnType::List
    25 => ColumnType::Struct
    26 => ColumnType::Map
    27 => ColumnType::Uuid
    28 => ColumnType::Union
    29 => ColumnType::Bit
    30 => ColumnType::TimeTz
    31 => ColumnType::TimestampTz
    32 => ColumnType::UHugeInt
    33 => ColumnType::Array
    34 => ColumnType::Any
    35 => ColumnType::Bignum
    36 => ColumnType::SqlNull
    37 => ColumnType::StringLiteral
    38 => ColumnType::IntegerLiteral
    39 => ColumnType::TimeNs
    _ => ColumnType::Unknown(id)
  }
}

///|
/// Infer and parse a string value into an appropriate Value type.
pub fn parse_value(s : String) -> Value {
  if s == "true" {
    Value::Bool(true)
  } else if s == "false" {
    Value::Bool(false)
  } else if is_integer(s) {
    Value::Int(parse_int(s))
  } else if is_double(s) {
    Value::Double(parse_double(s))
  } else if is_date(s) {
    match parse_date(s) {
      Ok(days) => Value::Date(days)
      Err(_) => Value::String(s)
    }
  } else if is_timestamp(s) {
    match parse_timestamp(s) {
      Ok(micros) => Value::Timestamp(micros)
      Err(_) => Value::String(s)
    }
  } else {
    Value::String(s)
  }
}

///|
/// Parse a string value using the declared column type when available.
fn parse_value_with_type(s : String, column_type : ColumnType) -> Value {
  match column_type {
    ColumnType::Boolean =>
      if s == "true" {
        Value::Bool(true)
      } else if s == "false" {
        Value::Bool(false)
      } else {
        Value::String(s)
      }
    ColumnType::TinyInt
    | ColumnType::SmallInt
    | ColumnType::Integer
    | ColumnType::BigInt
    | ColumnType::UTinyInt
    | ColumnType::USmallInt
    | ColumnType::UInteger
    | ColumnType::UBigInt => Value::Int(parse_int(s))
    ColumnType::Float | ColumnType::Double =>
      if is_special_float_string(s) {
        Value::String(s)
      } else {
        Value::Double(parse_double(s))
      }
    ColumnType::Date =>
      match parse_date(s) {
        Ok(days) => Value::Date(days)
        Err(_) => Value::String(s)
      }
    ColumnType::Timestamp
    | ColumnType::TimestampS
    | ColumnType::TimestampMs
    | ColumnType::TimestampNs
    | ColumnType::TimestampTz =>
      match parse_timestamp(s) {
        Ok(micros) => Value::Timestamp(micros)
        Err(_) => Value::String(s)
      }
    ColumnType::Varchar
    | ColumnType::Enum
    | ColumnType::Uuid
    | ColumnType::StringLiteral => Value::String(s)
    ColumnType::Decimal
    | ColumnType::HugeInt
    | ColumnType::UHugeInt
    | ColumnType::Interval
    | ColumnType::List
    | ColumnType::Struct
    | ColumnType::Map
    | ColumnType::Array
    | ColumnType::Union
    | ColumnType::Bit
    | ColumnType::Time
    | ColumnType::TimeTz
    | ColumnType::TimeNs
    | ColumnType::Any
    | ColumnType::Bignum
    | ColumnType::Blob
    | ColumnType::SqlNull
    | ColumnType::IntegerLiteral => Value::String(s)
    ColumnType::Invalid | ColumnType::Unknown(_) => parse_value(s)
  }
}

///|
pub fn is_special_float_string(s : String) -> Bool {
  s == "nan" ||
  s == "NaN" ||
  s == "inf" ||
  s == "Infinity" ||
  s == "-inf" ||
  s == "-Infinity"
}

///|
/// Check if string represents an integer.
pub fn is_integer(s : String) -> Bool {
  if s.length() == 0 {
    false
  } else {
    let mut has_digit = false
    let mut i = 0
    let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
    while i < s.length() {
      let c = s[i]
      if c < '0' || c > '9' {
        if i >= start {
          return false
        }
      } else {
        has_digit = true
      }
      i = i + 1
    }
    has_digit
  }
}

///|
/// Check if string represents a double.
pub fn is_double(s : String) -> Bool {
  if s.length() == 0 {
    false
  } else {
    let mut has_dot = false
    let mut has_digit = false
    let mut i = 0
    let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
    while i < s.length() {
      let c = s[i]
      if c == '.' {
        if has_dot {
          return false
        }
        has_dot = true
      } else if c < '0' || c > '9' {
        if i >= start {
          return false
        }
      } else {
        has_digit = true
      }
      i = i + 1
    }
    has_digit && has_dot
  }
}

///|
/// Parse string to Int.
pub fn parse_int(s : String) -> Int {
  let mut result = 0
  let mut i = 0
  let mut negative = false
  if s.length() > 0 {
    if s[0] == '-' {
      negative = true
      i = 1
    } else if s[0] == '+' {
      i = 1
    }
  }
  let limit = if negative { @int.min_value } else { -@int.max_value }
  let multmin = limit / 10
  while i < s.length() {
    let c = s[i]
    if c >= '0' && c <= '9' {
      let digit = c.to_int() - '0'.to_int()
      if result < multmin {
        return if negative { @int.min_value } else { @int.max_value }
      }
      result = result * 10
      let next = result - digit
      if next < limit {
        return if negative { @int.min_value } else { @int.max_value }
      }
      result = next
    }
    i = i + 1
  }
  if negative {
    result
  } else {
    -result
  }
}

///|
/// Parse string to Double.
pub fn parse_double(s : String) -> Double {
  // For simplicity, use Int parsing and add fractional part
  let dot_index = find_dot(s)
  match dot_index {
    Some(idx) => {
      let int_part = s.view(start_offset=0, end_offset=idx).to_string()
      let frac_part = s
        .view(start_offset=idx + 1, end_offset=s.length())
        .to_string()
      let int_val = parse_int(int_part)
      let frac_val = parse_fractional(frac_part)
      let sign = if s.length() > 0 && s[0] == '-' { -1.0 } else { 1.0 }
      sign * (int_val.abs().to_double() + frac_val)
    }
    None => parse_int(s).to_double()
  }
}

///|
/// Find dot position in string.
fn find_dot(s : String) -> Int? {
  let mut i = 0
  while i < s.length() {
    if s[i] == '.' {
      return Some(i)
    }
    i = i + 1
  }
  None
}

///|
/// Parse fractional part of double.
fn parse_fractional(s : String) -> Double {
  let mut result = 0.0
  let mut divisor = 1.0
  let mut i = 0
  while i < s.length() {
    let c = s[i]
    if c >= '0' && c <= '9' {
      divisor = divisor * 10.0
      result = result + (c.to_int() - '0'.to_int()).to_double() / divisor
    }
    i = i + 1
  }
  result
}

///|
/// Check if string matches ISO date format (YYYY-MM-DD).
fn is_date(s : String) -> Bool {
  s.length() == 10 && s[4] == '-' && s[7] == '-'
}

///|
/// Parse ISO date string to days since epoch.
pub fn parse_date(s : String) -> Result[Int, String] {
  if !is_date(s) {
    Err("invalid date format")
  } else {
    let year_str = s.view(start_offset=0, end_offset=4).to_string()
    let month_str = s.view(start_offset=5, end_offset=7).to_string()
    let day_str = s.view(start_offset=8, end_offset=10).to_string()
    let year = parse_int(year_str)
    let month = parse_int(month_str)
    let day = parse_int(day_str)

    // Validate month range
    if month < 1 || month > 12 {
      Err("invalid month")
    } else if day < 1 || day > typed_days_in_month(year, month) {
      // Validate day range for the specific month/year
      Err("invalid day")
    } else {
      Ok(date_to_days(year, month, day))
    }
  }
}

///|
/// Convert year, month, day to days since epoch (1970-01-01).
pub fn date_to_days(year : Int, month : Int, day : Int) -> Int {
  // Simplified calculation - days from 1970-01-01
  let y = year - 1970
  let mut days = y * 365

  // Add leap days
  let mut leap_years = 0
  let mut ly = 1970
  while ly < year {
    if typed_is_leap_year(ly) {
      leap_years = leap_years + 1
    }
    ly = ly + 1
  }
  days = days + leap_years

  // Add days for months in current year
  let mut m = 1
  while m < month {
    days = days + typed_days_in_month(year, m)
    m = m + 1
  }
  days = days + (day - 1)
  days
}

///|
/// Check if a year is a leap year.
fn typed_is_leap_year(year : Int) -> Bool {
  (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

///|
/// Get the number of days in a month.
fn typed_days_in_month(year : Int, month : Int) -> Int {
  if month == 2 {
    if typed_is_leap_year(year) {
      29
    } else {
      28
    }
  } else if month == 4 || month == 6 || month == 9 || month == 11 {
    30
  } else {
    31
  }
}

///|
/// Check if string matches ISO timestamp format.
fn is_timestamp(s : String) -> Bool {
  s.length() >= 19 &&
  s[4] == '-' &&
  s[7] == '-' &&
  s[10] == ' ' &&
  s[13] == ':' &&
  s[16] == ':'
}

///|
/// Parse ISO timestamp string to microseconds since epoch.
pub fn parse_timestamp(s : String) -> Result[Int64, String] {
  if !is_timestamp(s) {
    Err("invalid timestamp format")
  } else {
    let date_part = s.view(start_offset=0, end_offset=10).to_string()
    match parse_date(date_part) {
      Ok(days) => {
        let time_part = s
          .view(start_offset=11, end_offset=s.length())
          .to_string()
        match parse_time_to_micros(time_part) {
          Ok(micros) => {
            // Use Int64 to avoid overflow
            let days_64 = days.to_int64()
            let days_micros = days_64 * 86400L * 1000000L
            Ok(days_micros + micros)
          }
          Err(e) => Err(e)
        }
      }
      Err(e) => Err(e)
    }
  }
}

///|
/// Parse fractional seconds (up to 6 digits) into microseconds.
pub fn parse_fraction_to_micros(s : String) -> Int {
  let mut micros = 0
  let mut factor = 100000
  let mut i = 0
  while i < s.length() {
    let c = s[i]
    if c < '0' || c > '9' {
      break
    }
    let digit = c.to_int() - '0'.to_int()
    if factor >= 1 {
      micros = micros + digit * factor
      factor = factor / 10
    }
    i = i + 1
  }
  micros
}

///|
/// Parse time string (HH:MM:SS[.sss...]) to microseconds since midnight.
pub fn parse_time_to_micros(s : String) -> Result[Int64, String] {
  // Manual parsing since split() returns Iter
  let mut colon_count = 0
  let mut colon1 = -1
  let mut colon2 = -1
  let mut i = 0
  while i < s.length() {
    if s[i] == ':' {
      if colon1 < 0 {
        colon1 = i
      } else {
        colon2 = i
      }
      colon_count = colon_count + 1
    }
    i = i + 1
  }
  if colon_count < 2 {
    Err("invalid time format")
  } else {
    let hour_str = s.view(start_offset=0, end_offset=colon1).to_string()
    let minute_str = s
      .view(start_offset=colon1 + 1, end_offset=colon2)
      .to_string()
    let sec_part = s
      .view(start_offset=colon2 + 1, end_offset=s.length())
      .to_string()

    // Parse seconds (may have fractional part)
    let dot_idx = find_dot(sec_part)
    let (second, frac_micros) = match dot_idx {
      Some(idx) => {
        let second = parse_int(
          sec_part.view(start_offset=0, end_offset=idx).to_string(),
        )
        let frac = sec_part
          .view(start_offset=idx + 1, end_offset=sec_part.length())
          .to_string()
        (second, parse_fraction_to_micros(frac))
      }
      None => (parse_int(sec_part), 0)
    }
    let hour = parse_int(hour_str)
    let minute = parse_int(minute_str)
    // Use Int64 to avoid overflow
    let base = (hour.to_int64() * 3600L + minute.to_int64() * 60L + second.to_int64()) * 1000000L
    Ok(base + frac_micros.to_int64())
  }
}

// ============================================================================
// TypedQueryResult Conversion
// ============================================================================

///|
/// Convert a QueryResult to a TypedQueryResult by parsing string values.
pub fn QueryResult::to_typed(self : QueryResult) -> TypedQueryResult {
  let column_count = self.column_count()
  let row_count = self.row_count()
  let column_types = self.column_types

  // Initialize column arrays
  let data : Array[Array[Value]] = []
  for col = 0; col < column_count; col = col + 1 {
    data.push([])
  } else {
    ()
  }

  // Parse each cell and populate column-wise
  for row = 0; row < row_count; row = row + 1 {
    for col = 0; col < column_count; col = col + 1 {
      let is_null = self.nulls[row][col]
      let value = if is_null {
        Value::Null
      } else {
        let column_type = if col < column_types.length() {
          column_types[col]
        } else {
          ColumnType::Unknown(-1)
        }
        parse_value_with_type(self.rows[row][col], column_type)
      }
      data[col].push(value)
    } else {
      ()
    }
  } else {
    ()
  }
  { columns: self.columns, data }
}

// ============================================================================
// TypedQueryResult Row-Major Access
// ============================================================================

///|
/// Get the number of rows in the result.
pub fn TypedQueryResult::row_count(self : TypedQueryResult) -> Int {
  if self.data.length() == 0 {
    0
  } else {
    self.data[0].length()
  }
}

///|
/// Get the number of columns in the result.
pub fn TypedQueryResult::column_count(self : TypedQueryResult) -> Int {
  self.columns.length()
}

///|
/// Get a value at the specified row and column.
pub fn TypedQueryResult::get_value(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Value? {
  if row < 0 || row >= self.row_count() || col < 0 || col >= self.column_count() {
    None
  } else {
    Some(self.data[col][row])
  }
}

///|
/// Get an Int value at the specified position.
pub fn TypedQueryResult::get_int(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Int? {
  match self.get_value(row, col) {
    Some(Value::Int(i)) => Some(i)
    _ => None
  }
}

///|
/// Get a Double value at the specified position.
pub fn TypedQueryResult::get_double(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Double? {
  match self.get_value(row, col) {
    Some(Value::Double(d)) => Some(d)
    _ => None
  }
}

///|
/// Get a Bool value at the specified position.
pub fn TypedQueryResult::get_bool(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Bool? {
  match self.get_value(row, col) {
    Some(Value::Bool(b)) => Some(b)
    _ => None
  }
}

///|
/// Get a String value at the specified position.
pub fn TypedQueryResult::get_string(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> String? {
  match self.get_value(row, col) {
    Some(Value::String(s)) => Some(s)
    _ => None
  }
}

///|
/// Get a Date value (days since epoch) at the specified position.
pub fn TypedQueryResult::get_date(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Int? {
  match self.get_value(row, col) {
    Some(Value::Date(d)) => Some(d)
    _ => None
  }
}

///|
/// Get a Timestamp value (microseconds since epoch) at the specified position.
pub fn TypedQueryResult::get_timestamp(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Int64? {
  match self.get_value(row, col) {
    Some(Value::Timestamp(t)) => Some(t)
    _ => None
  }
}

///|
/// Get a Decimal value at the specified position.
pub fn TypedQueryResult::get_decimal(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Decimal? {
  match self.get_value(row, col) {
    Some(Value::Decimal(d)) => Some(d)
    _ => None
  }
}

///|
/// Get a Blob value at the specified position.
pub fn TypedQueryResult::get_blob(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Bytes? {
  match self.get_value(row, col) {
    Some(Value::Blob(b)) => Some(b)
    _ => None
  }
}

///|
/// Check if the value at the specified position is NULL.
pub fn TypedQueryResult::is_null(
  self : TypedQueryResult,
  row : Int,
  col : Int,
) -> Bool {
  match self.get_value(row, col) {
    Some(Value::Null) => true
    _ => false
  }
}

// ============================================================================
// TypedQueryResult Columnar Access
// ============================================================================

///|
/// Get an entire column as typed values.
pub fn TypedQueryResult::get_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Value]? {
  if col < 0 || col >= self.column_count() {
    None
  } else {
    Some(self.data[col])
  }
}

///|
/// Get an entire column as Option[Int] values.
pub fn TypedQueryResult::get_int_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Int?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Int?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Int(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[Double] values.
pub fn TypedQueryResult::get_double_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Double?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Double?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Double(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[Bool] values.
pub fn TypedQueryResult::get_bool_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Bool?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Bool?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Bool(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[String] values.
pub fn TypedQueryResult::get_string_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[String?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[String?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::String(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[Int] date values.
pub fn TypedQueryResult::get_date_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Int?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Int?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Date(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[Int64] timestamp values.
pub fn TypedQueryResult::get_timestamp_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Int64?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Int64?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Timestamp(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

///|
/// Get an entire column as Option[Decimal] values.
pub fn TypedQueryResult::get_decimal_column(
  self : TypedQueryResult,
  col : Int,
) -> Array[Decimal?]? {
  match self.get_column(col) {
    Some(column) => {
      let result : Array[Decimal?] = []
      for i = 0; i < column.length(); i = i + 1 {
        match column[i] {
          Value::Decimal(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } else {
        ()
      }
      Some(result)
    }
    None => None
  }
}

// ============================================================================
// Value Helper Methods
// ============================================================================

///|
/// Get the integer value if present, None otherwise.
pub fn Value::as_int(self : Value) -> Int? {
  match self {
    Int(i) => Some(i)
    _ => None
  }
}

///|
/// Get the double value if present, None otherwise.
pub fn Value::as_double(self : Value) -> Double? {
  match self {
    Double(d) => Some(d)
    _ => None
  }
}

///|
/// Get the boolean value if present, None otherwise.
pub fn Value::as_bool(self : Value) -> Bool? {
  match self {
    Bool(b) => Some(b)
    _ => None
  }
}

///|
/// Get the string value if present, None otherwise.
pub fn Value::as_string(self : Value) -> String? {
  match self {
    String(s) => Some(s)
    _ => None
  }
}

///|
/// Get the date value if present, None otherwise.
pub fn Value::as_date(self : Value) -> Int? {
  match self {
    Date(d) => Some(d)
    _ => None
  }
}

///|
/// Get the timestamp value if present, None otherwise.
pub fn Value::as_timestamp(self : Value) -> Int64? {
  match self {
    Timestamp(t) => Some(t)
    _ => None
  }
}

///|
/// Get the decimal value if present, None otherwise.
pub fn Value::as_decimal(self : Value) -> Decimal? {
  match self {
    Decimal(d) => Some(d)
    _ => None
  }
}

///|
/// Get the blob value if present, None otherwise.
pub fn Value::as_blob(self : Value) -> Bytes? {
  match self {
    Blob(b) => Some(b)
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
/// Convert a Value to its string representation.
pub fn Value::to_string(self : Value) -> String {
  match self {
    Int(n) => n.to_string()
    Double(d) => d.to_string()
    Bool(b) => if b { "true" } else { "false" }
    String(s) => s
    Date(days) => {
      // Convert days since epoch to YYYY-MM-DD
      let (y, m, d) = days_to_ymd(days)
      "\{y}-\{pad_int_2(m)}-\{pad_int_2(d)}"
    }
    Timestamp(micros) =>
      // Convert microseconds since epoch to timestamp string
      timestamp_to_string(micros)
    Decimal(dec) =>
      // Convert decimal to string representation
      // For 128-bit decimals, if upper is non-zero we indicate large value
      if dec.upper != 0 {
        "<large decimal: upper=\{dec.upper}, lower=\{dec.lower}>"
      } else {
        let divisor = int_pow10(dec.scale).to_int()
        let whole = dec.lower / divisor
        let frac = Int::abs(dec.lower % divisor)
        if dec.scale == 0 {
          whole.to_string()
        } else {
          "\{whole}.\{pad_int(frac.to_string(), dec.scale)}"
        }
      }
    Blob(_) =>
      // Convert bytes to hex string representation (simplified)
      "<blob>"
    Null => "NULL"
  }
}

///|
/// Pad an integer with leading zeros to 2 digits.
fn pad_int_2(n : Int) -> String {
  if n < 10 {
    "0\{n}"
  } else {
    n.to_string()
  }
}

///|
/// Pad a string with leading zeros to specified length.
fn pad_int(s : String, len : Int) -> String {
  if s.length() >= len {
    s
  } else {
    let mut result = s
    while result.length() < len {
      result = "0" + result
    }
    result
  }
}

///|
/// Convert days since epoch to year, month, day.
pub fn days_to_ymd(days : Int) -> (Int, Int, Int) {
  // Simplified calculation - accurate for dates after 1970
  let days_per_4_years = 365 * 4 + 1 // Include one leap year
  let mut remaining = days
  let mut year = 1970

  // Add years in 4-year chunks for efficiency
  while remaining >= days_per_4_years {
    remaining = remaining - days_per_4_years
    year = year + 4
  }

  // Add remaining years
  while remaining >= 365 {
    let days_in_year = if is_leap_year(year) { 366 } else { 365 }
    if remaining < days_in_year {
      break
    }
    remaining = remaining - days_in_year
    year = year + 1
  }

  // Add months
  let mut month = 1
  while remaining >= days_in_month(year, month) {
    remaining = remaining - days_in_month(year, month)
    month = month + 1
  }
  let day = remaining + 1
  (year, month, day)
}

///|
/// Convert microseconds since epoch to timestamp string.
fn timestamp_to_string(micros : Int64) -> String {
  let seconds = micros / 1000000L
  let days = seconds / 86400L
  let secs_in_day = seconds % 86400L
  let (year, month, day) = days_to_ymd(days.to_int())
  let hour = (secs_in_day / 3600L).to_int()
  let minute = (secs_in_day % 3600L / 60L).to_int()
  let second = (secs_in_day % 60L).to_int()
  "\{year}-\{pad_int_2(month)}-\{pad_int_2(day)} \{pad_int_2(hour)}:\{pad_int_2(minute)}:\{pad_int_2(second)}"
}

///|
/// Check if a year is a leap year.
pub fn is_leap_year(year : Int) -> Bool {
  (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

///|
/// Get the number of days in a month.
pub fn days_in_month(year : Int, month : Int) -> Int {
  if month == 2 {
    if is_leap_year(year) {
      29
    } else {
      28
    }
  } else if month == 4 || month == 6 || month == 9 || month == 11 {
    30
  } else {
    31
  }
}

///|
/// Compute 10^n for decimal scaling.
pub fn int_pow10(n : Int) -> Int64 {
  if n <= 0 {
    1L
  } else if n == 1 {
    10L
  } else if n == 2 {
    100L
  } else if n == 3 {
    1000L
  } else if n == 4 {
    10000L
  } else if n == 5 {
    100000L
  } else if n == 6 {
    1000000L
  } else if n == 7 {
    10000000L
  } else if n == 8 {
    100000000L
  } else if n == 9 {
    1000000000L
  } else if n == 10 {
    10000000000L
  } else if n == 11 {
    100000000000L
  } else if n == 12 {
    1000000000000L
  } else {
    1L
  } // Limit for now
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

///|
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
  for _ in 0..<column_count {
    data.push([])
  } nobreak {
    ()
  }

  // Parse each cell and populate column-wise
  for row in 0..<row_count {
    for col in 0..<column_count {
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
    } nobreak {
      ()
    }
  } nobreak {
    ()
  }
  { columns: self.columns, data, }
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
      for value in column {
        match value {
          Value::Int(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::Double(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::Bool(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::String(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::Date(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::Timestamp(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
      for value in column {
        match value {
          Value::Decimal(v) => result.push(Some(v))
          Value::Null => result.push(None)
          _ => result.push(None)
        }
      } nobreak {
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
/// Get the nanosecond timestamp value if present, None otherwise.
pub fn Value::as_timestamp_ns(self : Value) -> Int64? {
  match self {
    TimestampNs(t) => Some(t)
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
    Int64(n) => n.to_string()
    UInt64(n) => n.to_string()
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
    TimestampNs(nanos) => timestamp_nanos_render(nanos)
    Decimal(dec) => decimal_render(dec)
    Interval(iv) =>
      "\{iv.months} months \{iv.days} days \{iv.micros} microseconds"
    HugeInt(lower~, upper~) =>
      ((BigInt::from_int64(upper) << 64) | BigInt::from_uint64(lower)).to_string()
    Blob(_) =>
      // Convert bytes to hex string representation (simplified)
      "<blob>"
    List(items) => "[" + items.map(fn(v) { v.to_string() }).join(", ") + "]"
    Struct(fields~, values~) => {
      let pairs : Array[String] = []
      for i, field in fields {
        if i < values.length() {
          pairs.push("\{field}: \{values[i].to_string()}")
        }
      }
      "{" + pairs.join(", ") + "}"
    }
    Map(keys~, values~) => {
      let pairs : Array[String] = []
      for i, key in keys {
        if i < values.length() {
          pairs.push("\{key.to_string()}=\{values[i].to_string()}")
        }
      }
      "{" + pairs.join(", ") + "}"
    }
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
    for _ in result.length()..<len {
      result = "0" + result
    }
    result
  }
}

///|
/// Convert days since epoch to year, month, day. Howard Hinnant's
/// `civil_from_days` — correct for any day count, including pre-epoch.
pub fn days_to_ymd(days : Int) -> (Int, Int, Int) {
  let z = days.to_int64() + 719468L
  let era = floor_div_i64(z, 146097L)
  let doe = z - era * 146097L // [0, 146096]
  let yoe = (doe - doe / 1460L + doe / 36524L - doe / 146096L) / 365L // [0, 399]
  let y = yoe + era * 400L
  let doy = doe - (365L * yoe + yoe / 4L - yoe / 100L) // [0, 365]
  let mp = (5L * doy + 2L) / 153L // [0, 11]
  let d = doy - (153L * mp + 2L) / 5L + 1L // [1, 31]
  let m = if mp < 10L { mp + 3L } else { mp - 9L } // [1, 12]
  let year = if m <= 2L { y + 1L } else { y }
  (year.to_int(), m.to_int(), d.to_int())
}

///|
/// Floor division on Int64 (rounds toward negative infinity).
fn floor_div_i64(a : Int64, b : Int64) -> Int64 {
  let q = a / b
  if a % b != 0L && (a < 0L) != (b < 0L) {
    q - 1L
  } else {
    q
  }
}

///|
/// Convert microseconds since epoch to timestamp string.
fn timestamp_to_string(micros : Int64) -> String {
  let seconds = floor_div_i64(micros, 1000000L)
  let days = floor_div_i64(seconds, 86400L)
  let secs_in_day = seconds - days * 86400L
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

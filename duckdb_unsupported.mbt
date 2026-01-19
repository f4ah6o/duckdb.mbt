///|
pub fn connect(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  let _ = path
  touch_public_types(backend)
  on_ready(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Connection::close(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Connection::query(
  self : Connection,
  sql : String,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Connection::query_stream(
  self : Connection,
  sql : String,
  on_done~ : (Result[ResultStream, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn ResultStream::columns(self : ResultStream) -> Array[String] {
  let _ = self
  []
}

///|
pub fn ResultStream::column_count(self : ResultStream) -> Int {
  let _ = self
  0
}

///|
pub fn ResultStream::next(
  self : ResultStream,
  on_done~ : (Result[DataChunk?, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn ResultStream::close(
  self : ResultStream,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

// ============================================================================
// Prepared Statement Stubs
// ============================================================================

///|
pub fn Connection::prepare(
  self : Connection,
  sql : String,
  on_done~ : (Result[PreparedStatement, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn PreparedStatement::bind_int(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_bigint(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_double(
  self : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_varchar(
  self : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_bool(
  self : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_null(
  self : PreparedStatement,
  index : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::clear_bindings(
  self : PreparedStatement,
) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::execute(
  self : PreparedStatement,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn PreparedStatement::execute_stream(
  self : PreparedStatement,
  on_done~ : (Result[ResultStream, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn PreparedStatement::close(
  self : PreparedStatement,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

// ============================================================================
// Configuration Stubs
// ============================================================================

// Note: Config::create() and connect_with_config are not provided here
// because Config cannot be constructed in stub code for unsupported targets.
// These functions are provided by the external bindings on supported targets.

///|
pub fn Config::set(
  self : Config,
  key : String,
  value : String,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = key
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

// ============================================================================
// Appender Stubs
// ============================================================================

///|
pub fn Connection::create_appender(
  self : Connection,
  schema : String,
  table : String,
  on_done~ : (Result[Appender, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = schema
  let _ = table
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Appender::begin_row(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_int(
  self : Appender,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bigint(
  self : Appender,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_double(
  self : Appender,
  value : Double,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_varchar(
  self : Appender,
  value : String,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bool(
  self : Appender,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_null(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::end_row(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::flush(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

// Note: Appender::close() is not provided here to avoid conflict with Connection::close()
// The external bindings provide the implementation on supported targets.

// ============================================================================
// Date/Timestamp Stubs
// ============================================================================

///|
pub fn PreparedStatement::bind_date(
  self : PreparedStatement,
  index : Int,
  days : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = days
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_timestamp(
  self : PreparedStatement,
  index : Int,
  micros : Int64,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = micros
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_date(
  self : Appender,
  days : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = days
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_timestamp(
  self : Appender,
  micros : Int64,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = micros
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
fn _is_leap_year(year : Int) -> Bool {
  year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

///|
fn days_before_month(month : Int) -> Int {
  match month {
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
fn count_leap_years_before(year : Int) -> Int {
  let y = year - 1
  y / 4 - y / 100 + y / 400
}

///|
pub fn date_from_ymd(year : Int, month : Int, day : Int) -> Int {
  let year_delta = year - 1970
  let leap_years = count_leap_years_before(year) - count_leap_years_before(1970)
  let year_days = year_delta * 365 + leap_years
  let month_days = days_before_month(month)
  let leap_day = if month > 2 && _is_leap_year(year) { 1 } else { 0 }
  let day_days = day - 1
  year_days + month_days + leap_day + day_days
}

///|
pub fn date_to_ymd(days : Int) -> (Int, Int, Int) {
  let mut remaining_days = days
  let mut year = 1970
  while remaining_days >= 365 {
    let leap = if _is_leap_year(year) { 1 } else { 0 }
    let days_in_year = 365 + leap
    if remaining_days >= days_in_year {
      remaining_days = remaining_days - days_in_year
      year = year + 1
    } else {
      break
    }
  }
  let is_leap = _is_leap_year(year)
  let mut month = 1
  let mut remaining = remaining_days
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
pub fn timestamp_from_ymd_hms(
  year : Int,
  month : Int,
  day : Int,
  hour : Int,
  minute : Int,
  second : Int,
) -> Int64 {
  let date_days = date_from_ymd(year, month, day)
  let date_days_64 = date_days.to_int64()
  let hour_64 = hour.to_int64()
  let minute_64 = minute.to_int64()
  let second_64 = second.to_int64()
  let day_micros = date_days_64 * 86400L * 1000000L
  let time_micros = (hour_64 * 3600L + minute_64 * 60L + second_64) * 1000000L
  day_micros + time_micros
}

///|
pub fn timestamp_to_ymd_hms(micros : Int64) -> (Int, Int, Int, Int, Int, Int) {
  let total_seconds = micros / 1000000L
  let mut days = total_seconds / 86400L
  let mut seconds_in_day = total_seconds % 86400L
  if seconds_in_day < 0L {
    days = days - 1L
    seconds_in_day = seconds_in_day + 86400L
  }
  let hour = (seconds_in_day / 3600L).to_int()
  let remaining_seconds = seconds_in_day % 3600L
  let minute = (remaining_seconds / 60L).to_int()
  let second = (remaining_seconds % 60L).to_int()
  let (year, month, day) = date_to_ymd(days.to_int())
  (year, month, day, hour, minute, second)
}

// ============================================================================
// Advanced Data Types Stubs
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
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_blob(
  self : Appender,
  value : Bytes,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

// ----------------------------------------------------------------------------
// Decimal Type
// ----------------------------------------------------------------------------

///|
fn _int_pow10(exp : Int) -> Int {
  let mut result = 1
  let mut i = 0
  while i < exp {
    result = result * 10
    i = i + 1
  }
  result
}

///|
pub fn PreparedStatement::bind_decimal(
  self : PreparedStatement,
  index : Int,
  value : Decimal,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_decimal(
  self : Appender,
  value : Decimal,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn decimal_from_double(value : Double, width : Int, scale : Int) -> Decimal {
  let multiplier = _int_pow10(scale)
  let scaled = (value * Double::from_int(multiplier)).to_int()
  let upper = if scaled >= 0 { 0 } else { -1 }
  { width, scale, lower: scaled, upper }
}

///|
pub fn decimal_to_double(decimal : Decimal) -> Double {
  let divisor = Double::from_int(_int_pow10(decimal.scale))
  Double::from_int(decimal.lower) / divisor
}

///|
pub fn decimal_from_parts(
  whole : Int,
  fractional : Int,
  scale : Int,
) -> Decimal {
  let divisor = _int_pow10(scale)
  let value = if whole < 0 {
    whole * divisor - fractional
  } else {
    whole * divisor + fractional
  }
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
  let divisor = _int_pow10(decimal.scale)
  let whole = decimal.lower / divisor
  let remainder = decimal.lower % divisor
  let fractional = if remainder < 0 {
    divisor - Int::abs(remainder)
  } else {
    remainder
  }
  (whole, fractional)
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
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_interval(
  self : Appender,
  value : Interval,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn interval_from_parts(
  months : Int,
  days : Int,
  micros : Int64,
) -> Interval {
  { months, days, micros }
}

///|
pub fn interval_from_days(days : Int) -> Interval {
  { months: 0, days, micros: 0 }
}

///|
pub fn interval_from_hours(hours : Int) -> Interval {
  let micros = hours.to_int64() * 3600L * 1000000L
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_minutes(minutes : Int) -> Interval {
  let micros = minutes.to_int64() * 60L * 1000000L
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_seconds(seconds : Int) -> Interval {
  let micros = seconds.to_int64() * 1000000L
  { months: 0, days: 0, micros }
}

///|
pub fn interval_from_months(months : Int) -> Interval {
  { months, days: 0, micros: 0 }
}

///|
pub fn interval_to_micros(interval : Interval) -> Int64 {
  let days_micros = interval.days.to_int64() * 86400L * 1000000L
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
  let _ = self
  let _ = index
  let _ = values
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
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
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
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
  let _ = self
  let _ = index
  let _ = map
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
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

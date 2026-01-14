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

type Config = Connection

///|
pub fn Config::create() -> Config {
  unsafe_cast { }
}

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

///|
pub fn connect_with_config(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  config : Config?,
  path? : String,
  backend? : JsBackend,
) -> Unit {
  let _ = config
  let _ = path
  let _ = backend
  touch_public_types(backend)
  on_ready(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

// ============================================================================
// Appender Stubs
// ============================================================================

type Appender = Connection

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
pub fn Appender::append_int(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bigint(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_double(self : Appender, value : Double) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_varchar(self : Appender, value : String) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bool(self : Appender, value : Bool) -> Result[Unit, DuckDBError] {
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

///|
pub fn Appender::close(self : Appender, on_done~ : (Result[Unit, DuckDBError]) -> Unit) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

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
  micros : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = micros
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_date(self : Appender, days : Int) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = days
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_timestamp(
  self : Appender,
  micros : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = micros
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn date_from_ymd(year : Int, month : Int, day : Int) -> Int {
  let _ = year
  let _ = month
  let _ = day
  0
}

///|
pub fn date_to_ymd(days : Int) -> (Int, Int, Int) {
  let _ = days
  (1970, 1, 1)
}

///|
pub fn timestamp_from_ymd_hms(
  year : Int,
  month : Int,
  day : Int,
  hour : Int,
  minute : Int,
  second : Int,
) -> Int {
  let _ = year
  let _ = month
  let _ = day
  let _ = hour
  let _ = minute
  let _ = second
  0
}

///|
pub fn timestamp_to_ymd_hms(micros : Int) -> (Int, Int, Int, Int, Int, Int) {
  let _ = micros
  (1970, 1, 1, 0, 0, 0)
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
pub fn Appender::append_blob(self : Appender, value : Bytes) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

// ----------------------------------------------------------------------------
// Decimal Type
// ----------------------------------------------------------------------------

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
pub fn Appender::append_decimal(self : Appender, value : Decimal) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn decimal_from_double(value : Double, width : Int, scale : Int) -> Decimal {
  let _ = value
  let _ = width
  let _ = scale
  { width: 0, scale: 0, value: 0 }
}

///|
pub fn decimal_to_double(decimal : Decimal) -> Double {
  let _ = decimal
  0.0
}

///|
pub fn decimal_from_parts(whole : Int, fractional : Int, scale : Int) -> Decimal {
  let _ = whole
  let _ = fractional
  let _ = scale
  { width: 0, scale: 0, value: 0 }
}

///|
pub fn decimal_to_parts(decimal : Decimal) -> (Int, Int) {
  let _ = decimal
  (0, 0)
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
pub fn Appender::append_interval(self : Appender, value : Interval) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn interval_from_parts(months : Int, days : Int, micros : Int) -> Interval {
  let _ = months
  let _ = days
  let _ = micros
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_from_days(days : Int) -> Interval {
  let _ = days
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_from_hours(hours : Int) -> Interval {
  let _ = hours
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_from_minutes(minutes : Int) -> Interval {
  let _ = minutes
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_from_seconds(seconds : Int) -> Interval {
  let _ = seconds
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_from_months(months : Int) -> Interval {
  let _ = months
  { months: 0, days: 0, micros: 0 }
}

///|
pub fn interval_to_micros(interval : Interval) -> Int {
  let _ = interval
  0
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
  let _ = elements
  { elements: [] }
}

///|
pub fn list_length(list : List) -> Int {
  let _ = list
  0
}

///|
pub fn list_get(list : List, index : Int) -> String? {
  let _ = list
  let _ = index
  None
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
pub fn struct_from_arrays(fields : Array[String], values : Array[String]) -> Struct {
  let _ = fields
  let _ = values
  { fields: [], values: [] }
}

///|
pub fn struct_from_pairs(pairs : Array[(String, String)]) -> Struct {
  let _ = pairs
  { fields: [], values: [] }
}

///|
pub fn struct_get(s : Struct, field_name : String) -> String? {
  let _ = s
  let _ = field_name
  None
}

///|
pub fn struct_field_count(s : Struct) -> Int {
  let _ = s
  0
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
  let _ = keys
  let _ = values
  { keys: [], values: [] }
}

///|
pub fn map_from_pairs(pairs : Array[(String, String)]) -> Map {
  let _ = pairs
  { keys: [], values: [] }
}

///|
pub fn map_get(m : Map, key : String) -> String? {
  let _ = m
  let _ = key
  None
}

///|
pub fn map_size(m : Map) -> Int {
  let _ = m
  0
}


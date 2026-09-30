///|
// ============================================================================
// Canonical typed columnar result model (issue #61)
// ============================================================================
//
// `Vector`/`VectorChunk` are the canonical internal representation of query
// results across all backends. Data lives in owned MoonBit typed arrays, so
// queries can be consumed without converting every cell to `String`. The
// `QueryResult`/`DataChunk` row-string APIs are a compatibility facade on top
// (see `to_query_result`/`to_data_chunk`).
//
// Ownership rules: a `Vector` owns its arrays once constructed — no aliasing
// with backend memory. Backends may keep borrowed handles (e.g. `NativeVector`
// wrapping a `duckdb_vector`) only transiently while materializing a chunk.
// `VectorChunk`s are plain values and outlive the result/stream they came from.

///|
/// Backend-neutral typed column storage for one result column.
///
/// Variants hold one owned array per column, laid out like a DuckDB vector.
/// `Logical/physical` split: `Int32` stores TINYINT/SMALLINT/INTEGER/
/// UTINYINT/USMALLINT/DATE payloads; `Int64` stores BIGINT/UINTEGER/TIME/
/// TIMESTAMP*/TIME_NS payloads (micros/nanos/seconds normalized by
/// `Vector::logical_type`); `HugeInt` stores HUGEINT/UHUGEINT/UUID 128-bit
/// payloads; `Any` is the fallback for types not yet vectorized (UNION, BIT,
/// TIME_TZ, BIGNUM, ...), holding per-cell `Value`s.
pub(all) enum VectorData {
  Bool(FixedArray[Bool])
  Int32(FixedArray[Int])
  Int64(FixedArray[Int64])
  UInt64(FixedArray[UInt64])
  Float(FixedArray[Float])
  Double(FixedArray[Double])
  Varchar(Array[String])
  Blob(Array[Bytes])
  Decimal(FixedArray[Decimal])
  Interval(FixedArray[Interval])
  HugeInt(lower~ : FixedArray[UInt64], upper~ : FixedArray[Int64])
  /// Row `i` is `child[offsets[i]..<offsets[i]+lengths[i]]`.
  /// Also used for fixed-size ARRAY columns (`logical_type` distinguishes).
  List(
    child~ : Vector,
    offsets~ : FixedArray[UInt64],
    lengths~ : FixedArray[UInt64]
  )
  Struct(fields~ : Array[String], children~ : Array[Vector])
  /// Row `i` maps `keys[offsets[i]..<offsets[i]+lengths[i]]` to
  /// `values[offsets[i]..<offsets[i]+lengths[i]]`.
  Map(
    offsets~ : FixedArray[UInt64],
    lengths~ : FixedArray[UInt64],
    keys~ : Vector,
    values~ : Vector
  )
  /// Fallback storage for column types without a dedicated vector layout;
  /// `cells[i]` is the decoded per-row `Value` (never NULL — validity mask
  /// reports NULLs).
  Any(Array[Value])
}

///|
/// A canonical typed column vector: `logical_type` + columnar `data` +
/// per-row `validity` (`validity[i] == false` means row `i` is NULL).
/// `validity.length()` is the row count.
pub(all) struct Vector {
  logical_type : ColumnType
  data : VectorData
  validity : FixedArray[Bool]
}

///|
/// A canonical result chunk: column names plus one typed `Vector` each.
/// Equivalent in spirit to a DuckDB `DataChunk`, but fully owned by MoonBit.
pub(all) struct VectorChunk {
  columns : Array[String]
  vectors : Array[Vector]
}

// ============================================================================
// Vector construction helpers (used by backend materializers)
// ============================================================================

///|
/// Combine multiple chunks into one `QueryResult` (compatibility facade).
pub fn chunks_to_query_result(chunks : Array[VectorChunk]) -> QueryResult {
  let columns : Array[String] = []
  let column_types : Array[ColumnType] = []
  let rows : Array[Array[String]] = []
  let nulls : Array[Array[Bool]] = []
  for chunk in chunks {
    if columns.length() == 0 {
      for name in chunk.columns {
        columns.push(name)
      }
      for vector in chunk.vectors {
        column_types.push(vector.logical_type)
      }
    }
    for row = 0; row < chunk.row_count(); row = row + 1 {
      let row_values : Array[String] = []
      let row_nulls : Array[Bool] = []
      for col = 0; col < chunk.column_count(); col = col + 1 {
        let vector = chunk.vectors[col]
        let is_null = !vector.validity[row]
        row_nulls.push(is_null)
        row_values.push(if is_null { "" } else { vector.string_at(row) })
      } nobreak {
        ()
      }
      rows.push(row_values)
      nulls.push(row_nulls)
    } nobreak {
      ()
    }
  } nobreak {
    ()
  }
  { columns, column_types, rows, nulls, }
}

// ============================================================================
// VectorChunk accessors
// ============================================================================

///|
/// Number of rows in this chunk.
pub fn VectorChunk::row_count(self : VectorChunk) -> Int {
  if self.vectors.length() == 0 {
    0
  } else {
    self.vectors[0].len()
  }
}

///|
/// Number of columns in this chunk.
pub fn VectorChunk::column_count(self : VectorChunk) -> Int {
  self.columns.length()
}

///|
/// The `i`-th column vector.
pub fn VectorChunk::column(self : VectorChunk, index : Int) -> Vector? {
  if index >= 0 && index < self.vectors.length() {
    Some(self.vectors[index])
  } else {
    None
  }
}

///|
/// The column vector named `name` (first match).
pub fn VectorChunk::column_by_name(
  self : VectorChunk,
  name : String,
) -> Vector? {
  for i, col in self.columns {
    if col == name && i < self.vectors.length() {
      return Some(self.vectors[i])
    }
  }
  None
}

///|
/// Logical type of column `col`.
pub fn VectorChunk::column_type(self : VectorChunk, col : Int) -> ColumnType {
  match self.column(col) {
    Some(vector) => vector.logical_type
    None => ColumnType::Invalid
  }
}

///|
/// The typed value at (`row`, `col`), or `None` when out of bounds or the
/// cell is NULL.
pub fn VectorChunk::get_value(
  self : VectorChunk,
  row : Int,
  col : Int,
) -> Value? {
  match self.column(col) {
    Some(vector) =>
      if row >= 0 && row < vector.len() && vector.validity[row] {
        Some(vector.value_at(row))
      } else {
        None
      }
    None => None
  }
}

///|
/// True when the cell at (`row`, `col`) is NULL.
pub fn VectorChunk::is_null(self : VectorChunk, row : Int, col : Int) -> Bool {
  match self.column(col) {
    Some(vector) =>
      if row >= 0 && row < vector.len() {
        !vector.validity[row]
      } else {
        true
      }
    None => true
  }
}

///|
/// Materialize this chunk as the string-based `QueryResult` compatibility
/// facade (column types come from the vectors).
pub fn VectorChunk::to_query_result(self : VectorChunk) -> QueryResult {
  chunks_to_query_result([self])
}

///|
/// Materialize this chunk as the string-based `DataChunk` compatibility
/// facade used by `ResultStream::next`.
pub fn VectorChunk::to_data_chunk(self : VectorChunk) -> DataChunk {
  let rows : Array[Array[String]] = []
  let nulls : Array[Array[Bool]] = []
  for row = 0; row < self.row_count(); row = row + 1 {
    let row_values : Array[String] = []
    let row_nulls : Array[Bool] = []
    for col = 0; col < self.column_count(); col = col + 1 {
      let vector = self.vectors[col]
      let is_null = !vector.validity[row]
      row_nulls.push(is_null)
      row_values.push(if is_null { "" } else { vector.string_at(row) })
    } nobreak {
      ()
    }
    rows.push(row_values)
    nulls.push(row_nulls)
  } nobreak {
    ()
  }
  { columns: self.columns, rows, nulls, }
}

///|
/// Convert directly to `TypedQueryResult` without a string round-trip.
pub fn VectorChunk::to_typed(self : VectorChunk) -> TypedQueryResult {
  let data : Array[Array[Value]] = []
  for vector in self.vectors {
    let column : Array[Value] = []
    for row = 0; row < vector.len(); row = row + 1 {
      if vector.validity[row] {
        column.push(vector.value_at(row))
      } else {
        column.push(Value::Null)
      }
    } nobreak {
      ()
    }
    data.push(column)
  } nobreak {
    ()
  }
  { columns: self.columns, data, }
}

// ============================================================================
// Vector accessors
// ============================================================================

///|
/// Number of rows in this vector.
pub fn Vector::len(self : Vector) -> Int {
  self.validity.length()
}

///|
/// True when row `row` is NULL.
pub fn Vector::is_null(self : Vector, row : Int) -> Bool {
  if row >= 0 && row < self.validity.length() {
    !self.validity[row]
  } else {
    true
  }
}

///|
/// The raw `FixedArray[Int]` payload for an `Int32` vector, or `None` for
/// other storage kinds. Combined with `validity` this exposes the column
/// without per-cell boxing.
pub fn Vector::ints(self : Vector) -> FixedArray[Int]? {
  match self.data {
    Int32(values) => Some(values)
    _ => None
  }
}

///|
/// The raw `FixedArray[Int64]` payload for an `Int64` vector, or `None`.
pub fn Vector::int64s(self : Vector) -> FixedArray[Int64]? {
  match self.data {
    Int64(values) => Some(values)
    _ => None
  }
}

///|
/// The raw `FixedArray[UInt64]` payload for a `UInt64` vector, or `None`.
pub fn Vector::uint64s(self : Vector) -> FixedArray[UInt64]? {
  match self.data {
    UInt64(values) => Some(values)
    _ => None
  }
}

///|
/// The raw `FixedArray[Double]` payload for a `Double` vector, or `None`.
/// `Float` vectors are widened into a fresh `FixedArray[Double]` copy.
pub fn Vector::doubles(self : Vector) -> FixedArray[Double]? {
  match self.data {
    Double(values) => Some(values)
    Float(values) =>
      Some(FixedArray::makei(values.length(), fn(i) { values[i].to_double() }))
    _ => None
  }
}

///|
/// The raw `FixedArray[Bool]` payload for a `Bool` vector, or `None`.
pub fn Vector::bools(self : Vector) -> FixedArray[Bool]? {
  match self.data {
    Bool(values) => Some(values)
    _ => None
  }
}

///|
/// The `Array[String]` payload for a `Varchar` vector, or `None`.
pub fn Vector::strings(self : Vector) -> Array[String]? {
  match self.data {
    Varchar(values) => Some(values)
    _ => None
  }
}

///|
/// The `Array[Bytes]` payload for a `Blob` vector, or `None`.
pub fn Vector::blobs(self : Vector) -> Array[Bytes]? {
  match self.data {
    Blob(values) => Some(values)
    _ => None
  }
}

///|
/// The `FixedArray[Decimal]` payload for a `Decimal` vector, or `None`.
pub fn Vector::decimals(self : Vector) -> FixedArray[Decimal]? {
  match self.data {
    Decimal(values) => Some(values)
    _ => None
  }
}

///|
/// The `FixedArray[Interval]` payload for an `Interval` vector, or `None`.
pub fn Vector::intervals(self : Vector) -> FixedArray[Interval]? {
  match self.data {
    Interval(values) => Some(values)
    _ => None
  }
}

///|
/// The child vector and per-row `offsets`/`lengths` for a `List` vector.
pub fn Vector::list_parts(
  self : Vector,
) -> (Vector, FixedArray[UInt64], FixedArray[UInt64])? {
  match self.data {
    List(child~, offsets~, lengths~) => Some((child, offsets, lengths))
    _ => None
  }
}

///|
/// Field names and child vectors for a `Struct` vector.
pub fn Vector::struct_parts(self : Vector) -> (Array[String], Array[Vector])? {
  match self.data {
    Struct(fields~, children~) => Some((fields, children))
    _ => None
  }
}

///|
/// Entry offsets/lengths plus key/value vectors for a `Map` vector.
pub fn Vector::map_parts(
  self : Vector,
) -> (FixedArray[UInt64], FixedArray[UInt64], Vector, Vector)? {
  match self.data {
    Map(offsets~, lengths~, keys~, values~) =>
      Some((offsets, lengths, keys, values))
    _ => None
  }
}

///|
/// Decoded per-cell values for an `Any` vector, or `None`.
pub fn Vector::any_cells(self : Vector) -> Array[Value]? {
  match self.data {
    Any(cells) => Some(cells)
    _ => None
  }
}

// ============================================================================
// Vector → Value decode
// ============================================================================

///|
/// The typed `Value` for row `row`. Caller must ensure the row is valid
/// (`!is_null(row)`); NULL rows return `Value::Null`.
pub fn Vector::value_at(self : Vector, row : Int) -> Value {
  if self.is_null(row) {
    return Value::Null
  }
  match self.data {
    Bool(values) => Value::Bool(values[row])
    Int32(values) =>
      match self.logical_type {
        ColumnType::Date => Value::Date(values[row])
        _ => Value::Int(values[row])
      }
    Int64(values) =>
      match self.logical_type {
        ColumnType::Timestamp
        | ColumnType::TimestampS
        | ColumnType::TimestampMs
        | ColumnType::TimestampNs
        | ColumnType::TimestampTz =>
          // Stored as microseconds since epoch (see backend materializers).
          Value::Timestamp(values[row])
        _ => Value::Int64(values[row])
      }
    UInt64(values) => Value::UInt64(values[row])
    Float(values) => Value::Double(values[row].to_double())
    Double(values) => Value::Double(values[row])
    Varchar(values) => Value::String(values[row])
    Blob(values) => Value::Blob(values[row])
    Decimal(values) => Value::Decimal(values[row])
    Interval(values) => Value::Interval(values[row])
    HugeInt(lower~, upper~) =>
      Value::HugeInt(lower=lower[row], upper=upper[row])
    List(child~, offsets~, lengths~) => {
      let start = offsets[row].to_int()
      let len = lengths[row].to_int()
      let items : Array[Value] = []
      for i in start..<(start + len) {
        if i < child.len() {
          items.push(
            if child.validity[i] {
              child.value_at(i)
            } else {
              Value::Null
            },
          )
        }
      }
      Value::List(items)
    }
    Struct(fields~, children~) => {
      let values : Array[Value] = []
      for child in children {
        values.push(
          if row < child.len() && child.validity[row] {
            child.value_at(row)
          } else {
            Value::Null
          },
        )
      }
      Value::Struct(fields~, values~)
    }
    Map(offsets~, lengths~, keys~, values~) => {
      let start = offsets[row].to_int()
      let len = lengths[row].to_int()
      let ks : Array[Value] = []
      let vs : Array[Value] = []
      for i in start..<(start + len) {
        ks.push(
          if i < keys.len() && keys.validity[i] {
            keys.value_at(i)
          } else {
            Value::Null
          },
        )
        vs.push(
          if i < values.len() && values.validity[i] {
            values.value_at(i)
          } else {
            Value::Null
          },
        )
      }
      Value::Map(keys=ks, values=vs)
    }
    Any(cells) => if row < cells.length() { cells[row] } else { Value::Null }
  }
}

// ============================================================================
// DuckDB-style string rendering (compat materialization)
// ============================================================================

///|
/// Render row `row` of this vector as a DuckDB-style string (best-effort
/// match to `duckdb_value_varchar` output). Used by the compat facade; NULL
/// rows yield `""`.
pub fn Vector::string_at(self : Vector, row : Int) -> String {
  if self.is_null(row) {
    return ""
  }
  match self.data {
    Int32(values) =>
      match self.logical_type {
        ColumnType::Date => date_render(values[row])
        _ => values[row].to_string()
      }
    Int64(values) =>
      match self.logical_type {
        ColumnType::Time => time_micros_render(values[row])
        ColumnType::TimeNs => time_nanos_render(values[row])
        ColumnType::Timestamp
        | ColumnType::TimestampS
        | ColumnType::TimestampMs
        | ColumnType::TimestampTz =>
          // S/MS payloads are normalized to micros by the materializers.
          timestamp_micros_render(values[row])
        ColumnType::TimestampNs => timestamp_nanos_render(values[row])
        _ => values[row].to_string()
      }
    UInt64(values) => values[row].to_string()
    Float(values) => duckdb_double_render(values[row].to_double())
    Double(values) => duckdb_double_render(values[row])
    HugeInt(lower~, upper~) =>
      match self.logical_type {
        ColumnType::Uuid => uuid_render(lower[row], upper[row])
        _ => hugeint_render(lower[row], upper[row])
      }
    _ => value_to_duckdb_string(self.value_at(row))
  }
}

///|
/// Render a `Value` the way `duckdb_value_varchar` would (best effort).
fn value_to_duckdb_string(value : Value) -> String {
  match value {
    Value::Int(n) => n.to_string()
    Value::Int64(n) => n.to_string()
    Value::UInt64(n) => n.to_string()
    Value::Double(d) => duckdb_double_render(d)
    Value::Bool(b) => if b { "true" } else { "false" }
    Value::String(s) => s
    Value::Date(days) => date_render(days)
    Value::Timestamp(micros) => timestamp_micros_render(micros)
    Value::Decimal(dec) => decimal_render(dec)
    Value::Interval(iv) => interval_render(iv)
    Value::HugeInt(lower~, upper~) => hugeint_render(lower, upper)
    Value::Blob(bytes) => blob_render(bytes)
    Value::List(items) => {
      let sb = StringBuilder()
      sb.write_char('[')
      for i, item in items {
        if i > 0 {
          sb.write_view(", ")
        }
        sb.write_view(
          match item {
            Value::Null => "NULL"
            _ => value_to_duckdb_string(item)
          },
        )
      }
      sb.write_char(']')
      sb.to_string()
    }
    Value::Struct(fields~, values~) => {
      let sb = StringBuilder()
      sb.write_char('{')
      for i, field in fields {
        if i > 0 {
          sb.write_view(", ")
        }
        sb.write_view(field)
        sb.write_view(": ")
        if i < values.length() {
          sb.write_view(
            match values[i] {
              Value::Null => "NULL"
              other => value_to_duckdb_string(other)
            },
          )
        }
      }
      sb.write_char('}')
      sb.to_string()
    }
    Value::Map(keys~, values~) => {
      let sb = StringBuilder()
      sb.write_char('{')
      for i, key in keys {
        if i > 0 {
          sb.write_view(", ")
        }
        sb.write_view(
          match key {
            Value::Null => "NULL"
            _ => value_to_duckdb_string(key)
          },
        )
        sb.write_char('=')
        if i < values.length() {
          sb.write_view(
            match values[i] {
              Value::Null => "NULL"
              other => value_to_duckdb_string(other)
            },
          )
        }
      }
      sb.write_char('}')
      sb.to_string()
    }
    Value::Null => "NULL"
  }
}

///|
/// Render a Double in DuckDB's cast-to-VARCHAR style: lowercase `nan`/`inf`/
/// `-inf`, an `e+NN`/`e-NN` exponent, and a trailing `.0` for integral values.
fn duckdb_double_render(d : Double) -> String {
  if d.is_nan() {
    return "nan"
  }
  if d.is_inf() {
    return if d < 0.0 { "-inf" } else { "inf" }
  }
  let s = d.to_string()
  // MoonBit emits e.g. "1e300"; DuckDB emits "1e+300".
  if s.contains("e") {
    normalize_exponent(s)
  } else if !s.contains(".") {
    s + ".0"
  } else {
    s
  }
}

///|
/// Normalize a MoonBit exponent `NeN`/`Ne-N`/`Ne+N` to DuckDB's `Ne+NN`.
fn normalize_exponent(s : String) -> String {
  let buf = StringBuilder()
  let mut seen_e = false
  for c in s {
    if c == 'e' || c == 'E' {
      seen_e = true
      buf.write_char('e')
      continue
    }
    if seen_e {
      if c != '+' && c != '-' && !(c >= '0' && c <= '9') {
        continue
      }
      buf.write_char(c)
    } else {
      buf.write_char(c)
    }
  }
  let out = buf.to_string()
  // Ensure the exponent carries an explicit sign.
  if out.contains("e-") || out.contains("e+") {
    out
  } else {
    out.replace(old="e", new="e+")
  }
}

///|
/// `YYYY-MM-DD` for days since epoch.
fn date_render(days : Int) -> String {
  let (year, month, day) = days_to_ymd(days)
  "\{year}-\{pad2(month)}-\{pad2(day)}"
}

///|
fn pad2(n : Int) -> String {
  if n < 10 {
    "0\{n}"
  } else {
    n.to_string()
  }
}

///|
/// `HH:MM:SS[.ffffff]` for microseconds since midnight (DuckDB TIME format).
fn time_micros_render(micros : Int64) -> String {
  let hour = micros / 3600000000L
  let minute = micros % 3600000000L / 60000000L
  let second = micros % 60000000L / 1000000L
  let frac = micros % 1000000L
  let base = "\{pad2(hour.to_int())}:\{pad2(minute.to_int())}:\{pad2(second.to_int())}"
  if frac == 0L {
    base
  } else {
    base + "." + trim_frac(frac, 6)
  }
}

///|
/// `HH:MM:SS[.fffffffff]` for nanoseconds since midnight (DuckDB TIME_NS).
fn time_nanos_render(nanos : Int64) -> String {
  let hour = nanos / 3600000000000L
  let minute = nanos % 3600000000000L / 60000000000L
  let second = nanos % 60000000000L / 1000000000L
  let frac = nanos % 1000000000L
  let base = "\{pad2(hour.to_int())}:\{pad2(minute.to_int())}:\{pad2(second.to_int())}"
  if frac == 0L {
    base
  } else {
    base + "." + trim_frac(frac, 9)
  }
}

///|
/// `YYYY-MM-DD HH:MM:SS[.ffffff]` for microseconds since epoch.
fn timestamp_micros_render(micros : Int64) -> String {
  let (year, month, day, hour, minute, second, frac) = split_micros(micros)
  let base = "\{year}-\{pad2(month)}-\{pad2(day)} \{pad2(hour)}:\{pad2(minute)}:\{pad2(second)}"
  if frac == 0L {
    base
  } else {
    base + "." + trim_frac(frac, 6)
  }
}

///|
/// `YYYY-MM-DD HH:MM:SS[.fffffffff]` for nanoseconds since epoch.
fn timestamp_nanos_render(nanos : Int64) -> String {
  let micros = nanos / 1000L
  let extra = nanos % 1000L
  let (year, month, day, hour, minute, second, frac) = split_micros(micros)
  let nanofrac = frac * 1000L + extra
  let base = "\{year}-\{pad2(month)}-\{pad2(day)} \{pad2(hour)}:\{pad2(minute)}:\{pad2(second)}"
  if nanofrac == 0L {
    base
  } else {
    base + "." + trim_frac(nanofrac, 9)
  }
}

///|
/// Split microseconds since epoch into (year, month, day, hour, min, sec,
/// microfraction), floor-dividing for negative timestamps.
fn split_micros(micros : Int64) -> (Int, Int, Int, Int, Int, Int, Int64) {
  let total_seconds = micros / 1000000L
  let mut frac = micros % 1000000L
  let mut secs = total_seconds
  if frac < 0L {
    secs = secs - 1L
    frac = frac + 1000000L
  }
  let mut days = secs / 86400L
  let mut secs_in_day = secs % 86400L
  if secs_in_day < 0L {
    days = days - 1L
    secs_in_day = secs_in_day + 86400L
  }
  let (year, month, day) = days_to_ymd(days.to_int())
  (
    year,
    month,
    day,
    (secs_in_day / 3600L).to_int(),
    (secs_in_day % 3600L / 60L).to_int(),
    (secs_in_day % 60L).to_int(),
    frac,
  )
}

///|
/// A fractional part rendered with `width` digits then trailing zeros
/// stripped (DuckDB prints the minimal precision needed).
fn trim_frac(frac : Int64, width : Int) -> String {
  let mut digits = frac.to_string()
  for _ in digits.length()..<width {
    digits = "0" + digits
  }
  let mut end = digits.length()
  while end > 0 && digits[end - 1] == '0' {
    end = end - 1
  }
  if end == 0 {
    "0"
  } else {
    digits.view(start_offset=0, end_offset=end).to_owned()
  }
}

///|
/// Render a DuckDB INTERVAL (`months`, `days`, `micros`) in the style
/// `1 year 2 months 3 days 04:05:06.789`.
fn interval_render(iv : Interval) -> String {
  let parts : Array[String] = []
  let months = iv.months
  if months != 0 {
    let years = months / 12
    let rem = months % 12
    if years != 0 {
      parts.push("\{years} year\{if years.abs() == 1 { "" } else { "s" }}")
    }
    if rem != 0 {
      parts.push("\{rem} month\{if rem.abs() == 1 { "" } else { "s" }}")
    }
  }
  if iv.days != 0 {
    parts.push("\{iv.days} day\{if iv.days.abs() == 1 { "" } else { "s" }}")
  }
  if iv.micros != 0L {
    let micros = iv.micros
    let sign = if micros < 0L { "-" } else { "" }
    let abs = micros.abs()
    let hour = abs / 3600000000L
    let minute = abs % 3600000000L / 60000000L
    let second = abs % 60000000L / 1000000L
    let frac = abs % 1000000L
    let base = "\{sign}\{pad2(hour.to_int())}:\{pad2(minute.to_int())}:\{pad2(second.to_int())}"
    parts.push(if frac == 0L { base } else { base + "." + trim_frac(frac, 6) })
  }
  if parts.length() == 0 {
    "00:00:00"
  } else {
    parts.join(" ")
  }
}

///|
/// Render a signed 128-bit value (split as `lower`/`upper` halves).
fn hugeint_render(lower : UInt64, upper : Int64) -> String {
  ((BigInt::from_int64(upper) << 64) | BigInt::from_uint64(lower)).to_string()
}

///|
/// Render a UUID from its stored `uhugeint` payload (top bit flipped).
fn uuid_render(lower : UInt64, upper : Int64) -> String {
  let hi = upper.reinterpret_as_uint64()
  fn hex64(v : UInt64) -> String {
    let digits = v.to_string(radix=16)
    let mut s = digits
    for _ in digits.length()..<16 {
      s = "0" + s
    }
    s
  }

  let hi_hex = hex64(hi)
  let lo_hex = hex64(lower)
  "\{hi_hex.view(start_offset=0, end_offset=8)}-\{hi_hex.view(start_offset=8, end_offset=12)}-\{hi_hex.view(start_offset=12, end_offset=16)}-\{lo_hex.view(start_offset=0, end_offset=4)}-\{lo_hex.view(start_offset=4, end_offset=16)}"
}

///|
/// Render blob bytes the way `duckdb_value_varchar` does: printable ASCII
/// bytes verbatim, others as `\xNN` lowercase hex escapes.
fn blob_render(bytes : Bytes) -> String {
  let sb = StringBuilder()
  for b in bytes {
    let v = b.to_int()
    if v >= 0x20 && v < 0x7f {
      sb.write_char(v.unsafe_to_char())
    } else {
      sb.write_view("\\x")
      let hi = v / 16
      let lo = v % 16
      sb.write_char(hex_digit(hi))
      sb.write_char(hex_digit(lo))
    }
  }
  sb.to_string()
}

///|
fn hex_digit(n : Int) -> Char {
  if n < 10 {
    ('0'.to_int() + n).unsafe_to_char()
  } else {
    ('a'.to_int() + n - 10).unsafe_to_char()
  }
}

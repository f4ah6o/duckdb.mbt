///|
#external
pub type ArrowResult

// ============================================================================
// Arrow FFI Declarations
// ============================================================================

///|
#borrow(conn, sql)
extern "C" fn native_query_arrow(conn : Connection, sql : Bytes) -> ArrowResult = "duckdb_mb_query_arrow"

///|
#borrow(result)
extern "C" fn native_arrow_destroy(result : ArrowResult) = "duckdb_mb_arrow_destroy"

///|
#borrow(result)
extern "C" fn native_arrow_column_count(result : ArrowResult) -> Int = "duckdb_mb_arrow_column_count"

///|
#borrow(result)
extern "C" fn native_arrow_row_count(result : ArrowResult) -> Int = "duckdb_mb_arrow_row_count"

///|
#borrow(result)
extern "C" fn native_arrow_schema(result : ArrowResult) -> Bytes = "duckdb_mb_arrow_schema"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int32(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_int32"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int64(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_int64"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_double(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_double"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_string(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_string"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_bool(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_bool"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int32_nullable(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_int32_nullable"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_int64_nullable(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_int64_nullable"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_double_nullable(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_double_nullable"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_string_nullable(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_string_nullable"

///|
#borrow(result, col)
extern "C" fn native_arrow_get_column_bool_nullable(
  result : ArrowResult,
  col : Int,
) -> Bytes = "duckdb_mb_arrow_get_column_bool_nullable"

///|
extern "C" fn native_is_null_arrow_result(result : ArrowResult) -> Bool = "duckdb_mb_is_null_arrow_result"

///|
#borrow(data, offset)
extern "C" fn native_bytes_to_double(data : Bytes, offset : Int) -> Double = "duckdb_mb_bytes_to_double"

// ============================================================================
// Public API
// ============================================================================

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

///|
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  let sql_bytes = @encoding/utf8.encode(sql)
  let result = native_query_arrow(self, sql_bytes)
  if native_is_null_arrow_result(result) {
    on_done(Err(DuckDBError::Message(last_error("arrow query failed"))))
  } else {
    on_done(Ok(result))
  }
}

///|
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  native_arrow_column_count(self)
}

///|
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  native_arrow_row_count(self)
}

///|
pub fn ArrowResult::get_schema(
  self : ArrowResult,
) -> Result[ArrowSchemaInfo, DuckDBError] {
  let json_bytes = native_arrow_schema(self)
  // Use unsafe conversion for JSON bytes to string
  let json_str = json_str_from_bytes(json_bytes)
  match parse_arrow_schema_json(json_str) {
    Ok(fields) => Ok(ArrowSchemaInfo::{ fields, })
    Err(msg) => Err(DuckDBError::Message("schema parse failed: " + msg))
  }
}

// Helper function to convert Bytes to String

///|
fn json_str_from_bytes(bytes : Bytes) -> String {
  @encoding/utf8.decode_lossy(bytes)
}

// Unsafe conversion from Int to Char

///|
fn unsafe_int_to_char(n : Int) -> Char {
  // This is an unsafe conversion that assumes n is a valid Unicode code point
  // For ASCII/UTF-8 single bytes, this should be safe
  match n {
    0 => '\u{0}'
    1 => '\u{1}'
    2 => '\u{2}'
    3 => '\u{3}'
    4 => '\u{4}'
    5 => '\u{5}'
    6 => '\u{6}'
    7 => '\u{7}'
    8 => '\u{8}'
    9 => '\u{9}'
    10 => '\u{10}'
    11 => '\u{11}'
    12 => '\u{12}'
    13 => '\u{13}'
    14 => '\u{14}'
    15 => '\u{15}'
    16 => '\u{16}'
    17 => '\u{17}'
    18 => '\u{18}'
    19 => '\u{19}'
    20 => '\u{20}'
    21 => '\u{21}'
    22 => '\u{22}'
    23 => '\u{23}'
    24 => '\u{24}'
    25 => '\u{25}'
    26 => '\u{26}'
    27 => '\u{27}'
    28 => '\u{28}'
    29 => '\u{29}'
    30 => '\u{30}'
    31 => '\u{31}'
    32 => '\u{32}'
    33 => '\u{33}'
    34 => '\u{34}'
    35 => '\u{35}'
    36 => '\u{36}'
    37 => '\u{37}'
    38 => '\u{38}'
    39 => '\u{39}'
    40 => '\u{40}'
    41 => '\u{41}'
    42 => '\u{42}'
    43 => '\u{43}'
    44 => '\u{44}'
    45 => '\u{45}'
    46 => '\u{46}'
    47 => '\u{47}'
    48 => '\u{48}'
    49 => '\u{49}'
    50 => '\u{50}'
    51 => '\u{51}'
    52 => '\u{52}'
    53 => '\u{53}'
    54 => '\u{54}'
    55 => '\u{55}'
    56 => '\u{56}'
    57 => '\u{57}'
    58 => '\u{58}'
    59 => '\u{59}'
    60 => '\u{60}'
    61 => '\u{61}'
    62 => '\u{62}'
    63 => '\u{63}'
    64 => '\u{64}'
    65 => '\u{65}'
    66 => '\u{66}'
    67 => '\u{67}'
    68 => '\u{68}'
    69 => '\u{69}'
    70 => '\u{70}'
    71 => '\u{71}'
    72 => '\u{72}'
    73 => '\u{73}'
    74 => '\u{74}'
    75 => '\u{75}'
    76 => '\u{76}'
    77 => '\u{77}'
    78 => '\u{78}'
    79 => '\u{79}'
    80 => '\u{80}'
    81 => '\u{81}'
    82 => '\u{82}'
    83 => '\u{83}'
    84 => '\u{84}'
    85 => '\u{85}'
    86 => '\u{86}'
    87 => '\u{87}'
    88 => '\u{88}'
    89 => '\u{89}'
    90 => '\u{90}'
    91 => '\u{91}'
    92 => '\u{92}'
    93 => '\u{93}'
    94 => '\u{94}'
    95 => '\u{95}'
    96 => '\u{96}'
    97 => '\u{97}'
    98 => '\u{98}'
    99 => '\u{99}'
    100 => '\u{100}'
    101 => '\u{101}'
    102 => '\u{102}'
    103 => '\u{103}'
    104 => '\u{104}'
    105 => '\u{105}'
    106 => '\u{106}'
    107 => '\u{107}'
    108 => '\u{108}'
    109 => '\u{109}'
    110 => '\u{110}'
    111 => '\u{111}'
    112 => '\u{112}'
    113 => '\u{113}'
    114 => '\u{114}'
    115 => '\u{115}'
    116 => '\u{116}'
    117 => '\u{117}'
    118 => '\u{118}'
    119 => '\u{119}'
    120 => '\u{120}'
    121 => '\u{121}'
    122 => '\u{122}'
    123 => '\u{123}'
    124 => '\u{124}'
    125 => '\u{125}'
    126 => '\u{126}'
    127 => '\u{127}'
    _ => '\u{0}' // Default to null character for out-of-range values
  }
}

// Simple JSON parser for Arrow schema
// Expected format: [{"name":"...","nullable":true/false,"type_id":"..."},...]

///|
fn parse_arrow_schema_json(json : String) -> Result[Array[ArrowField], String] {
  if json.length() < 2 {
    return Err("empty json")
  }

  // Skip outer brackets
  let mut pos = 0
  if json[0] != '[' {
    return Err("not an array")
  }
  pos = pos + 1
  let fields : Array[ArrowField] = []
  while pos < json.length() {
    // Skip whitespace
    while pos < json.length() &&
          (json[pos] == ' ' || json[pos] == '\n' || json[pos] == '\t') {
      pos = pos + 1
    }
    if pos >= json.length() || json[pos] == ']' {
      break
    }
    if json[pos] == '{' {
      pos = pos + 1
      let mut name : String = ""
      let mut nullable : Bool = false
      let mut type_id : String = ""

      // Parse object fields
      while pos < json.length() && json[pos] != '}' {
        // Skip whitespace
        while pos < json.length() &&
              (
                json[pos] == ' ' ||
                json[pos] == ',' ||
                json[pos] == '\n' ||
                json[pos] == '\t'
              ) {
          pos = pos + 1
        }
        if pos >= json.length() || json[pos] == '}' {
          break
        }

        // Find field name
        if json[pos] == '"' {
          pos = pos + 1
          let field_start = pos
          while pos < json.length() && json[pos] != '"' {
            pos = pos + 1
          }
          let field_name = json.substring(start=field_start, end=pos)
          pos = pos + 1 // skip closing quote

          // Skip to colon
          while pos < json.length() && json[pos] != ':' {
            pos = pos + 1
          }
          pos = pos + 1 // skip colon

          // Skip to value
          while pos < json.length() && (json[pos] == ' ' || json[pos] == '\t') {
            pos = pos + 1
          }

          // Parse value
          if field_name == "name" || field_name == "type_id" {
            if json[pos] == '"' {
              pos = pos + 1
              let value_start = pos
              while pos < json.length() && json[pos] != '"' {
                pos = pos + 1
              }
              let value = json.substring(start=value_start, end=pos)
              pos = pos + 1 // skip closing quote
              if field_name == "name" {
                name = value
              } else if field_name == "type_id" {
                type_id = value
              }
            }
          } else if field_name == "nullable" {
            if json[pos] == 't' &&
              pos + 4 <= json.length() &&
              json.substring(start=pos, end=pos + 4) == "true" {
              nullable = true
              pos = pos + 4
            } else if json[pos] == 'f' &&
              pos + 5 <= json.length() &&
              json.substring(start=pos, end=pos + 5) == "false" {
              nullable = false
              pos = pos + 5
            } else {
              pos = pos + 1
            }
          }
        } else {
          pos = pos + 1
        }
      }
      if pos < json.length() && json[pos] == '}' {
        pos = pos + 1
      }
      fields.push(ArrowField::{ name, nullable, type_id })
    } else {
      pos = pos + 1
    }
  }
  Ok(fields)
}

///|
pub fn ArrowResult::get_column_int32(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let data = native_arrow_get_column_int32(self, col)
  decode_int32_array(data)
}

///|
fn decode_int32_array(data : Bytes) -> Array[Int] {
  if data.length() < 4 {
    return []
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return []
  }
  let expected_len = 4 + count * 4
  if data.length() < expected_len {
    return []
  }
  let result = Array::make(count, 0)
  let mut i = 0
  while i < count {
    result[i] = read_int32_le(data, 4 + i * 4)
    i = i + 1
  }
  result
}

///|
fn read_int32_le(data : Bytes, offset : Int) -> Int {
  if offset + 4 > data.length() {
    return 0
  }
  // Little-endian int32
  let b0 = data[offset].to_int()
  let b1 = data[offset + 1].to_int()
  let b2 = data[offset + 2].to_int()
  let b3 = data[offset + 3].to_int()
  b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
}

///|
pub fn ArrowResult::get_column_int64(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let data = native_arrow_get_column_int64(self, col)
  if data.length() < 4 {
    return []
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return []
  }
  let expected_len = 4 + count * 8
  if data.length() < expected_len {
    return []
  }
  let result = Array::make(count, 0)
  let mut i = 0
  while i < count {
    // Int64 values - read as Int (MoonBit Int is int64)
    let base = 4 + i * 8
    let b0 = data[base].to_int()
    let b1 = data[base + 1].to_int()
    let b2 = data[base + 2].to_int()
    let b3 = data[base + 3].to_int()
    let b4 = data[base + 4].to_int()
    let b5 = data[base + 5].to_int()
    let b6 = data[base + 6].to_int()
    let b7 = data[base + 7].to_int()
    result[i] = b0 |
      (b1 << 8) |
      (b2 << 16) |
      (b3 << 24) |
      (b4 << 32) |
      (b5 << 40) |
      (b6 << 48) |
      (b7 << 56)
    i = i + 1
  }
  result
}

///|
pub fn ArrowResult::get_column_double(
  self : ArrowResult,
  col : Int,
) -> Array[Double] {
  let data = native_arrow_get_column_double(self, col)
  if data.length() < 4 {
    return []
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return []
  }
  let expected_len = 4 + count * 8
  if data.length() < expected_len {
    return []
  }
  let result = Array::make(count, 0.0)
  let mut i = 0
  while i < count {
    let base = 4 + i * 8
    // Read 8 bytes as double
    let value = bytes_to_double(data, base)
    result[i] = value
    i = i + 1
  }
  result
}

///|
fn bytes_to_double(data : Bytes, offset : Int) -> Double {
  // Use C FFI to properly convert 8 bytes to double (IEEE 754 bit-cast)
  if offset + 8 > data.length() {
    return 0.0
  }
  native_bytes_to_double(data, offset)
}

///|
pub fn ArrowResult::get_column_string(
  self : ArrowResult,
  col : Int,
) -> Array[String] {
  let data = native_arrow_get_column_string(self, col)
  if data.length() < 8 {
    return []
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return []
  }
  let result = Array::make(count, "")
  let mut pos = 8 // Skip count and total_data_len headers
  let mut i = 0
  while i < count {
    let start_pos = pos
    // Find null terminator
    while pos < data.length() && data[pos] != 0 {
      pos = pos + 1
    }
    if start_pos < data.length() {
      // Convert BytesView to String using UTF-8 decode
      let bytes_view = data.sub(start=start_pos, end=pos)
      result[i] = @encoding/utf8.decode_lossy(bytes_view)
    }
    pos = pos + 1 // Skip null terminator
    i = i + 1
  }
  result
}

///|
pub fn ArrowResult::get_column_bool(
  self : ArrowResult,
  col : Int,
) -> Array[Bool] {
  let data = native_arrow_get_column_bool(self, col)
  if data.length() < 4 {
    return []
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return []
  }
  let expected_len = 4 + count
  if data.length() < expected_len {
    return []
  }
  let result = Array::make(count, false)
  let data_start = 4
  let mut i = 0
  while i < count {
    result[i] = data[data_start + i] != 0
    i = i + 1
  }
  result
}

///|
pub fn ArrowResult::close(
  self : ArrowResult,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  native_arrow_destroy(self)
  on_done(Ok(()))
}

// ============================================================================
// Nullable Column Getters - Return (values, validity) tuples
// ============================================================================

///|
pub fn ArrowResult::get_column_int32_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Int], Array[Bool]) {
  let data = native_arrow_get_column_int32_nullable(self, col)
  decode_int32_nullable_array(data)
}

///|
fn decode_int32_nullable_array(data : Bytes) -> (Array[Int], Array[Bool]) {
  if data.length() < 4 {
    return ([], [])
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return ([], [])
  }
  let values_len = count * 4
  let expected_len = 4 + values_len + count
  if data.length() < expected_len {
    return ([], [])
  }
  let values = Array::make(count, 0)
  let validity = Array::make(count, false)
  let values_start = 4
  let validity_start = 4 + values_len
  let mut i = 0
  while i < count {
    values[i] = read_int32_le(data, values_start + i * 4)
    validity[i] = data[validity_start + i] != 0
    i = i + 1
  }
  (values, validity)
}

///|
pub fn ArrowResult::get_column_int64_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Int], Array[Bool]) {
  let data = native_arrow_get_column_int64_nullable(self, col)
  decode_int64_nullable_array(data)
}

///|
fn decode_int64_nullable_array(data : Bytes) -> (Array[Int], Array[Bool]) {
  if data.length() < 4 {
    return ([], [])
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return ([], [])
  }
  let values_len = count * 8
  let expected_len = 4 + values_len + count
  if data.length() < expected_len {
    return ([], [])
  }
  let values = Array::make(count, 0)
  let validity = Array::make(count, false)
  let values_start = 4
  let validity_start = 4 + values_len
  let mut i = 0
  while i < count {
    let base = values_start + i * 8
    let b0 = data[base].to_int()
    let b1 = data[base + 1].to_int()
    let b2 = data[base + 2].to_int()
    let b3 = data[base + 3].to_int()
    let b4 = data[base + 4].to_int()
    let b5 = data[base + 5].to_int()
    let b6 = data[base + 6].to_int()
    let b7 = data[base + 7].to_int()
    values[i] = b0 |
      (b1 << 8) |
      (b2 << 16) |
      (b3 << 24) |
      (b4 << 32) |
      (b5 << 40) |
      (b6 << 48) |
      (b7 << 56)
    validity[i] = data[validity_start + i] != 0
    i = i + 1
  }
  (values, validity)
}

///|
pub fn ArrowResult::get_column_double_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Double], Array[Bool]) {
  let data = native_arrow_get_column_double_nullable(self, col)
  decode_double_nullable_array(data)
}

///|
fn decode_double_nullable_array(data : Bytes) -> (Array[Double], Array[Bool]) {
  if data.length() < 4 {
    return ([], [])
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return ([], [])
  }
  let values_len = count * 8
  let expected_len = 4 + values_len + count
  if data.length() < expected_len {
    return ([], [])
  }
  let values = Array::make(count, 0.0)
  let validity = Array::make(count, false)
  let values_start = 4
  let validity_start = 4 + values_len
  let mut i = 0
  while i < count {
    let base = values_start + i * 8
    values[i] = bytes_to_double(data, base)
    validity[i] = data[validity_start + i] != 0
    i = i + 1
  }
  (values, validity)
}

///|
pub fn ArrowResult::get_column_string_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[String], Array[Bool]) {
  let data = native_arrow_get_column_string_nullable(self, col)
  decode_string_nullable_array(data)
}

///|
fn decode_string_nullable_array(data : Bytes) -> (Array[String], Array[Bool]) {
  if data.length() < 8 {
    return ([], [])
  }
  let count = read_int32_le(data, 0)
  let total_data_len = read_int32_le(data, 4)
  if count <= 0 || count > 1000000 {
    return ([], [])
  }
  let expected_len = 8 + total_data_len + count
  if data.length() < expected_len {
    return ([], [])
  }
  let values = Array::make(count, "")
  let validity = Array::make(count, false)
  let data_start = 8
  let validity_start = 8 + total_data_len
  let mut pos = data_start
  let mut i = 0
  while i < count {
    let start_pos = pos
    // Find null terminator
    while pos < data.length() && data[pos] != 0 {
      pos = pos + 1
    }
    if start_pos < data.length() {
      let bytes_view = data.sub(start=start_pos, end=pos)
      values[i] = @encoding/utf8.decode_lossy(bytes_view)
    }
    pos = pos + 1 // Skip null terminator
    validity[i] = data[validity_start + i] != 0
    i = i + 1
  }
  (values, validity)
}

///|
pub fn ArrowResult::get_column_bool_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Bool], Array[Bool]) {
  let data = native_arrow_get_column_bool_nullable(self, col)
  decode_bool_nullable_array(data)
}

///|
fn decode_bool_nullable_array(data : Bytes) -> (Array[Bool], Array[Bool]) {
  if data.length() < 4 {
    return ([], [])
  }
  let count = read_int32_le(data, 0)
  if count <= 0 || count > 1000000 {
    return ([], [])
  }
  let expected_len = 4 + count + count
  if data.length() < expected_len {
    return ([], [])
  }
  let values = Array::make(count, false)
  let validity = Array::make(count, false)
  let values_start = 4
  let validity_start = 4 + count
  let mut i = 0
  while i < count {
    values[i] = data[values_start + i] != 0
    validity[i] = data[validity_start + i] != 0
    i = i + 1
  }
  (values, validity)
}

///|
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
    let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
    for i in start..<s.length() {
      let c = s[i]
      if c < '0' || c > '9' {
        return false
      }
      has_digit = true
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
    let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
    for i in start..<s.length() {
      let c = s[i]
      if c == '.' {
        if has_dot {
          return false
        }
        has_dot = true
      } else if c < '0' || c > '9' {
        return false
      } else {
        has_digit = true
      }
    }
    has_digit && has_dot
  }
}

///|
/// Parse string to Int.
pub fn parse_int(s : String) -> Int {
  let mut result = 0
  let mut negative = false
  let mut start = 0
  if s.length() > 0 {
    if s[0] == '-' {
      negative = true
      start = 1
    } else if s[0] == '+' {
      start = 1
    }
  }
  let limit = if negative { @int.min_value } else { -@int.max_value }
  let multmin = limit / 10
  for i in start..<s.length() {
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
  for i in 0..<s.length() {
    if s[i] == '.' {
      return Some(i)
    }
  }
  None
}

///|
/// Parse fractional part of double.
fn parse_fractional(s : String) -> Double {
  let mut result = 0.0
  let mut divisor = 1.0
  for i in 0..<s.length() {
    let c = s[i]
    if c >= '0' && c <= '9' {
      divisor = divisor * 10.0
      result = result + (c.to_int() - '0'.to_int()).to_double() / divisor
    }
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
  for ly in 1970..<year {
    if typed_is_leap_year(ly) {
      leap_years = leap_years + 1
    }
  }
  days = days + leap_years

  // Add days for months in current year
  for m in 1..<month {
    days = days + typed_days_in_month(year, m)
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
  for i in 0..<s.length() {
    let c = s[i]
    if c < '0' || c > '9' {
      break
    }
    let digit = c.to_int() - '0'.to_int()
    if factor >= 1 {
      micros = micros + digit * factor
      factor = factor / 10
    }
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
  for i in 0..<s.length() {
    if s[i] == ':' {
      if colon1 < 0 {
        colon1 = i
      } else {
        colon2 = i
      }
      colon_count = colon_count + 1
    }
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
    let base = (
        hour.to_int64() * 3600L + minute.to_int64() * 60L + second.to_int64()
      ) *
      1000000L
    Ok(base + frac_micros.to_int64())
  }
}

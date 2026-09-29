///|
/// PBT Generators for DuckDB MoonBit bindings
/// Provides value generators for property-based testing

pub fn gen_date() -> @gen.Gen[(Int, Int, Int)] {
  @gen.int_range(1970, 2100).bind(fn(year) {
    @gen.int_range(1, 12).bind(fn(month) {
      let max_day = match month {
        2 => 29 // Simplified - will be filtered by tests
        4 | 6 | 9 | 11 => 30
        _ => 31
      }
      @gen.int_range(1, max_day).map(fn(day) { (year, month, day) })
    })
  })
}

///|
/// Generate edge case dates including leap years and boundaries
pub fn gen_date_edge() -> @gen.Gen[(Int, Int, Int)] {
  @gen.one_of([
    @gen.pure((1970, 1, 1)), // Epoch
    @gen.pure((2000, 2, 29)), // Leap day (divisible by 400)
    @gen.pure((1900, 2, 28)), // Non-leap century
    @gen.pure((1900, 3, 1)), // Day after non-leap Feb 28
    @gen.pure((2024, 2, 29)), // Leap day
    @gen.pure((2023, 2, 28)), // Non-leap year
    @gen.pure((2099, 12, 31)), // Upper boundary
    @gen.pure((1970, 12, 31)), // End of first year
    @gen.pure((1971, 1, 1)), // Start of second year
    @gen.Gen::fmap(@gen.int_range(2000, 2024), fn(y) {
      let is_leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
      let day = if is_leap { 29 } else { 28 }
      (y, 2, day)
    }),
  ])
}

///|
/// Generate safe timestamp values (within Int range)
/// NOTE: Due to 32-bit Int overflow, timestamps beyond ~35 minutes from epoch will overflow
/// This generator uses a very conservative range to avoid overflow
pub fn gen_timestamp_safe() -> @gen.Gen[(Int, Int, Int, Int, Int, Int)] {
  @gen.int_range(1970, 1970).bind(fn(year) {
    @gen.int_range(1, 1).bind(fn(month) {
      @gen.int_range(1, 31).bind(fn(day) {
        @gen.int_range(0, 0).bind(fn(hour) {
          @gen.int_range(0, 35).bind(fn(minute) {
            @gen.int_range(0, 59).map(fn(second) {
              (year, month, day, hour, minute, second)
            })
          })
        })
      })
    })
  })
}

///|
/// Generate timestamp values that will definitely overflow
/// For testing overflow behavior and error handling
pub fn gen_timestamp_overflow() -> @gen.Gen[(Int, Int, Int, Int, Int, Int)] {
  @gen.one_of([
    @gen.pure((1970, 1, 1, 1, 0, 0)), // 1 hour - will overflow
    @gen.pure((2020, 1, 1, 0, 0, 0)), // Far future - will overflow
    @gen.pure((2000, 6, 15, 12, 30, 0)), // Arbitrary date - will overflow
  ])
}

///|
/// Generate edge case timestamps
pub fn gen_timestamp_edge() -> @gen.Gen[(Int, Int, Int, Int, Int, Int)] {
  @gen.one_of([
    @gen.pure((1970, 1, 1, 0, 0, 0)), // Epoch
    @gen.pure((1970, 1, 1, 23, 59, 59)), // End of epoch day
    @gen.pure((2000, 2, 29, 0, 0, 0)), // Leap day midnight
    @gen.pure((2000, 2, 29, 23, 59, 59)), // Leap day end
    @gen.pure((2099, 12, 31, 23, 59, 59)), // Upper boundary
    @gen.pure((1970, 1, 1, 0, 0, 1)), // One second after epoch
    @gen.Gen::fmap(@gen.int_range(0, 23), fn(h) {
      (1970, 1, 1, h, 0, 0) // Hour variations on epoch
    }),
  ])
}

///|
/// Generate Value variants with weighted frequency
pub fn gen_value() -> @gen.Gen[Value] {
  @gen.frequency([
    (20, @gen.Gen::fmap(@gen.int_range(-1000000, 1000000), fn(n) {
      Value::Int(n)
    })),
    (15, @gen.Gen::fmap(@gen.int_range(-1000000, 1000000), fn(n) {
      Value::Double(n.to_double() / 1000.0)
    })),
    (5, @gen.one_of([
      @gen.pure(Value::Bool(true)),
      @gen.pure(Value::Bool(false)),
    ])),
    (15, @gen.Gen::fmap(@gen.int_range(0, 10000), fn(n) {
      Value::String("test_" + n.to_string())
    })),
    (10, @gen.Gen::fmap(gen_date(), fn((y, m, d)) {
      Value::Date(date_to_days(y, m, d))
    })),
    (10, @gen.Gen::fmap(gen_timestamp_safe(), fn((y, m, d, h, min, s)) {
      Value::Timestamp(timestamp_from_ymd_hms(y, m, d, h, min, s))
    })),
    (5, gen_blob_value()),
    (15, @gen.pure(Value::Null)),
  ])
}

///|
/// Generate non-null Value variants
pub fn gen_value_non_null() -> @gen.Gen[Value] {
  @gen.frequency([
    (25, @gen.Gen::fmap(@gen.int_range(-1000000, 1000000), fn(n) {
      Value::Int(n)
    })),
    (20, @gen.Gen::fmap(@gen.int_range(-1000000, 1000000), fn(n) {
      Value::Double(n.to_double() / 1000.0)
    })),
    (10, @gen.one_of([
      @gen.pure(Value::Bool(true)),
      @gen.pure(Value::Bool(false)),
    ])),
    (20, @gen.Gen::fmap(@gen.int_range(0, 10000), fn(n) {
      Value::String("test_" + n.to_string())
    })),
    (15, @gen.Gen::fmap(gen_date(), fn((y, m, d)) {
      Value::Date(date_to_days(y, m, d))
    })),
    (10, @gen.Gen::fmap(gen_timestamp_safe(), fn((y, m, d, h, min, s)) {
      Value::Timestamp(timestamp_from_ymd_hms(y, m, d, h, min, s))
    })),
    (5, gen_blob_value()),
  ])
}

///|
/// Generate ColumnType variants
pub fn gen_column_type() -> @gen.Gen[ColumnType] {
  @gen.one_of([
    @gen.pure(ColumnType::Invalid),
    @gen.pure(ColumnType::Boolean),
    @gen.pure(ColumnType::TinyInt),
    @gen.pure(ColumnType::SmallInt),
    @gen.pure(ColumnType::Integer),
    @gen.pure(ColumnType::BigInt),
    @gen.pure(ColumnType::UTinyInt),
    @gen.pure(ColumnType::USmallInt),
    @gen.pure(ColumnType::UInteger),
    @gen.pure(ColumnType::UBigInt),
    @gen.pure(ColumnType::Float),
    @gen.pure(ColumnType::Double),
    @gen.pure(ColumnType::Timestamp),
    @gen.pure(ColumnType::Date),
    @gen.pure(ColumnType::Time),
    @gen.pure(ColumnType::Interval),
    @gen.pure(ColumnType::HugeInt),
    @gen.pure(ColumnType::UHugeInt),
    @gen.pure(ColumnType::Varchar),
    @gen.pure(ColumnType::Blob),
    @gen.pure(ColumnType::Decimal),
    @gen.pure(ColumnType::TimestampS),
    @gen.pure(ColumnType::TimestampMs),
    @gen.pure(ColumnType::TimestampNs),
    @gen.pure(ColumnType::Enum),
    @gen.pure(ColumnType::List),
    @gen.pure(ColumnType::Struct),
    @gen.pure(ColumnType::Map),
    @gen.pure(ColumnType::Array),
    @gen.pure(ColumnType::Uuid),
    @gen.pure(ColumnType::Union),
    @gen.pure(ColumnType::Bit),
    @gen.pure(ColumnType::TimeTz),
    @gen.pure(ColumnType::TimestampTz),
    @gen.Gen::fmap(@gen.int_range(-100, 1000), fn(n) {
      ColumnType::Unknown(n)
    }),
  ])
}

///|
/// Generate safe Decimal values (within valid width/scale bounds)
pub fn gen_decimal_safe() -> @gen.Gen[Decimal] {
  @gen.int_range(1, 38).bind(fn(width) {
    @gen.int_range(0, width).bind(fn(scale) {
      let max_val = int_pow10(scale).to_int()
      @gen.int_range(-max_val * 100, max_val * 100).map(fn(value) {
        Decimal::{ width, scale, lower: value, upper: 0 }
      })
    })
  })
}

///|
/// Generate Decimal with specific width and scale
pub fn gen_decimal_with(width : Int, scale : Int) -> @gen.Gen[Decimal] {
  let max_val = int_pow10(scale).to_int()
  @gen.int_range(-max_val * 100, max_val * 100).map(fn(value) {
    Decimal::{ width, scale, lower: value, upper: 0 }
  })
}

///|
/// Generate edge case Decimal values
pub fn gen_decimal_edge() -> @gen.Gen[Decimal] {
  @gen.one_of([
    // Width/scale boundaries
    @gen.pure(Decimal::{ width: 1, scale: 0, lower: 0, upper: 0 }),
    @gen.pure(Decimal::{ width: 38, scale: 0, lower: 0, upper: 0 }),
    @gen.pure(Decimal::{ width: 38, scale: 38, lower: 0, upper: 0 }),
    @gen.pure(Decimal::{ width: 10, scale: 2, lower: 0, upper: 0 }),
    // Negative values
    @gen.pure(Decimal::{ width: 10, scale: 2, lower: -1, upper: -1 }),
    @gen.pure(Decimal::{ width: 10, scale: 2, lower: -100, upper: -1 }),
    // Zero
    @gen.pure(Decimal::{ width: 5, scale: 2, lower: 0, upper: 0 }),
    // Large positive values
    @gen.Gen::fmap(@gen.int_range(100000, 1000000), fn(n) {
      Decimal::{ width: 20, scale: 4, lower: n, upper: 0 }
    }),
    // Scale = width (all decimal places)
    @gen.Gen::fmap(@gen.int_range(1, 10), fn(scale) {
      Decimal::{ width: scale, scale: scale, lower: 123, upper: 0 }
    }),
  ])
}

///|
/// Generate safe Interval values
pub fn gen_interval_safe() -> @gen.Gen[Interval] {
  @gen.int_range(-1000, 1000).bind(fn(months) {
    @gen.int_range(-1000, 1000).bind(fn(days) {
      @gen.int_range(-1000000, 1000000).map(fn(micros) {
        Interval::{ months, days, micros }
      })
    })
  })
}

///|
/// Generate edge case Interval values
pub fn gen_interval_edge() -> @gen.Gen[Interval] {
  @gen.one_of([
    // Zero interval
    @gen.pure(Interval::{ months: 0, days: 0, micros: 0 }),
    // Individual components
    @gen.Gen::fmap(@gen.int_range(-10000, 10000), fn(m) {
      Interval::{ months: m, days: 0, micros: 0 }
    }),
    @gen.Gen::fmap(@gen.int_range(-10000, 10000), fn(d) {
      Interval::{ months: 0, days: d, micros: 0 }
    }),
    @gen.Gen::fmap(@gen.int_range(-10000000, 10000000), fn(u) {
      Interval::{ months: 0, days: 0, micros: u }
    }),
    // Boundary combinations
    @gen.pure(Interval::{ months: 1, days: 0, micros: 0 }),
    @gen.pure(Interval::{ months: 0, days: 1, micros: 0 }),
    @gen.pure(Interval::{ months: 0, days: 0, micros: 1000000 }), // 1 second
    @gen.pure(Interval::{ months: 0, days: 0, micros: 60000000 }), // 1 minute
    @gen.pure(Interval::{ months: 0, days: 0, micros: 3600000000 }), // 1 hour
    // Negative values
    @gen.pure(Interval::{ months: -1, days: 0, micros: 0 }),
    @gen.pure(Interval::{ months: 0, days: -1, micros: 0 }),
    @gen.pure(Interval::{ months: 0, days: 0, micros: -1 }),
    // Mixed signs
    @gen.pure(Interval::{ months: 1, days: -1, micros: 1000000 }),
  ])
}

///|
/// Generate Bytes (byte array) for blob testing
pub fn gen_bytes() -> @gen.Gen[Bytes] {
  @gen.int_range(0, 1000).bind(fn(len) {
    array_of(@gen.int_range(0, 255)).map(fn(arr) {
      if arr.length() == 0 {
        Bytes::empty()
      } else {
        let mut bytes = Array::make(len, 0.to_uint8())
        let mut i = 0
        for val in arr {
          if i < len {
            bytes[i] = val.to_uint8()
            i = i + 1
          }
        }
        Bytes::from_array(bytes)
      }
    })
  })
}

///|
/// Generate Value::Blob variants
pub fn gen_blob_value() -> @gen.Gen[Value] {
  @gen.Gen::fmap(gen_bytes(), fn(bytes) { Value::Blob(bytes) })
}

///|
/// Generate empty blob value
pub fn gen_blob_empty() -> @gen.Gen[Value] {
  @gen.pure(Value::Blob(Bytes::empty()))
}

///|
/// Generate List values
pub fn gen_list_value() -> @gen.Gen[List] {
  @gen.int_range(0, 100).bind(fn(len) {
    array_of(@gen.int_range(-1000, 1000)).map(fn(arr) {
      let strs = arr.map(fn(n) { n.to_string() })
      List::{ elements: strs }
    })
  })
}

///|
/// Generate Struct values
pub fn gen_struct_value() -> @gen.Gen[Struct] {
  @gen.int_range(0, 10).bind(fn(field_count) {
    array_of(@gen.int_range(0, 100)).bind(fn(values_arr) {
      let fields = Array::makei(field_count, fn(i) { "field_" + i.to_string() })
      let values = values_arr.map(fn(n) { n.to_string() })
      @gen.pure(Struct::{ fields, values })
    })
  })
}

///|
/// Generate Map values
pub fn gen_map_value() -> @gen.Gen[Map] {
  @gen.int_range(0, 20).bind(fn(size) {
    array_of(@gen.int_range(0, 100)).bind(fn(keys_arr) {
      array_of(@gen.int_range(0, 100)).map(fn(vals_arr) {
        let keys = keys_arr.map(fn(n) { "key_" + n.to_string() })
        let values = vals_arr.map(fn(n) { "val_" + n.to_string() })
        Map::{ keys, values }
      })
    })
  })
}

///|
/// Generate small integer for bounded testing
pub fn gen_small_int() -> @gen.Gen[Int] {
  @gen.int_range(-100, 100)
}

///|
/// Generate non-negative integer
pub fn gen_nat() -> @gen.Gen[Int] {
  @gen.int_range(0, 1000000)
}

///|
/// Generate string for testing
pub fn gen_string() -> @gen.Gen[String] {
  @gen.Gen::fmap(@gen.int_range(0, 100), fn(len) {
    let mut s = ""
    let mut i = 0
    while i < len {
      s = s + "x"
      i = i + 1
    }
    s
  })
}

///|
/// Generate alphanumeric string
pub fn gen_alphanum_string() -> @gen.Gen[String] {
  @gen.Gen::fmap(@gen.int_range(0, 50), fn(len) {
    let mut s = ""
    let mut i = 0
    let chars = "abc123xyz"
    while i < len {
      let idx = i % chars.length()
      s = s + chars[idx].to_string()
      i = i + 1
    }
    s
  })
}

///|
/// Generate array of values
pub fn gen_value_array() -> @gen.Gen[Array[Value]] {
  @gen.int_range(0, 50).bind(fn(len) {
    array_of(gen_value_non_null()).map(fn(arr) {
      if arr.length() > len {
        arr.view(start_offset=0, end_offset=len).to_array()
      } else {
        arr
      }
    })
  })
}

///|
/// Generate Value::Int specifically
pub fn gen_value_int() -> @gen.Gen[Value] {
  @gen.Gen::fmap(@gen.int_range(-1000000000, 1000000000), fn(n) {
    Value::Int(n)
  })
}

///|
/// Generate Value::Double specifically
pub fn gen_value_double() -> @gen.Gen[Value] {
  @gen.Gen::fmap(@gen.int_range(-1000000, 1000000), fn(n) {
    Value::Double(n.to_double() / 1000.0)
  })
}

///|
/// Generate Value::Bool specifically
pub fn gen_value_bool() -> @gen.Gen[Value] {
  @gen.one_of([
    @gen.pure(Value::Bool(true)),
    @gen.pure(Value::Bool(false)),
  ])
}

///|
/// Generate Value::String specifically
pub fn gen_value_string() -> @gen.Gen[Value] {
  @gen.Gen::fmap(gen_alphanum_string(), fn(s) { Value::String(s) })
}

///|
/// Generate Value::Date specifically
pub fn gen_value_date() -> @gen.Gen[Value] {
  @gen.Gen::fmap(gen_date(), fn((y, m, d)) {
    Value::Date(date_to_days(y, m, d))
  })
}

///|
/// Generate Value::Timestamp specifically (safe range)
pub fn gen_value_timestamp_safe() -> @gen.Gen[Value] {
  @gen.Gen::fmap(gen_timestamp_safe(), fn((y, m, d, h, min, s)) {
    Value::Timestamp(timestamp_from_ymd_hms(y, m, d, h, min, s))
  })
}

///|
/// Generate Value::Decimal specifically
pub fn gen_value_decimal() -> @gen.Gen[Value] {
  @gen.Gen::fmap(gen_decimal_safe(), fn(d) { Value::Decimal(d) })
}

///|
/// Generate Value::Null
pub fn gen_value_null() -> @gen.Gen[Value] {
  @gen.pure(Value::Null)
}

///|
/// Generate positive integers only
pub fn gen_pos_int() -> @gen.Gen[Int] {
  @gen.int_range(1, 1000000000)
}

///|
/// Generate negative integers only
pub fn gen_neg_int() -> @gen.Gen[Int] {
  @gen.int_range(-1000000000, -1)
}

///|
/// Generate small positive integers (for exhaustive testing)
pub fn gen_small_pos_int() -> @gen.Gen[Int] {
  @gen.int_range(0, 100)
}

///|
/// Generate array of strings for map keys/values
pub fn gen_string_array() -> @gen.Gen[Array[String]] {
  @gen.int_range(0, 20).bind(fn(len) {
    array_of(@gen.int_range(0, 100)).map(fn(arr) {
      let strs = arr.map(fn(n) { "key_" + n.to_string() })
      strs.view(start_offset=0, end_offset=Int::min(len, strs.length())).to_array()
    })
  })
}

///|
/// Generate pair of arrays for map_from_arrays
pub fn gen_map_arrays() -> @gen.Gen[(Array[String], Array[String])] {
  @gen.int_range(0, 10).bind(fn(len) {
    array_of(@gen.int_range(0, 100)).bind(fn(keys_arr) {
      array_of(@gen.int_range(0, 100)).map(fn(vals_arr) {
        let keys = keys_arr.map(fn(n) { "key_" + n.to_string() })
          .view(start_offset=0, end_offset=len)
          .to_array()
        let values = vals_arr.map(fn(n) { "val_" + n.to_string() })
          .view(start_offset=0, end_offset=len)
          .to_array()
        (keys, values)
      })
    })
  })
}

///|
/// Generate valid month/day combinations for a given year
pub fn gen_valid_date_in_year(year : Int) -> @gen.Gen[(Int, Int, Int)] {
  @gen.int_range(1, 12).bind(fn(month) {
    let max_day = match month {
      2 => if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 { 29 } else { 28 }
      4 | 6 | 9 | 11 => 30
      _ => 31
    }
    @gen.int_range(1, max_day).map(fn(day) { (year, month, day) })
  })
}


///|
/// Property-Based Tests for DuckDB MoonBit bindings
/// Tests round-trip properties for date/timestamp conversion and integer parsing

///|
/// Check if year is a leap year (divisible by 4, not by 100, unless also by 400)
fn _is_leap_year_internal(year : Int) -> Bool {
  (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

///|
/// Property: date_from_ymd/date_to_ymd round-trip
/// Converting a date to days and back should yield the original date
test "prop_date_roundtrip" {
  let gen = @pbt.Gen::choose_int(1970, 2100).bind(fn(year) {
    @pbt.Gen::choose_int(1, 12).bind(fn(month) {
      let max_day = match month {
        2 => if _is_leap_year_internal(year) { 29 } else { 28 }
        4 | 6 | 9 | 11 => 30
        _ => 31
      }
      @pbt.Gen::choose_int(1, max_day).map(fn(day) { (year, month, day) })
    })
  })

  let config = @pbt.CheckConfig::new(1000, 100, 12345, 50)
  @pbt.assert_check(
    "date_from_ymd/date_to_ymd round-trip",
    gen,
    fn(input) {
      let (year, month, day) = input
      let days = date_from_ymd(year, month, day)
      let (y2, m2, d2) = date_to_ymd(days)
      if y2 == year && m2 == month && d2 == day {
        Ok(())
      } else {
        Err("(\{year}, \{month}, \{day}) -> \{days} -> (\{y2}, \{m2}, \{d2})")
      }
    },
    config~,
  )
}

///|
/// Property: timestamp_from_ymd_hms/timestamp_to_ymd_hms round-trip
/// Converting a timestamp to microseconds and back should yield the original timestamp
test "prop_timestamp_roundtrip" {
  let gen = @pbt.Gen::choose_int(1970, 2100).bind(fn(year) {
    @pbt.Gen::choose_int(1, 12).bind(fn(month) {
      let max_day = match month {
        2 => if _is_leap_year_internal(year) { 29 } else { 28 }
        4 | 6 | 9 | 11 => 30
        _ => 31
      }
      @pbt.Gen::choose_int(1, max_day).map(fn(day) { (year, month, day) })
    })
  }).bind(fn(date) {
    let (year, month, day) = date
    @pbt.Gen::choose_int(0, 23).bind(fn(hour) {
      @pbt.Gen::choose_int(0, 59).bind(fn(minute) {
        @pbt.Gen::choose_int(0, 59).map(fn(second) {
          (year, month, day, hour, minute, second)
        })
      })
    })
  })

  let config = @pbt.CheckConfig::new(1000, 100, 54321, 50)
  @pbt.assert_check(
    "timestamp round-trip",
    gen,
    fn(input) {
      let (year, month, day, hour, minute, second) = input
      let micros = timestamp_from_ymd_hms(year, month, day, hour, minute, second)
      let (y2, m2, d2, h2, min2, s2) = timestamp_to_ymd_hms(micros)
      if y2 == year && m2 == month && d2 == day &&
         h2 == hour && min2 == minute && s2 == second {
        Ok(())
      } else {
        Err("(\{year}-\{month}-\{day} \{hour}:\{minute}:\{second}) != (\{y2}-\{m2}-\{d2} \{h2}:\{min2}:\{s2})")
      }
    },
    config~,
  )
}

///|
/// Property: Int -> String -> Int round-trip
/// Converting an integer to string and parsing should yield the original integer
test "prop_parse_int_roundtrip" {
  let config = @pbt.CheckConfig::new(500, 100, 11111, 20)
  let gen = @pbt.Gen::choose_int(-1000000, 1000000)
  @pbt.assert_check(
    "Int -> String -> Int",
    gen,
    fn(n) {
      let s = n.to_string()
      if not(is_integer(s)) {
        Err("is_integer(\{s}) should be true")
      } else {
        let parsed = parse_int(s)
        if parsed == n { Ok(()) } else { Err("\{n} -> \{s} -> \{parsed}") }
      }
    },
    config~,
    shrink=@pbt.shrink_int,
  )
}

///|
/// Property: is_integer should return true for integer string representations
test "prop_is_integer_for_ints" {
  let config = @pbt.CheckConfig::new(500, 100, 22222, 20)
  let gen = @pbt.Gen::choose_int(-1000000, 1000000)
  @pbt.assert_check(
    "is_integer(n.to_string()) == true",
    gen,
    fn(n) {
      let s = n.to_string()
      if is_integer(s) {
        Ok(())
      } else {
        Err("is_integer(\{s}) should be true for \{n}")
      }
    },
    config~,
    shrink=@pbt.shrink_int,
  )
}

// ============================================================================
// Phase 1: ColumnType and Value Round-Trip Tests
// ============================================================================

///|
/// Property: ColumnType ID round-trip
/// Converting an ID to ColumnType should preserve the ID for Unknown type
test "prop_column_type_id_roundtrip" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::choose_int(0, 39),
    @pbt.Gen::choose_int(-100, -1),
    @pbt.Gen::choose_int(40, 1000)
  ])
  let config = @pbt.CheckConfig::new(500, 100, 33333, 30)

  @pbt.assert_check("ColumnType ID round-trip", gen, fn(id) {
    let ct = column_type_from_id(id)
    match ct {
      ColumnType::Unknown(original) => {
        if original == id { Ok(()) }
        else { Err("Unknown(\{original}) should contain \{id}") }
      }
      _ => Ok(())
    }
  }, config~)
}

///|
/// Property: Value::Int round-trip
/// Converting an integer to string and parsing should yield the original integer
test "prop_value_int_roundtrip" {
  let gen = @pbt.Gen::choose_int(-1000000000, 1000000000)
  let config = @pbt.CheckConfig::new(800, 100, 44444, 30)

  @pbt.assert_check("Value::Int round-trip", gen, fn(n) {
    let str_val = n.to_string()
    let parsed = parse_value(str_val)
    match parsed {
      Value::Int(m) => if m == n { Ok(()) }
                       else { Err("\{n} -> \{str_val} -> Value::Int(\{m})") }
      _ => Err("Expected Value::Int")
    }
  }, config~, shrink=@pbt.shrink_int)
}

///|
/// Property: Value::Double round-trip
/// Converting a double to string and parsing should yield approximately the same value
test "prop_value_double_roundtrip" {
  let gen = @pbt.Gen::map(
    @pbt.Gen::choose_int(-1000000, 1000000),
    fn(n) { n.to_double() / 1000.0 }
  )
  let config = @pbt.CheckConfig::new(800, 100, 55555, 30)

  @pbt.assert_check("Value::Double round-trip", gen, fn(d) {
    let str_val = d.to_string()
    let parsed = parse_value(str_val)
    match parsed {
      Value::Double(d2) => {
        let diff = if d2 > d { d2 - d } else { d - d2 }
        if diff < 0.0001 { Ok(()) }
        else { Err("Double mismatch: \{d} vs \{d2}, diff: \{diff}") }
      }
      _ => Err("Expected Value::Double")
    }
  }, config~)
}

///|
/// Property: Value::Bool round-trip
/// Converting a boolean to string and parsing should yield the original boolean
test "prop_value_bool_roundtrip" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::pure(true),
    @pbt.Gen::pure(false)
  ])
  let config = @pbt.CheckConfig::new(10, 10, 66666, 5)

  @pbt.assert_check("Value::Bool round-trip", gen, fn(b) {
    let str_val = if b { "true" } else { "false" }
    let parsed = parse_value(str_val)
    match parsed {
      Value::Bool(b2) => if b2 == b { Ok(()) }
                         else { Err("Bool mismatch: \{b} vs \{b2}") }
      _ => Err("Expected Value::Bool")
    }
  }, config~)
}

///|
/// Property: Value::String round-trip
/// Converting a string to value and parsing should yield the original string
test "prop_value_string_roundtrip" {
  let gen = @pbt.Gen::map(
    @pbt.Gen::choose_int(0, 255),
    fn(code) { "x" + code.to_string() }
  )
  let config = @pbt.CheckConfig::new(500, 100, 77777, 30)

  @pbt.assert_check("Value::String round-trip", gen, fn(s) {
    let parsed = parse_value(s)
    match parsed {
      Value::String(s2) => if s2 == s { Ok(()) }
                           else { Err("String mismatch: \{s} vs \{s2}") }
      _ => Err("Expected Value::String")
    }
  }, config~)
}

// ============================================================================
// Phase 2: Boundary Value and Edge Case Tests
// ============================================================================

///|
/// Property: parse_int boundary values
/// Parsing integers should correctly handle various integer formats
test "prop_parse_int_boundary_values" {
  let gen = @pbt.Gen::choose_int(-1000000, 1000000)
  let config = @pbt.CheckConfig::new(300, 100, 88888, 30)

  @pbt.assert_check("parse_int boundary values", gen, fn(n) {
    let s = n.to_string()
    let parsed = parse_int(s)
    if parsed == n { Ok(()) }
    else { Err("parse_int(\{s}) = \{parsed}, expected \{n}") }
  }, config~)
}

///|
/// Property: is_integer edge cases
/// is_integer should correctly identify valid and invalid integer strings
test "prop_is_integer_edge_cases" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::map(@pbt.Gen::choose_int(-10000, 10000), fn(n) { n.to_string() }),
    @pbt.Gen::pure(""),
    @pbt.Gen::pure("+"),
    @pbt.Gen::pure("-"),
    @pbt.Gen::pure("0"),
    @pbt.Gen::pure("00"),
    @pbt.Gen::pure("-0"),
    @pbt.Gen::pure(" 123"),
    @pbt.Gen::pure("+-123"),
  ])
  let config = @pbt.CheckConfig::new(300, 100, 121212, 30)

  @pbt.assert_check("is_integer edge cases", gen, fn(s) {
    let result = is_integer(s)
    let is_valid = if s.length() == 0 {
      false
    } else {
      let mut i = 0
      let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
      let mut has_digit = false
      while i < s.length() {
        let c = s[i]
        if c >= '0' && c <= '9' {
          has_digit = true
        } else if i >= start {
          has_digit = false
          break
        }
        i = i + 1
      }
      start < s.length() && has_digit
    }
    if result == is_valid { Ok(()) }
    else { Err("is_integer(\{s}) = \{result}, expected \{is_valid}") }
  }, config~)
}

///|
/// Property: is_double edge cases
/// is_double should correctly identify valid and invalid double strings
test "prop_is_double_edge_cases" {
  let gen = @pbt.Gen::one_of([
    @pbt.Gen::map(@pbt.Gen::choose_int(-10000, 10000), fn(n) {
      n.to_string() + ".5"
    }),
    @pbt.Gen::map(@pbt.Gen::choose_int(-10000, 10000), fn(n) { n.to_string() }),
    @pbt.Gen::pure("."),
    @pbt.Gen::pure("1.2.3"),
    @pbt.Gen::pure(".123"),
    @pbt.Gen::pure("123."),
  ])
  let config = @pbt.CheckConfig::new(400, 100, 131313, 30)

  @pbt.assert_check("is_double edge cases", gen, fn(s) {
    let result = is_double(s)
    let is_valid = if s.length() == 0 {
      false
    } else {
      let mut dot_count = 0
      let mut has_digit = false
      let mut i = 0
      let start = if s[0] == '-' || s[0] == '+' { 1 } else { 0 }
      while i < s.length() {
        let c = s[i]
        if c == '.' {
          dot_count = dot_count + 1
        } else if c >= '0' && c <= '9' {
          has_digit = true
        } else if i >= start {
          has_digit = false
          break
        }
        i = i + 1
      }
      start < s.length() && has_digit && dot_count == 1
    }
    if result == is_valid { Ok(()) }
    else { Err("is_double(\{s}) = \{result}, expected \{is_valid}") }
  }, config~)
}

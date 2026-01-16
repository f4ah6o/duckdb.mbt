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

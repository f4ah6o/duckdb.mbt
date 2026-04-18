///|
/// Generate array of strings for map keys/values.
fn gen_string_array_local() -> @pbt.Gen[Array[String]] {
  @pbt.int_range(0, 20).bind(fn(len) {
    @pbt.Gen::fmap(array_of(@pbt.int_range(0, 100)), fn(arr) {
      let strs = arr.map(fn(n) { "key_" + n.to_string() })
      take_string_array(strs, len)
    })
  })
}

///|
/// Generate pair of arrays for map_from_arrays.
fn gen_map_arrays_local() -> @pbt.Gen[(Array[String], Array[String])] {
  @pbt.int_range(0, 10).bind(fn(len) {
    array_of(@pbt.int_range(0, 100)).bind(fn(keys_arr) {
      @pbt.Gen::fmap(array_of(@pbt.int_range(0, 100)), fn(vals_arr) {
        let keys_full = keys_arr.map(fn(n) { "key_" + n.to_string() })
        let values_full = vals_arr.map(fn(n) { "val_" + n.to_string() })
        let min_len = Int::min(
          len,
          Int::min(keys_full.length(), values_full.length()),
        )
        let keys = take_string_array(keys_full, min_len)
        let values = take_string_array(values_full, min_len)
        (keys, values)
      })
    })
  })
}

///|
/// Take the first N elements from an array (or fewer if shorter).
fn take_string_array(arr : Array[String], len : Int) -> Array[String] {
  let result : Array[String] = []
  let mut i = 0
  let target = if len < arr.length() { len } else { arr.length() }
  while i < target {
    result.push(arr[i])
    i = i + 1
  }
  result
}

///|
/// Shrink string arrays toward simpler inputs.
fn shrink_string_array_local(arr : Array[String]) -> Iter[Array[String]] {
  let len = arr.length()
  if len == 0 {
    return [].iter()
  }
  let candidates : Array[Array[String]] = [[]]
  let half = len / 2
  if half > 0 {
    candidates.push(take_string_array(arr, half))
  }
  if len > 1 {
    candidates.push(take_string_array(arr, 1))
  }
  candidates.iter()
}

///|
/// Shrink map input arrays while keeping key/value lengths aligned.
fn shrink_map_arrays_local(
  input : (Array[String], Array[String]),
) -> Iter[(Array[String], Array[String])] {
  let (keys, values) = input
  let candidates : Array[(Array[String], Array[String])] = []
  for shrunk_keys in shrink_string_array_local(keys) {
    let len = Int::min(shrunk_keys.length(), values.length())
    let trimmed_keys = take_string_array(shrunk_keys, len)
    let trimmed_values = take_string_array(values, len)
    candidates.push((trimmed_keys, trimmed_values))
  }
  for shrunk_values in shrink_string_array_local(values) {
    let len = Int::min(keys.length(), shrunk_values.length())
    let trimmed_keys = take_string_array(keys, len)
    let trimmed_values = take_string_array(shrunk_values, len)
    candidates.push((trimmed_keys, trimmed_values))
  }
  candidates.iter()
}

///|
/// Property: list_from_strings preserves length for arbitrary input arrays.
test "prop_list_from_strings_length_preserved_pbt" {
  let gen = gen_string_array_local()
  let config = CheckConfig::new(200, 20, 4242, 20)
  assert_check(
    "list_from_strings preserves length",
    gen,
    fn(arr) {
      let list = list_from_strings(arr)
      if list_length(list) == arr.length() {
        Ok(())
      } else {
        Err("Length mismatch: \{list_length(list)} vs \{arr.length()}")
      }
    },
    config~,
    shrink=shrink_string_array_local,
  )
}

///|
/// Property: map_from_arrays never produces more entries than input pairs.
test "prop_map_from_arrays_size_bounded_pbt" {
  let gen = gen_map_arrays_local()
  let config = CheckConfig::new(200, 20, 4243, 20)
  assert_check(
    "map_from_arrays size bounded by input length",
    gen,
    fn(input) {
      let (keys, values) = input
      let map = map_from_arrays(keys, values)
      if map_size(map) <= keys.length() {
        Ok(())
      } else {
        Err("Map size exceeded inputs: \{map_size(map)} vs \{keys.length()}")
      }
    },
    config~,
    shrink=shrink_map_arrays_local,
  )
}

///|
/// Property: map_from_pairs never produces more entries than input pairs.
test "prop_map_from_pairs_size_bounded_pbt" {
  let gen = gen_map_arrays_local()
  let config = CheckConfig::new(200, 20, 4244, 20)
  assert_check(
    "map_from_pairs size bounded by input length",
    gen,
    fn(input) {
      let (keys, values) = input
      let pairs : Array[(String, String)] = []
      let mut i = 0
      while i < keys.length() {
        pairs.push((keys[i], values[i]))
        i = i + 1
      }
      let map = map_from_pairs(pairs)
      if map_size(map) <= pairs.length() {
        Ok(())
      } else {
        Err("Map size exceeded pairs: \{map_size(map)} vs \{pairs.length()}")
      }
    },
    config~,
    shrink=shrink_map_arrays_local,
  )
}

///|
fn pow10_int(exp : Int) -> Int {
  let mut result = 1
  let mut i = 0
  while i < exp {
    result = result * 10
    i = i + 1
  }
  result
}

///|
fn gen_valid_date() -> @pbt.Gen[(Int, Int, Int)] {
  @pbt.int_range(1970, 2100).bind(fn(year) {
    @pbt.int_range(1, 12).bind(fn(month) {
      let max_day = days_in_month(year, month)
      @pbt.Gen::fmap(@pbt.int_range(1, max_day), fn(day) { (year, month, day) })
    })
  })
}

///|
test "prop_date_roundtrip_valid_ymd" {
  let gen = gen_valid_date()
  let config = CheckConfig::new(200, 20, 730001, 20)
  assert_check(
    "date_from_ymd/date_to_ymd round-trip",
    gen,
    fn(input) {
      let (year, month, day) = input
      let days = date_from_ymd(year, month, day)
      let (y2, m2, d2) = date_to_ymd(days)
      if year == y2 && month == m2 && day == d2 {
        Ok(())
      } else {
        Err(
          "round-trip failed: (\{year},\{month},\{day}) -> (\{y2},\{m2},\{d2})",
        )
      }
    },
    config~,
  )
}

///|
fn gen_valid_timestamp_parts() -> @pbt.Gen[(Int, Int, Int, Int, Int, Int)] {
  gen_valid_date().bind(fn(date) {
    let (year, month, day) = date
    @pbt.int_range(0, 23).bind(fn(hour) {
      @pbt.int_range(0, 59).bind(fn(minute) {
        @pbt.Gen::fmap(@pbt.int_range(0, 59), fn(second) {
          (year, month, day, hour, minute, second)
        })
      })
    })
  })
}

///|
test "prop_timestamp_roundtrip_valid_ymd_hms" {
  let gen = gen_valid_timestamp_parts()
  let config = CheckConfig::new(200, 20, 730002, 20)
  assert_check(
    "timestamp_from_ymd_hms/timestamp_to_ymd_hms round-trip",
    gen,
    fn(input) {
      let (year, month, day, hour, minute, second) = input
      let micros = timestamp_from_ymd_hms(
        year, month, day, hour, minute, second,
      )
      let (y2, m2, d2, h2, min2, s2) = timestamp_to_ymd_hms(micros)
      if year == y2 &&
        month == m2 &&
        day == d2 &&
        hour == h2 &&
        minute == min2 &&
        second == s2 {
        Ok(())
      } else {
        Err(
          "round-trip failed: (\{year},\{month},\{day},\{hour},\{minute},\{second}) -> (\{y2},\{m2},\{d2},\{h2},\{min2},\{s2})",
        )
      }
    },
    config~,
  )
}

///|
fn gen_decimal_parts() -> @pbt.Gen[(Int, Int, Int)] {
  @pbt.int_range(0, 6).bind(fn(scale) {
    let max_frac = if scale == 0 { 0 } else { pow10_int(scale) - 1 }
    @pbt.int_range(0, 1000).bind(fn(whole) {
      let frac_gen = if max_frac == 0 {
        @pbt.pure(0)
      } else {
        @pbt.int_range(0, max_frac)
      }
      @pbt.Gen::fmap(frac_gen, fn(frac) { (whole, frac, scale) })
    })
  })
}

///|
test "prop_decimal_parts_roundtrip" {
  let gen = gen_decimal_parts()
  let config = CheckConfig::new(200, 20, 730003, 20)
  assert_check(
    "decimal_from_parts/decimal_to_parts round-trip",
    gen,
    fn(input) {
      let (whole, frac, scale) = input
      let dec = decimal_from_parts(whole, frac, scale)
      let (whole2, frac2) = decimal_to_parts(dec)
      if whole == whole2 && frac == frac2 {
        Ok(())
      } else {
        Err(
          "round-trip failed: (\{whole},\{frac},\{scale}) -> (\{whole2},\{frac2})",
        )
      }
    },
    config~,
  )
}

///|
/// Unit-scale property: interval_from_seconds(n) yields n * 1_000_000 micros.
test "prop_interval_from_seconds_scales_to_micros" {
  let gen = @pbt.int_range(-1000000, 1000000)
  let config = CheckConfig::new(200, 20, 810001, 20)
  assert_check(
    "interval_from_seconds * 1_000_000 == interval_to_micros",
    gen,
    fn(n) {
      let iv = interval_from_seconds(n)
      let expected = n.to_int64() * 1000000L
      let actual = interval_to_micros(iv)
      if actual == expected {
        Ok(())
      } else {
        Err("seconds=\{n}: expected \{expected} got \{actual}")
      }
    },
    config~,
  )
}

///|
/// Unit-scale property: interval_from_minutes(n) yields n * 60 * 1_000_000 micros.
test "prop_interval_from_minutes_scales_to_micros" {
  let gen = @pbt.int_range(-100000, 100000)
  let config = CheckConfig::new(200, 20, 810002, 20)
  assert_check(
    "interval_from_minutes * 60_000_000 == interval_to_micros",
    gen,
    fn(n) {
      let iv = interval_from_minutes(n)
      let expected = n.to_int64() * 60L * 1000000L
      let actual = interval_to_micros(iv)
      if actual == expected {
        Ok(())
      } else {
        Err("minutes=\{n}: expected \{expected} got \{actual}")
      }
    },
    config~,
  )
}

///|
/// Unit-scale property: interval_from_hours(n) yields n * 3600 * 1_000_000 micros.
test "prop_interval_from_hours_scales_to_micros" {
  let gen = @pbt.int_range(-10000, 10000)
  let config = CheckConfig::new(200, 20, 810003, 20)
  assert_check(
    "interval_from_hours * 3_600_000_000 == interval_to_micros",
    gen,
    fn(n) {
      let iv = interval_from_hours(n)
      let expected = n.to_int64() * 3600L * 1000000L
      let actual = interval_to_micros(iv)
      if actual == expected {
        Ok(())
      } else {
        Err("hours=\{n}: expected \{expected} got \{actual}")
      }
    },
    config~,
  )
}

///|
/// Unit-scale property: interval_from_days(n) yields n * 86400 * 1_000_000 micros.
test "prop_interval_from_days_scales_to_micros" {
  let gen = @pbt.int_range(-10000, 10000)
  let config = CheckConfig::new(200, 20, 810004, 20)
  assert_check(
    "interval_from_days * 86_400_000_000 == interval_to_micros",
    gen,
    fn(n) {
      let iv = interval_from_days(n)
      let expected = n.to_int64() * 86400L * 1000000L
      let actual = interval_to_micros(iv)
      if actual == expected {
        Ok(())
      } else {
        Err("days=\{n}: expected \{expected} got \{actual}")
      }
    },
    config~,
  )
}

///|
/// Invariant: months do not contribute to interval_to_micros.
test "prop_interval_from_months_micros_zero" {
  let gen = @pbt.int_range(-100000, 100000)
  let config = CheckConfig::new(200, 20, 810005, 20)
  assert_check(
    "interval_from_months carries no micros",
    gen,
    fn(n) {
      let iv = interval_from_months(n)
      let actual = interval_to_micros(iv)
      if actual == 0L {
        Ok(())
      } else {
        Err("months=\{n}: expected 0 got \{actual}")
      }
    },
    config~,
  )
}

///|
fn gen_interval_parts() -> @pbt.Gen[(Int, Int, Int64)] {
  @pbt.int_range(-100000, 100000).bind(fn(months) {
    @pbt.int_range(-10000, 10000).bind(fn(days) {
      @pbt.Gen::fmap(@pbt.int_range(-1000000, 1000000), fn(micros_int) {
        (months, days, micros_int.to_int64())
      })
    })
  })
}

///|
/// Invariant: interval_from_parts(months, days, micros) → days * 86_400_000_000 + micros.
test "prop_interval_from_parts_to_micros" {
  let gen = gen_interval_parts()
  let config = CheckConfig::new(200, 20, 810006, 20)
  assert_check(
    "interval_from_parts yields days*86_400_000_000 + micros",
    gen,
    fn(input) {
      let (months, days, micros) = input
      let iv = interval_from_parts(months, days, micros)
      let expected = days.to_int64() * 86400L * 1000000L + micros
      let actual = interval_to_micros(iv)
      if actual == expected {
        Ok(())
      } else {
        Err(
          "parts=(\{months},\{days},\{micros}): expected \{expected} got \{actual}",
        )
      }
    },
    config~,
  )
}

///|
/// Round-trip: list_get(list_from_strings(arr), i) == Some(arr[i]) for valid i.
test "prop_list_get_roundtrip" {
  let gen = gen_string_array_local()
  let config = CheckConfig::new(200, 20, 810007, 20)
  assert_check(
    "list_get returns the element at each valid index",
    gen,
    fn(arr) {
      let list = list_from_strings(arr)
      let mut i = 0
      while i < arr.length() {
        match list_get(list, i) {
          Some(v) =>
            if v != arr[i] {
              return Err("list_get(\{i}) = \{v} but arr[\{i}] = \{arr[i]}")
            }
          None =>
            return Err(
              "list_get(\{i}) = None for arr of length \{arr.length()}",
            )
        }
        i = i + 1
      }
      Ok(())
    },
    config~,
    shrink=shrink_string_array_local,
  )
}

///|
/// Boundary: list_get returns None for out-of-range indices.
test "prop_list_get_out_of_range" {
  let gen = gen_string_array_local()
  let config = CheckConfig::new(200, 20, 810008, 20)
  assert_check(
    "list_get returns None outside [0, length)",
    gen,
    fn(arr) {
      let list = list_from_strings(arr)
      match list_get(list, -1) {
        Some(v) => return Err("list_get(-1) = Some(\{v}), expected None")
        None => ()
      }
      match list_get(list, arr.length()) {
        Some(v) =>
          return Err("list_get(\{arr.length()}) = Some(\{v}), expected None")
        None => ()
      }
      Ok(())
    },
    config~,
    shrink=shrink_string_array_local,
  )
}

///|
/// Round-trip: map_get returns the value associated with any key from map_from_arrays.
test "prop_map_get_returns_known_value" {
  let gen = gen_map_arrays_local()
  let config = CheckConfig::new(200, 20, 810009, 20)
  assert_check(
    "map_get returns the value bound to the first occurrence of each key",
    gen,
    fn(input) {
      let (keys, values) = input
      let map = map_from_arrays(keys, values)
      let mut i = 0
      while i < keys.length() {
        let key = keys[i]
        let mut first = 0
        while keys[first] != key {
          first = first + 1
        }
        match map_get(map, key) {
          Some(v) =>
            if v != values[first] {
              return Err("map_get(\{key}) = \{v}, expected \{values[first]}")
            }
          None => return Err("map_get(\{key}) = None, expected Some")
        }
        i = i + 1
      }
      Ok(())
    },
    config~,
    shrink=shrink_map_arrays_local,
  )
}

///|
/// Roundtrip: struct_from_pairs preserves fields in order.
test "prop_struct_from_pairs_field_count_and_get" {
  let gen = gen_map_arrays_local()
  let config = CheckConfig::new(200, 20, 810010, 20)
  assert_check(
    "struct_from_pairs preserves field_count and round-trips via struct_get",
    gen,
    fn(input) {
      let (keys, values) = input
      let pairs : Array[(String, String)] = []
      let mut i = 0
      while i < keys.length() {
        pairs.push((keys[i], values[i]))
        i = i + 1
      }
      let s = struct_from_pairs(pairs)
      if struct_field_count(s) != pairs.length() {
        return Err(
          "field_count=\{struct_field_count(s)} vs pairs=\{pairs.length()}",
        )
      }
      let mut j = 0
      while j < keys.length() {
        let key = keys[j]
        let mut first = 0
        while keys[first] != key {
          first = first + 1
        }
        match struct_get(s, key) {
          Some(v) =>
            if v != values[first] {
              return Err("struct_get(\{key}) = \{v}, expected \{values[first]}")
            }
          None => return Err("struct_get(\{key}) = None, expected Some")
        }
        j = j + 1
      }
      Ok(())
    },
    config~,
    shrink=shrink_map_arrays_local,
  )
}

///|
/// Equivalence: struct_from_arrays and struct_from_pairs produce the same field_count.
test "prop_struct_from_arrays_matches_from_pairs" {
  let gen = gen_map_arrays_local()
  let config = CheckConfig::new(200, 20, 810011, 20)
  assert_check(
    "struct_from_arrays and struct_from_pairs agree on field_count",
    gen,
    fn(input) {
      let (keys, values) = input
      let from_arrays = struct_from_arrays(keys, values)
      let pairs : Array[(String, String)] = []
      let mut i = 0
      while i < keys.length() {
        pairs.push((keys[i], values[i]))
        i = i + 1
      }
      let from_pairs = struct_from_pairs(pairs)
      if struct_field_count(from_arrays) == struct_field_count(from_pairs) {
        Ok(())
      } else {
        Err(
          "arrays=\{struct_field_count(from_arrays)} vs pairs=\{struct_field_count(from_pairs)}",
        )
      }
    },
    config~,
    shrink=shrink_map_arrays_local,
  )
}

///|
/// is_special_float_string returns true for each known special marker.
test "prop_is_special_float_string_accepts_known" {
  let specials = ["nan", "NaN", "inf", "Infinity", "-inf", "-Infinity"]
  let gen = @pbt.Gen::fmap(@pbt.int_range(0, specials.length()), fn(i) {
    specials[i]
  })
  let config = CheckConfig::new(200, 20, 810012, 20)
  assert_check(
    "is_special_float_string(known) == true",
    gen,
    fn(s) {
      if is_special_float_string(s) {
        Ok(())
      } else {
        Err("is_special_float_string(\{s}) = false, expected true")
      }
    },
    config~,
  )
}

///|
/// is_special_float_string rejects ordinary integer-looking strings.
test "prop_is_special_float_string_rejects_integers" {
  let gen = @pbt.Gen::fmap(@pbt.int_range(-100000, 100000), fn(n) {
    n.to_string()
  })
  let config = CheckConfig::new(200, 20, 810013, 20)
  assert_check(
    "is_special_float_string rejects integer strings",
    gen,
    fn(s) {
      if is_special_float_string(s) {
        Err("is_special_float_string(\{s}) = true, expected false")
      } else {
        Ok(())
      }
    },
    config~,
  )
}

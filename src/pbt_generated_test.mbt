///|
/// Generate array of strings for map keys/values.
fn gen_string_array_local() -> @pbt.Gen[Array[String]] {
  @pbt.Gen::choose_int(0, 20).bind(fn(len) {
    @pbt.Gen::array_of(@pbt.Gen::choose_int(0, 100)).map(fn(arr) {
      let strs = arr.map(fn(n) { "key_" + n.to_string() })
      take_string_array(strs, len)
    })
  })
}

///|
/// Generate pair of arrays for map_from_arrays.
fn gen_map_arrays_local() -> @pbt.Gen[(Array[String], Array[String])] {
  @pbt.Gen::choose_int(0, 10).bind(fn(len) {
    @pbt.Gen::array_of(@pbt.Gen::choose_int(0, 100)).bind(fn(keys_arr) {
      @pbt.Gen::array_of(@pbt.Gen::choose_int(0, 100)).map(fn(vals_arr) {
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
  let config = @pbt.CheckConfig::new(200, 20, 4242, 20)
  @pbt.assert_check(
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
  let config = @pbt.CheckConfig::new(200, 20, 4243, 20)
  @pbt.assert_check(
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
  let config = @pbt.CheckConfig::new(200, 20, 4244, 20)
  @pbt.assert_check(
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
  @pbt.Gen::choose_int(1970, 2100).bind(fn(year) {
    @pbt.Gen::choose_int(1, 12).bind(fn(month) {
      let max_day = days_in_month(year, month)
      @pbt.Gen::choose_int(1, max_day).map(fn(day) { (year, month, day) })
    })
  })
}

///|
test "prop_date_roundtrip_valid_ymd" {
  let gen = gen_valid_date()
  let config = @pbt.CheckConfig::new(200, 20, 730001, 20)
  @pbt.assert_check(
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
    @pbt.Gen::choose_int(0, 23).bind(fn(hour) {
      @pbt.Gen::choose_int(0, 59).bind(fn(minute) {
        @pbt.Gen::choose_int(0, 59).map(fn(second) {
          (year, month, day, hour, minute, second)
        })
      })
    })
  })
}

///|
test "prop_timestamp_roundtrip_valid_ymd_hms" {
  let gen = gen_valid_timestamp_parts()
  let config = @pbt.CheckConfig::new(200, 20, 730002, 20)
  @pbt.assert_check(
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
  @pbt.Gen::choose_int(0, 6).bind(fn(scale) {
    let max_frac = if scale == 0 { 0 } else { pow10_int(scale) - 1 }
    @pbt.Gen::choose_int(0, 1000).bind(fn(whole) {
      @pbt.Gen::choose_int(0, max_frac).map(fn(frac) { (whole, frac, scale) })
    })
  })
}

///|
test "prop_decimal_parts_roundtrip" {
  let gen = gen_decimal_parts()
  let config = @pbt.CheckConfig::new(200, 20, 730003, 20)
  @pbt.assert_check(
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

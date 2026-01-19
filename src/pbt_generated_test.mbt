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

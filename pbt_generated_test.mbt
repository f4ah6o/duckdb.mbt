///|
/// Property-Based Tests for duckdb.mbt
/// Auto-generated from PBT markdown
///

///|
/// Date Round-Trip: date_from_ymd / date_to_ymd preserves year, month, day
test "prop_date_from_ymd_to_ymd_roundtrip" {
  // Test epoch
  let (y1, m1, d1) = date_to_ymd(date_from_ymd(1970, 1, 1))
  inspect((y1, m1, d1), content="(1970, 1, 1)")

  // Test common dates
  let (y2, m2, d2) = date_to_ymd(date_from_ymd(2024, 6, 15))
  inspect((y2, m2, d2), content="(2024, 6, 15)")

  // Test leap year
  let (y3, m3, d3) = date_to_ymd(date_from_ymd(2024, 2, 29))
  inspect((y3, m3, d3), content="(2024, 2, 29)")

  // Test month boundaries
  let (y4, m4, d4) = date_to_ymd(date_from_ymd(2024, 1, 31))
  inspect((y4, m4, d4), content="(2024, 1, 31)")
  let (y5, m5, d5) = date_to_ymd(date_from_ymd(2024, 12, 31))
  inspect((y5, m5, d5), content="(2024, 12, 31)")
}

///|
/// Timestamp Round-Trip: timestamp_from_ymd_hms / timestamp_to_ymd_hms preserves datetime
test "prop_timestamp_from_ymd_hms_to_ymd_hms_roundtrip" {
  // Test epoch
  let (y1, m1, d1, h1, min1, s1) = timestamp_to_ymd_hms(
    timestamp_from_ymd_hms(1970, 1, 1, 0, 0, 0),
  )
  inspect((y1, m1, d1, h1, min1, s1), content="(1970, 1, 1, 0, 0, 0)")

  // Test common datetime
  let (y2, m2, d2, h2, min2, s2) = timestamp_to_ymd_hms(
    timestamp_from_ymd_hms(2024, 6, 3, 12, 34, 56),
  )
  inspect((y2, m2, d2, h2, min2, s2), content="(2024, 6, 3, 12, 34, 56)")

  // Test future date
  let (y3, m3, d3, h3, min3, s3) = timestamp_to_ymd_hms(
    timestamp_from_ymd_hms(2050, 12, 31, 23, 59, 59),
  )
  inspect((y3, m3, d3, h3, min3, s3), content="(2050, 12, 31, 23, 59, 59)")
}

///|
/// Decimal Round-Trip: decimal_from_double / decimal_to_double preserves value
test "prop_decimal_from_double_to_double_roundtrip" {
  // Test simple values
  let d1 = decimal_from_double(123.45, 20, 2)
  let v1 = decimal_to_double(d1)
  inspect(Double::abs(v1 - 123.45) < 0.01, content="true")

  // Test zero
  let d2 = decimal_from_double(0.0, 20, 2)
  let v2 = decimal_to_double(d2)
  inspect(Double::abs(v2) < 0.001, content="true")

  // Test negative value
  let d3 = decimal_from_double(-99.99, 20, 2)
  let v3 = decimal_to_double(d3)
  inspect(Double::abs(v3 - -99.99) < 0.01, content="true")
}

///|
/// Decimal Round-Trip: decimal_from_parts / decimal_to_parts preserves components
test "prop_decimal_from_parts_to_parts_roundtrip" {
  // Test positive value
  let d1 = decimal_from_parts(123, 45, 2)
  let (w1, f1) = decimal_to_parts(d1)
  inspect((w1, f1), content="(123, 45)")

  // Test zero
  let d2 = decimal_from_parts(0, 0, 2)
  let (w2, f2) = decimal_to_parts(d2)
  inspect((w2, f2), content="(0, 0)")

  // Test negative value
  let d3 = decimal_from_parts(-100, 50, 2)
  let (w3, f3) = decimal_to_parts(d3)
  inspect((w3, f3), content="(-100, 50)")
}

///|
/// Interval: interval_from_parts / interval_to_micros correctly converts to microseconds
test "prop_interval_from_parts_to_micros" {
  // Test 1 day in microseconds
  let i1 = interval_from_parts(0, 1, 0L)
  let micros1 = interval_to_micros(i1)
  inspect(micros1, content="86400000000")

  // Test 1000 microseconds
  let i2 = interval_from_parts(0, 0, 1000L)
  let micros2 = interval_to_micros(i2)
  inspect(micros2, content="1000")

  // Test combined: 1 day + 1000 microseconds
  let i3 = interval_from_parts(0, 1, 1000L)
  let micros3 = interval_to_micros(i3)
  inspect(micros3, content="86400001000")
}

///|
/// List: list_from_strings preserves array length
test "prop_list_from_strings_length_preserved" {
  let arr1 = []
  let l1 = list_from_strings(arr1)
  inspect(list_length(l1), content="0")
  let arr2 = ["a", "b", "c"]
  let l2 = list_from_strings(arr2)
  inspect(list_length(l2), content="3")
  let arr3 = ["x"]
  let l3 = list_from_strings(arr3)
  inspect(list_length(l3), content="1")
}

///|
/// List: list_get returns correct element by index
test "prop_list_get_element_access" {
  let arr = ["apple", "banana", "cherry"]
  let list = list_from_strings(arr)
  inspect(
    list_get(list, 0),
    content=(
      #|Some("apple")
    ),
  )
  inspect(
    list_get(list, 1),
    content=(
      #|Some("banana")
    ),
  )
  inspect(
    list_get(list, 2),
    content=(
      #|Some("cherry")
    ),
  )
  inspect(list_get(list, 3), content="None")
  inspect(list_get(list, -1), content="None")
}

///|
/// Struct: struct_from_pairs preserves field count
test "prop_struct_from_pairs_field_count_preserved" {
  let pairs1 = []
  let s1 = struct_from_pairs(pairs1)
  inspect(struct_field_count(s1), content="0")
  let pairs2 = [("name", "Alice"), ("age", "30")]
  let s2 = struct_from_pairs(pairs2)
  inspect(struct_field_count(s2), content="2")
  let pairs3 = [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]
  let s3 = struct_from_pairs(pairs3)
  inspect(struct_field_count(s3), content="4")
}

///|
/// Struct: struct_get returns correct field value by name
test "prop_struct_get_field_access" {
  let pairs = [("name", "Alice"), ("age", "30"), ("city", "Tokyo")]
  let s = struct_from_pairs(pairs)
  inspect(
    struct_get(s, "name"),
    content=(
      #|Some("Alice")
    ),
  )
  inspect(
    struct_get(s, "age"),
    content=(
      #|Some("30")
    ),
  )
  inspect(
    struct_get(s, "city"),
    content=(
      #|Some("Tokyo")
    ),
  )
  inspect(struct_get(s, "missing"), content="None")
}

///|
/// Map: map_from_pairs preserves entry count
test "prop_map_from_pairs_size_preserved" {
  let pairs1 = []
  let m1 = map_from_pairs(pairs1)
  inspect(map_size(m1), content="0")
  let pairs2 = [("key1", "value1"), ("key2", "value2")]
  let m2 = map_from_pairs(pairs2)
  inspect(map_size(m2), content="2")
  let pairs3 = [("a", "1"), ("b", "2"), ("c", "3")]
  let m3 = map_from_pairs(pairs3)
  inspect(map_size(m3), content="3")
}

///|
/// Map: map_get returns correct value by key
test "prop_map_get_key_lookup" {
  let pairs = [("apple", "red"), ("banana", "yellow"), ("grape", "purple")]
  let m = map_from_pairs(pairs)
  inspect(
    map_get(m, "apple"),
    content=(
      #|Some("red")
    ),
  )
  inspect(
    map_get(m, "banana"),
    content=(
      #|Some("yellow")
    ),
  )
  inspect(
    map_get(m, "grape"),
    content=(
      #|Some("purple")
    ),
  )
  inspect(map_get(m, "orange"), content="None")
}

///|
/// Map: map_from_arrays preserves size
test "prop_map_from_arrays_preserves_size" {
  let keys1 = []
  let values1 = []
  let m1 = map_from_arrays(keys1, values1)
  inspect(map_size(m1), content="0")
  let keys2 = ["a", "b", "c"]
  let values2 = ["1", "2", "3"]
  let m2 = map_from_arrays(keys2, values2)
  inspect(map_size(m2), content="3")
}

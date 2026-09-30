///|
// Whitebox tests for `js_pack_chunk` (issue #68): verify the pool layout the
// `js_appender_append_chunk_ffi` boundary consumes — kind tags, per-kind
// offsets, packed payloads, and the row*ncols+col null grid.

///|
test "js_pack_chunk pools flat columns" {
  let chunk : VectorChunk = {
    columns: ["b", "i", "s", "u"],
    vectors: [
      Vector::all_valid(
        ColumnType::Boolean,
        VectorData::Bool(FixedArray::from_array([true, false])),
      ),
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array([10, 20])),
      ),
      Vector::all_valid(ColumnType::Varchar, VectorData::Varchar(["x", "y"])),
      Vector::{
        logical_type: ColumnType::BigInt,
        data: VectorData::Int64(FixedArray::from_array([1L, 2L])),
        validity: FixedArray::from_array([true, false]),
      },
    ],
  }
  let (
    kinds,
    offsets,
    null_flags,
    pool_i32,
    pool_i64,
    pool_f64,
    pool_str,
    pool_bytes,
  ) = js_pack_chunk(chunk, 2)
  inspect(Array::from_fixed_array(kinds), content="[0, 1, 6, 2]")
  inspect(Array::from_fixed_array(offsets), content="[0, 2, 0, 0]")
  inspect(
    Array::from_fixed_array(null_flags),
    content="[0, 0, 0, 0, 0, 0, 0, 1]",
  )
  inspect(Array::from_fixed_array(pool_i32), content="[1, 0, 10, 20]")
  inspect(Array::from_fixed_array(pool_i64), content="[1, 2]")
  inspect(Array::from_fixed_array(pool_f64), content="[]")
  inspect(Array::from_fixed_array(pool_str), content="[x, y]")
  inspect(pool_bytes.length(), content="0")
}

///|
test "js_pack_chunk packs 128-bit and structured payload columns" {
  let decimals = FixedArray::from_array([
    decimal_from_hugeint(10UL, -1L, 18, 2), // negative 128-bit payload
    decimal_from_hugeint(5UL, 0L, 18, 2),
  ])
  let intervals = FixedArray::from_array([
    interval_from_parts(3, 4, 500L),
    interval_from_parts(0, 1, 0L),
  ])
  let chunk : VectorChunk = {
    columns: ["d", "iv", "h"],
    vectors: [
      Vector::all_valid(ColumnType::Decimal, VectorData::Decimal(decimals)),
      Vector::all_valid(ColumnType::Interval, VectorData::Interval(intervals)),
      Vector::all_valid(
        ColumnType::HugeInt,
        VectorData::HugeInt(
          lower=FixedArray::from_array([9UL, 8UL]),
          upper=FixedArray::from_array([7L, 6L]),
        ),
      ),
    ],
  }
  let (
    kinds,
    offsets,
    _null_flags,
    _pool_i32,
    pool_i64,
    _pool_f64,
    _pool_str,
    _pool_bytes,
  ) = js_pack_chunk(chunk, 2)
  inspect(Array::from_fixed_array(kinds), content="[9, 10, 8]")
  // Decimal pairs, interval triples, then hugeint pairs share pool_i64.
  inspect(Array::from_fixed_array(offsets), content="[0, 4, 10]")
  inspect(
    Array::from_fixed_array(pool_i64),
    content="[-1, 10, 0, 5, 3, 4, 500, 0, 1, 0, 7, 9, 6, 8]",
  )
}

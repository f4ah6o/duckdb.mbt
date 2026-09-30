///|
// ============================================================================
// Native backend: `Appender::append_chunk` (issue #68).
// ============================================================================
//
// Builds a `duckdb_data_chunk` from the canonical `VectorChunk`, bulk-copies
// each column payload into the matching `duckdb_vector`, then hands the whole
// chunk to `duckdb_append_data_chunk`. Bulk inserts therefore cost one FFI
// call per chunk instead of one per cell, while NULLs and typed payloads are
// preserved exactly (the C writers are symmetric with the streamed readers
// in `duckdb_vector_native.mbt`).

// ----------------------------------------------------------------------------
// DataChunk FFI declarations
// ----------------------------------------------------------------------------

///|
/// Create a `duckdb_mb_data_chunk` with `ncols` columns from DuckDB type ids;
/// `aux[i]` carries `(width << 16) | scale` for DECIMAL columns.
#borrow(type_ids, aux)
extern "C" fn native_create_data_chunk_typed(
  type_ids : FixedArray[Int],
  aux : FixedArray[Int],
  ncols : Int,
) -> NativeDataChunk = "duckdb_mb_create_data_chunk_typed"

///|
#borrow(chunk)
extern "C" fn native_destroy_data_chunk(chunk : NativeDataChunk) = "duckdb_mb_destroy_data_chunk"

///|
/// Borrowed vector handle for column `col` of a created chunk (valid only
/// while the chunk is alive).
#borrow(chunk)
extern "C" fn native_data_chunk_get_vector(
  chunk : NativeDataChunk,
  col : Int,
) -> NativeVector = "duckdb_mb_data_chunk_get_vector"

///|
#borrow(chunk)
extern "C" fn native_data_chunk_set_size(chunk : NativeDataChunk, size : Int64) = "duckdb_mb_data_chunk_set_size"

///|
#borrow(chunk)
extern "C" fn native_is_null_data_chunk(chunk : NativeDataChunk) -> Bool = "duckdb_mb_is_null_data_chunk"

///|
#borrow(append, chunk)
extern "C" fn native_appender_append_data_chunk(
  append : Appender,
  chunk : NativeDataChunk,
) -> Bool = "duckdb_mb_append_data_chunk"

// ----------------------------------------------------------------------------
// Vector writer FFI declarations
// ----------------------------------------------------------------------------

///|
/// Write 1=valid/0=null flags into the vector's validity mask.
#borrow(vector, flags)
extern "C" fn native_vector_write_validity(
  vector : NativeVector,
  flags : FixedArray[Int],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_validity"

///|
#borrow(vector, flags)
extern "C" fn native_vector_write_bool(
  vector : NativeVector,
  flags : FixedArray[Int],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_bool"

///|
/// TINYINT/SMALLINT/INTEGER/UTINYINT/USMALLINT/DATE payload (int32 values,
/// narrowed by `type_id` on the C side).
#borrow(vector, values)
extern "C" fn native_vector_write_i32(
  vector : NativeVector,
  type_id : Int,
  values : FixedArray[Int],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_i32"

///|
/// BIGINT/UINTEGER/TIME/TIME_NS/TIMESTAMP* payload (int64 values; TIMESTAMP_S
/// and TIMESTAMP_MS are converted back from canonical micros on the C side).
#borrow(vector, values)
extern "C" fn native_vector_write_i64(
  vector : NativeVector,
  type_id : Int,
  values : FixedArray[Int64],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_i64"

///|
/// UBIGINT payload (raw uint64 bits).
#borrow(vector, values)
extern "C" fn native_vector_write_u64(
  vector : NativeVector,
  values : FixedArray[UInt64],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_u64"

///|
#borrow(vector, values)
extern "C" fn native_vector_write_f32(
  vector : NativeVector,
  values : FixedArray[Float],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_f32"

///|
#borrow(vector, values)
extern "C" fn native_vector_write_f64(
  vector : NativeVector,
  values : FixedArray[Double],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_f64"

///|
/// VARCHAR/BLOB payload (ref array of utf8/raw byte strings, assigned via
/// `duckdb_vector_assign_string_element_len`).
#borrow(vector, values)
extern "C" fn native_vector_write_strings(
  vector : NativeVector,
  values : FixedArray[Bytes],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_strings"

///|
/// HUGEINT/UHUGEINT/UUID 128-bit payload as separate `upper`/`lower` halves;
/// `type_id` selects the sign-bit convention (UUID flips the top bit).
#borrow(vector, upper, lower)
extern "C" fn native_vector_write_i128(
  vector : NativeVector,
  type_id : Int,
  upper : FixedArray[Int64],
  lower : FixedArray[UInt64],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_i128"

///|
/// DECIMAL 128-bit payload; the internal physical width comes from the
/// column's logical type on the C side.
#borrow(vector, upper, lower)
extern "C" fn native_vector_write_decimal(
  vector : NativeVector,
  upper : FixedArray[Int64],
  lower : FixedArray[UInt64],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_decimal"

///|
/// INTERVAL payload flattened to `[months, days]` pairs plus `micros`.
#borrow(vector, months_days, micros)
extern "C" fn native_vector_write_interval(
  vector : NativeVector,
  months_days : FixedArray[Int],
  micros : FixedArray[Int64],
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_interval"

// ----------------------------------------------------------------------------
// Implementation
// ----------------------------------------------------------------------------

///|
/// 1=valid/0=null flags for the C validity writer.
fn native_validity_flags(vector : Vector) -> FixedArray[Int] {
  FixedArray::makei(vector.validity.length(), fn(i) {
    if vector.validity[i] {
      1
    } else {
      0
    }
  })
}

///|
/// Copy one `Vector`'s validity mask and payload into the borrowed
/// `duckdb_vector` `target`. `type_id` is the column's DuckDB type id from
/// `append_chunk_specs`.
fn write_column_native(
  vector : Vector,
  target : NativeVector,
  type_id : Int,
  rows : Int,
) -> Result[Unit, DuckDBError] {
  let write_error = fn() {
    append_error("append_chunk: failed to write column data")
  }
  if !native_vector_write_validity(target, native_validity_flags(vector), rows) {
    return Err(write_error())
  }
  let ok = match vector.data {
    Bool(values) =>
      native_vector_write_bool(
        target,
        FixedArray::makei(rows, fn(i) { if values[i] { 1 } else { 0 } }),
        rows,
      )
    Int32(values) => native_vector_write_i32(target, type_id, values, rows)
    Int64(values) => native_vector_write_i64(target, type_id, values, rows)
    UInt64(values) => native_vector_write_u64(target, values, rows)
    Float(values) => native_vector_write_f32(target, values, rows)
    Double(values) => native_vector_write_f64(target, values, rows)
    Varchar(values) =>
      native_vector_write_strings(
        target,
        FixedArray::makei(rows, fn(i) { @encoding/utf8.encode(values[i]) }),
        rows,
      )
    Blob(values) =>
      native_vector_write_strings(target, FixedArray::from_array(values), rows)
    HugeInt(lower~, upper~) =>
      native_vector_write_i128(target, type_id, upper, lower, rows)
    Decimal(cells) => {
      let upper = FixedArray::make(rows, 0L)
      let lower = FixedArray::make(rows, 0UL)
      for i in 0..<rows {
        upper[i] = cells[i].upper
        lower[i] = cells[i].lower
      }
      native_vector_write_decimal(target, upper, lower, rows)
    }
    Interval(cells) => {
      let months_days = FixedArray::make(rows * 2, 0)
      let micros = FixedArray::make(rows, 0L)
      for i in 0..<rows {
        months_days[i * 2] = cells[i].months
        months_days[i * 2 + 1] = cells[i].days
        micros[i] = cells[i].micros
      }
      native_vector_write_interval(target, months_days, micros, rows)
    }
    // Rejected by `append_chunk_specs` before any vector is written.
    List(..) | Struct(..) | Map(..) | Any(_) => false
  }
  if ok {
    Ok(())
  } else {
    Err(write_error())
  }
}

///|
/// Bulk-append a canonical `VectorChunk` in one shot: each column's payload
/// array is copied straight into a DuckDB data chunk and the whole chunk is
/// appended with `duckdb_append_data_chunk` — no per-cell FFI calls.
///
/// Supported payloads mirror the streamed-reader set: BOOLEAN, all integer
/// widths, DATE, TIME/TIME_NS, every TIMESTAMP variant, FLOAT/DOUBLE,
/// VARCHAR, BLOB, DECIMAL (uniform width/scale per column), INTERVAL, and
/// HUGEINT/UHUGEINT/UUID. LIST/STRUCT/MAP/ARRAY and `Any`-vector columns
/// return `Unsupported` — the row-wise `Appender` API still covers
/// VARCHAR-element nested columns.
///
/// The chunk is validated up front: an empty chunk, mismatched column
/// names/vector counts, ragged columns, or a logical type that disagrees
/// with its payload produce `InvalidArgument` before any data is written.
pub fn Appender::append_chunk(
  self : Appender,
  chunk : VectorChunk,
) -> Result[Unit, DuckDBError] {
  match self.capabilities().require(BackendFeature::AppenderChunk) {
    Err(err) => return Err(err)
    Ok(_) => ()
  }
  let (rows, type_ids, aux) = match append_chunk_specs(chunk, self.backend()) {
    Ok(specs) => specs
    Err(err) => return Err(err)
  }
  let ncols = type_ids.length()
  let data_chunk = native_create_data_chunk_typed(type_ids, aux, ncols)
  if native_is_null_data_chunk(data_chunk) {
    return Err(
      append_error(last_error("append_chunk: failed to create data chunk")),
    )
  }
  for col = 0; col < ncols; col = col + 1 {
    let target = native_data_chunk_get_vector(data_chunk, col)
    match write_column_native(chunk.vectors[col], target, type_ids[col], rows) {
      Err(err) => {
        native_destroy_data_chunk(data_chunk)
        return Err(err)
      }
      Ok(_) => ()
    }
  } nobreak {
    ()
  }
  native_data_chunk_set_size(data_chunk, rows.to_int64())
  let ok = native_appender_append_data_chunk(self, data_chunk)
  native_destroy_data_chunk(data_chunk)
  if ok {
    Ok(())
  } else {
    Err(append_error(appender_error(self, "append_chunk failed")))
  }
}

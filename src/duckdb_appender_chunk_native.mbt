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
//
// A `duckdb_data_chunk` vector has a fixed row capacity
// (`duckdb_vector_size()`, normally 2048), so `append_chunk` slices inputs
// larger than the capacity into consecutive sub-chunks before writing.

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
/// Row capacity of vectors inside a created data chunk.
extern "C" fn native_vector_size() -> Int = "duckdb_mb_vector_size"

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
//
// Every writer copies `rows` source elements starting at `offset` into the
// vector buffer, so a caller can feed it a slice of a larger payload array.

///|
/// Write 1=valid/0=null flags into the vector's validity mask.
#borrow(vector, flags)
extern "C" fn native_vector_write_validity(
  vector : NativeVector,
  flags : FixedArray[Int],
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_validity"

///|
#borrow(vector, flags)
extern "C" fn native_vector_write_bool(
  vector : NativeVector,
  flags : FixedArray[Int],
  offset : Int,
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
  offset : Int,
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
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_i64"

///|
/// UBIGINT payload (raw uint64 bits).
#borrow(vector, values)
extern "C" fn native_vector_write_u64(
  vector : NativeVector,
  values : FixedArray[UInt64],
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_u64"

///|
#borrow(vector, values)
extern "C" fn native_vector_write_f32(
  vector : NativeVector,
  values : FixedArray[Float],
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_f32"

///|
#borrow(vector, values)
extern "C" fn native_vector_write_f64(
  vector : NativeVector,
  values : FixedArray[Double],
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_f64"

///|
/// VARCHAR/BLOB payload (ref array of utf8/raw byte strings, assigned via
/// `duckdb_vector_assign_string_element_len`).
#borrow(vector, values)
extern "C" fn native_vector_write_strings(
  vector : NativeVector,
  values : FixedArray[Bytes],
  offset : Int,
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
  offset : Int,
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
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_decimal"

///|
/// INTERVAL payload flattened to `[months, days]` pairs plus `micros`.
#borrow(vector, months_days, micros)
extern "C" fn native_vector_write_interval(
  vector : NativeVector,
  months_days : FixedArray[Int],
  micros : FixedArray[Int64],
  offset : Int,
  rows : Int,
) -> Bool = "duckdb_mb_vector_write_interval"

// ----------------------------------------------------------------------------
// Implementation
// ----------------------------------------------------------------------------

///|
/// 1=valid/0=null flags for the `rows`-element slice at `offset`, for the C
/// validity writer.
fn native_validity_flags(
  vector : Vector,
  offset : Int,
  rows : Int,
) -> FixedArray[Int] {
  FixedArray::makei(rows, fn(i) {
    if vector.validity[offset + i] {
      1
    } else {
      0
    }
  })
}

///|
/// Copy the `rows`-element slice at `offset` of one `Vector`'s validity mask
/// and payload into the borrowed `duckdb_vector` `target`. `type_id` is the
/// column's DuckDB type id from `append_chunk_specs`.
fn write_column_native(
  vector : Vector,
  target : NativeVector,
  type_id : Int,
  offset : Int,
  rows : Int,
) -> Result[Unit, DuckDBError] {
  let write_error = fn() {
    append_error("append_chunk: failed to write column data")
  }
  if !native_vector_write_validity(
      target,
      native_validity_flags(vector, offset, rows),
      0,
      rows,
    ) {
    return Err(write_error())
  }
  let ok = match vector.data {
    Bool(values) =>
      native_vector_write_bool(
        target,
        FixedArray::makei(rows, fn(i) { if values[offset + i] { 1 } else { 0 } }),
        0,
        rows,
      )
    Int32(values) =>
      native_vector_write_i32(target, type_id, values, offset, rows)
    Int64(values) =>
      native_vector_write_i64(target, type_id, values, offset, rows)
    UInt64(values) => native_vector_write_u64(target, values, offset, rows)
    Float(values) => native_vector_write_f32(target, values, offset, rows)
    Double(values) => native_vector_write_f64(target, values, offset, rows)
    Varchar(values) =>
      native_vector_write_strings(
        target,
        FixedArray::makei(rows, fn(i) {
          @encoding/utf8.encode(values[offset + i])
        }),
        0,
        rows,
      )
    Blob(values) =>
      native_vector_write_strings(
        target,
        FixedArray::makei(rows, fn(i) { values[offset + i] }),
        0,
        rows,
      )
    HugeInt(lower~, upper~) =>
      native_vector_write_i128(target, type_id, upper, lower, offset, rows)
    Decimal(cells) => {
      let upper = FixedArray::make(rows, 0L)
      let lower = FixedArray::make(rows, 0UL)
      for i in 0..<rows {
        upper[i] = cells[offset + i].upper
        lower[i] = cells[offset + i].lower
      }
      native_vector_write_decimal(target, upper, lower, 0, rows)
    }
    Interval(cells) => {
      let months_days = FixedArray::make(rows * 2, 0)
      let micros = FixedArray::make(rows, 0L)
      for i in 0..<rows {
        months_days[i * 2] = cells[offset + i].months
        months_days[i * 2 + 1] = cells[offset + i].days
        micros[i] = cells[offset + i].micros
      }
      native_vector_write_interval(target, months_days, micros, 0, rows)
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
/// appended with `duckdb_append_data_chunk` — no per-cell FFI calls. Inputs
/// larger than the vector capacity (`duckdb_vector_size()`, normally 2048
/// rows) are appended as consecutive capacity-sized slices.
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
  let capacity = native_vector_size()
  for offset = 0; offset < rows; offset = offset + capacity {
    let slice_rows = if rows - offset < capacity {
      rows - offset
    } else {
      capacity
    }
    let data_chunk = native_create_data_chunk_typed(type_ids, aux, ncols)
    if native_is_null_data_chunk(data_chunk) {
      return Err(
        append_error(last_error("append_chunk: failed to create data chunk")),
      )
    }
    for col = 0; col < ncols; col = col + 1 {
      let target = native_data_chunk_get_vector(data_chunk, col)
      match
        write_column_native(
          chunk.vectors[col],
          target,
          type_ids[col],
          offset,
          slice_rows,
        ) {
        Err(err) => {
          native_destroy_data_chunk(data_chunk)
          return Err(err)
        }
        Ok(_) => ()
      }
    } nobreak {
      ()
    }
    native_data_chunk_set_size(data_chunk, slice_rows.to_int64())
    let ok = native_appender_append_data_chunk(self, data_chunk)
    native_destroy_data_chunk(data_chunk)
    if !ok {
      return Err(append_error(appender_error(self, "append_chunk failed")))
    }
  } nobreak {
    ()
  }
  Ok(())
}

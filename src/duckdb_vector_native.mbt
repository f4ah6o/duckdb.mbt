///|
// ============================================================================
// Native backend: materialize `NativeChunk` into canonical `VectorChunk`
// (issue #61). Borrowed `duckdb_vector` handles are only used transiently
// inside `vector_from_native` — every `Vector` holds owned MoonBit arrays.
// ============================================================================

///|

///|
#borrow(chunk, col)
extern "C" fn native_chunk_vector(
  chunk : NativeChunk,
  col : Int,
) -> NativeVector = "duckdb_mb_chunk_vector"

///|

///|
#borrow(chunk, col)
extern "C" fn native_chunk_column_type(chunk : NativeChunk, col : Int) -> Int = "duckdb_mb_chunk_column_type"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_validity_flags(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Int] = "duckdb_mb_vector_validity_flags"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_bool(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Int] = "duckdb_mb_vector_bool"

///|

///|
#borrow(vector, type_id, rows)
extern "C" fn native_vector_i32(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> FixedArray[Int] = "duckdb_mb_vector_i32"

///|

///|
#borrow(vector, type_id, rows)
extern "C" fn native_vector_i64(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> FixedArray[Int64] = "duckdb_mb_vector_i64"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_u64(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[UInt64] = "duckdb_mb_vector_u64"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_f32(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Float] = "duckdb_mb_vector_f32"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_f64(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Double] = "duckdb_mb_vector_f64"

///|

///|
#borrow(vector, type_id, rows)
extern "C" fn native_vector_strings(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> FixedArray[Bytes] = "duckdb_mb_vector_strings"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_blobs(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Bytes] = "duckdb_mb_vector_blobs"

///|

///|
#borrow(vector, type_id, rows)
extern "C" fn native_vector_i128_lo(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> FixedArray[UInt64] = "duckdb_mb_vector_i128_lo"

///|

///|
#borrow(vector, type_id, rows)
extern "C" fn native_vector_i128_hi(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> FixedArray[Int64] = "duckdb_mb_vector_i128_hi"

///|

///|
#borrow(vector)
extern "C" fn native_vector_decimal_meta(vector : NativeVector) -> Int = "duckdb_mb_vector_decimal_meta"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_decimal_lo(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[UInt64] = "duckdb_mb_vector_decimal_lo"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_decimal_hi(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Int64] = "duckdb_mb_vector_decimal_hi"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_interval(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[Int64] = "duckdb_mb_vector_interval"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_list_offsets(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[UInt64] = "duckdb_mb_vector_list_offsets"

///|

///|
#borrow(vector, rows)
extern "C" fn native_vector_list_lengths(
  vector : NativeVector,
  rows : Int,
) -> FixedArray[UInt64] = "duckdb_mb_vector_list_lengths"

///|

///|
#borrow(vector)
extern "C" fn native_vector_list_child(vector : NativeVector) -> NativeVector = "duckdb_mb_vector_list_child"

///|

///|
#borrow(vector)
extern "C" fn native_vector_list_child_size(vector : NativeVector) -> Int64 = "duckdb_mb_vector_list_child_size"

///|

///|
#borrow(vector)
extern "C" fn native_vector_list_child_type(vector : NativeVector) -> Int = "duckdb_mb_vector_list_child_type"

///|

///|
#borrow(vector)
extern "C" fn native_vector_map_key_type(vector : NativeVector) -> Int = "duckdb_mb_vector_map_key_type"

///|

///|
#borrow(vector)
extern "C" fn native_vector_map_value_type(vector : NativeVector) -> Int = "duckdb_mb_vector_map_value_type"

///|

///|
#borrow(vector)
extern "C" fn native_vector_struct_child_count(vector : NativeVector) -> Int = "duckdb_mb_vector_struct_child_count"

///|

///|
#borrow(vector, child)
extern "C" fn native_vector_struct_child(
  vector : NativeVector,
  child : Int,
) -> NativeVector = "duckdb_mb_vector_struct_child"

///|

///|
#borrow(vector, child)
extern "C" fn native_vector_struct_child_name(
  vector : NativeVector,
  child : Int,
) -> Bytes = "duckdb_mb_vector_struct_child_name"

///|

///|
#borrow(vector, child)
extern "C" fn native_vector_struct_child_type(
  vector : NativeVector,
  child : Int,
) -> Int = "duckdb_mb_vector_struct_child_type"

///|

///|
#borrow(vector)
extern "C" fn native_vector_array_size(vector : NativeVector) -> Int = "duckdb_mb_vector_array_size"

///|

///|
#borrow(vector)
extern "C" fn native_vector_array_child(vector : NativeVector) -> NativeVector = "duckdb_mb_vector_array_child"

///|

///|
#borrow(vector)
extern "C" fn native_vector_array_child_type(vector : NativeVector) -> Int = "duckdb_mb_vector_array_child_type"

///|

///|
#borrow(result)
extern "C" fn native_result_fetch_chunk(result : NativeResult) -> NativeChunk = "duckdb_mb_result_fetch_chunk"

///|

///|
#borrow(chunk, col)
extern "C" fn native_chunk_col_strings(
  chunk : NativeChunk,
  col : Int,
) -> FixedArray[Bytes] = "duckdb_mb_chunk_col_strings"

///|

///|
#borrow(vector, type_id, row)
extern "C" fn native_vector_cell_string(
  vector : NativeVector,
  type_id : Int,
  row : Int,
) -> Bytes = "duckdb_mb_vector_cell_string"

// ============================================================================
// Vector materialization
// ============================================================================

///|
/// Decode one borrowed `NativeVector` into an owned `Vector`.
/// `type_id` is the column's `duckdb_type` id; `rows` the element count.
fn vector_from_native(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> Vector {
  let validity = validity_from_flags(
    native_vector_validity_flags(vector, rows),
    rows,
  )
  let logical_type = column_type_from_id(type_id)
  let data = vector_data_from_native(vector, type_id, rows)
  { logical_type, data, validity, }
}

///|
fn vector_data_from_native(
  vector : NativeVector,
  type_id : Int,
  rows : Int,
) -> VectorData {
  match type_id {
    1 =>
      VectorData::Bool(
        FixedArray::makei(rows, fn(i) {
          native_vector_bool(vector, rows)[i] == 1
        }),
      )
    2 | 3 | 4 | 6 | 7 | 13 =>
      // TINYINT/SMALLINT/INTEGER/UTINYINT/USMALLINT/DATE
      VectorData::Int32(native_vector_i32(vector, type_id, rows))
    5 | 8 | 14 | 39 | 12 | 20 | 21 | 22 | 31 =>
      // BIGINT/UINTEGER/TIME/TIME_NS/TIMESTAMP*/TIMESTAMP_TZ
      VectorData::Int64(native_vector_i64(vector, type_id, rows))
    9 => VectorData::UInt64(native_vector_u64(vector, rows))
    10 => VectorData::Float(native_vector_f32(vector, rows))
    11 => VectorData::Double(native_vector_f64(vector, rows))
    17 | 23 => {
      // VARCHAR / ENUM
      let raw = native_vector_strings(vector, type_id, rows)
      VectorData::Varchar(Array::makei(rows, fn(i) { bytes_to_string(raw[i]) }))
    }
    18 => {
      // BLOB
      let raw = native_vector_blobs(vector, rows)
      VectorData::Blob(Array::makei(rows, fn(i) { raw[i] }))
    }
    16 | 32 | 27 =>
      // HUGEINT / UHUGEINT / UUID
      VectorData::HugeInt(
        lower=native_vector_i128_lo(vector, type_id, rows),
        upper=native_vector_i128_hi(vector, type_id, rows),
      )
    19 => {
      // DECIMAL: metadata packed as (width << 16) | scale
      let meta = native_vector_decimal_meta(vector)
      let lo = native_vector_decimal_lo(vector, rows)
      let hi = native_vector_decimal_hi(vector, rows)
      VectorData::Decimal(
        FixedArray::makei(rows, fn(i) {
          {
            width: meta >> 16,
            scale: meta & 0xffff,
            lower: lo[i],
            upper: hi[i],
          }
        }),
      )
    }
    15 => {
      // INTERVAL: flattened [months, days, micros] triples
      let flat = native_vector_interval(vector, rows)
      VectorData::Interval(
        FixedArray::makei(rows, fn(i) {
          {
            months: flat[i * 3].to_int(),
            days: flat[i * 3 + 1].to_int(),
            micros: flat[i * 3 + 2],
          }
        }),
      )
    }
    24 => {
      // LIST
      let offsets = native_vector_list_offsets(vector, rows)
      let lengths = native_vector_list_lengths(vector, rows)
      let child_size = native_vector_list_child_size(vector).to_int()
      let child_type = native_vector_list_child_type(vector)
      let child_vector = native_vector_list_child(vector)
      let child = vector_from_native(child_vector, child_type, child_size)
      VectorData::List(child~, offsets~, lengths~)
    }
    25 => {
      // STRUCT
      let count = native_vector_struct_child_count(vector)
      let fields : Array[String] = []
      let children : Array[Vector] = []
      for i in 0..<count {
        fields.push(bytes_to_string(native_vector_struct_child_name(vector, i)))
        children.push(
          vector_from_native(
            native_vector_struct_child(vector, i),
            native_vector_struct_child_type(vector, i),
            rows,
          ),
        )
      }
      VectorData::Struct(fields~, children~)
    }
    26 => {
      // MAP is a LIST of STRUCT{key, value} in the C API.
      let offsets = native_vector_list_offsets(vector, rows)
      let lengths = native_vector_list_lengths(vector, rows)
      let child_size = native_vector_list_child_size(vector).to_int()
      let child_vector = native_vector_list_child(vector)
      let keys = vector_from_native(
        native_vector_struct_child(child_vector, 0),
        native_vector_map_key_type(vector),
        child_size,
      )
      let values = vector_from_native(
        native_vector_struct_child(child_vector, 1),
        native_vector_map_value_type(vector),
        child_size,
      )
      VectorData::Map(offsets~, lengths~, keys~, values~)
    }
    33 => {
      // ARRAY: fixed `array_size` children per row, offsets are implicit.
      let array_size = native_vector_array_size(vector)
      let child_size = rows * array_size
      let child_type = native_vector_array_child_type(vector)
      let child = vector_from_native(
        native_vector_array_child(vector),
        child_type,
        child_size,
      )
      let offsets = FixedArray::makei(rows, fn(i) {
        (i * array_size).to_uint64()
      })
      let lengths = FixedArray::makei(rows, fn(_) { array_size.to_uint64() })
      VectorData::List(child~, offsets~, lengths~)
    }
    _ => {
      // Fallback for types without a dedicated layout (UNION, BIT, TIME_TZ,
      // BIGNUM, ...): per-cell strings.
      let cells : Array[Value] = []
      for row in 0..<rows {
        cells.push(
          Value::String(
            bytes_to_string(native_vector_cell_string(vector, type_id, row)),
          ),
        )
      }
      VectorData::Any(cells)
    }
  }
}

// ============================================================================
// Chunk materialization
// ============================================================================

///|
/// Fetch and materialize the next `VectorChunk` from a materialized result.
/// Returns `None` when the result is exhausted. `columns` supplies the
/// column names (they live on the result, not the chunk).
fn native_result_next_chunk(
  result : NativeResult,
  columns : Array[String],
) -> VectorChunk? {
  let native_chunk = native_result_fetch_chunk(result)
  if native_is_null_chunk(native_chunk) {
    return None
  }
  let vector_chunk = vector_chunk_from_native_with_names(native_chunk, columns)
  native_chunk_destroy(native_chunk)
  Some(vector_chunk)
}

///|
/// Materialize all columns of a `NativeChunk` into a `VectorChunk`.
/// Column names come from the owning result/stream; pass them explicitly.
fn vector_chunk_from_native_with_names(
  native_chunk : NativeChunk,
  columns : Array[String],
) -> VectorChunk {
  let column_count = native_chunk_column_count(native_chunk)
  let rows = native_chunk_row_count(native_chunk)
  let vectors : Array[Vector] = []
  for col in 0..<column_count {
    vectors.push(
      vector_from_native(
        native_chunk_vector(native_chunk, col),
        native_chunk_column_type(native_chunk, col),
        rows,
      ),
    )
  }
  { columns, vectors, }
}

///|
/// Column names declared on a materialized result.
fn native_result_column_names(result : NativeResult) -> Array[String] {
  let column_count = native_result_column_count(result)
  Array::makei(column_count, fn(col) {
    bytes_to_string(native_result_column_name(result, col))
  })
}

///|
/// Column names declared on a result stream.
fn native_stream_column_names(stream : ResultStream) -> Array[String] {
  let column_count = native_stream_column_count(stream)
  Array::makei(column_count, fn(col) {
    bytes_to_string(native_stream_column_name(stream, col))
  })
}

///|
/// Drain a materialized result into `VectorChunk`s (the canonical typed
/// representation), then destroy it.
fn native_result_to_vector_chunks(result : NativeResult) -> Array[VectorChunk] {
  let columns = native_result_column_names(result)
  let chunks : Array[VectorChunk] = []
  for ;; {
    match native_result_next_chunk(result, columns) {
      Some(chunk) => chunks.push(chunk)
      None => break
    }
  } nobreak {
    ()
  }
  native_result_destroy(result)
  chunks
}

// ============================================================================
// Public canonical-chunk API (issue #61)
// ============================================================================

///|
/// Run `sql` and return the result as canonical typed `VectorChunk`s —
/// columnar `Vector`s with owned typed arrays and per-row validity, no
/// per-cell string conversion. Consumed via `Vector::value_at`,
/// `Vector::ints`/`int64s`/`doubles`/..., `VectorChunk::get_value`, or
/// materialized to the compat `QueryResult` via `chunks_to_query_result`.
pub fn Connection::query_chunks(
  self : Connection,
  sql : String,
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  let result = native_query(self, @encoding/utf8.encode(sql))
  if native_is_null_result(result) {
    on_done(Err(native_typed_error(query_error, "duckdb_query failed")))
  } else {
    on_done(Ok(native_result_to_vector_chunks(result)))
  }
}

///|
/// Execute the prepared statement and return canonical typed `VectorChunk`s.
pub fn PreparedStatement::execute_chunks(
  self : PreparedStatement,
  on_done~ : (Result[Array[VectorChunk], DuckDBError]) -> Unit,
) -> Unit {
  let result = native_execute_prepared(self)
  if native_is_null_result(result) {
    on_done(Err(native_typed_error(query_error, "execute_prepared failed")))
  } else {
    on_done(Ok(native_result_to_vector_chunks(result)))
  }
}

///|
/// Fetch the next chunk of the stream as a canonical typed `VectorChunk`,
/// or `None` at end-of-stream. `ResultStream::next` builds its string
/// `DataChunk` on top of this same path.
pub fn ResultStream::next_chunk(
  self : ResultStream,
  on_done~ : (Result[VectorChunk?, DuckDBError]) -> Unit,
) -> Unit {
  let chunk = native_stream_fetch_chunk(self)
  if native_is_null_chunk(chunk) {
    let msg = bytes_to_string(native_last_error())
    if msg is "" {
      on_done(Ok(None))
    } else {
      on_done(Err(native_typed_error(query_error, "stream fetch failed")))
    }
  } else {
    let vector_chunk = vector_chunk_from_native_with_names(
      chunk,
      native_stream_column_names(self),
    )
    native_chunk_destroy(chunk)
    if vector_chunk.row_count() <= 0 || vector_chunk.column_count() <= 0 {
      on_done(Ok(None))
    } else {
      on_done(Ok(Some(vector_chunk)))
    }
  }
}

// ============================================================================
// Compat row-string materialization on top of chunks (issue #61)
// ============================================================================

///|
/// Materialize one fetched chunk as row-major string cells + null flags via
/// the per-column `duckdb_mb_chunk_col_strings` reader. This is the same
/// canonical chunk the typed path decodes; only the rendering differs.
fn native_chunk_string_rows(
  chunk : NativeChunk,
) -> (Array[Array[String]], Array[Array[Bool]]) {
  let row_count = native_chunk_row_count(chunk)
  let column_count = native_chunk_column_count(chunk)
  let column_cells : Array[FixedArray[Bytes]] = []
  let column_validity : Array[FixedArray[Int]] = []
  for col in 0..<column_count {
    column_cells.push(native_chunk_col_strings(chunk, col))
    column_validity.push(
      native_vector_validity_flags(native_chunk_vector(chunk, col), row_count),
    )
  }
  let rows : Array[Array[String]] = []
  let nulls : Array[Array[Bool]] = []
  for row in 0..<row_count {
    let row_values : Array[String] = []
    let row_nulls : Array[Bool] = []
    for col in 0..<column_count {
      let is_null = column_validity[col][row] == 0
      row_nulls.push(is_null)
      row_values.push(
        if is_null {
          ""
        } else {
          bytes_to_string(column_cells[col][row])
        },
      )
    }
    rows.push(row_values)
    nulls.push(row_nulls)
  }
  (rows, nulls)
}

///|
/// Build a `FixedArray[Bool]` validity mask from `0`/`1` flags produced by a
/// backend reader (`1` = valid). `flags` must have `rows` elements.
fn validity_from_flags(flags : FixedArray[Int], rows : Int) -> FixedArray[Bool] {
  FixedArray::makei(rows, fn(i) { i < flags.length() && flags[i] == 1 })
}

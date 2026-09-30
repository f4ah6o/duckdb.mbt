///|
// ============================================================================
// Arrow on canonical typed vectors — native backend (issue #62)
// ============================================================================
//
// The previous native implementation embedded a `duckdb_result` in a
// dedicated C struct and re-encoded every column into ad-hoc packed byte
// buffers (`[count][values]` / `[count][values][validity]`, plus a schema
// JSON string). That whole C surface is gone: `query_arrow` now materializes
// through the same `duckdb_fetch_chunk` -> `VectorChunk` pipeline as
// `Connection::query_chunks`, so Arrow columns share the canonical typed
// vectors (Int64/UInt64 precision, real validity masks, nested types).
// `ArrowResult::close` destroys the underlying `duckdb_result`.

///|
/// Owned `duckdb_result` retained by an `ArrowResult` until `close`.
struct ArrowHandle {
  inner : NativeResult
}

///|
/// Release the native result owned by `handle`.
fn arrow_result_close_handle(handle : ArrowHandle) -> Unit {
  native_result_destroy(handle.inner)
}

///|
/// Run `sql` and materialize the result as an `ArrowResult` backed by
/// canonical `VectorChunk`s.
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  let result = native_query(self, @encoding/utf8.encode(sql))
  if native_is_null_result(result) {
    on_done(Err(native_typed_error("arrow query failed")))
    return
  }
  let columns = native_result_column_names(result)
  let column_count = columns.length()
  let column_types = Array::makei(column_count, fn(col) {
    column_type_from_id(native_result_column_type(result, col))
  })
  let chunks : Array[VectorChunk] = []
  for ;; {
    match native_result_next_chunk(result, columns) {
      Some(chunk) => chunks.push(chunk)
      None => break
    }
  } nobreak {
    ()
  }
  on_done(
    Ok(ArrowResult::{
      handle: Some({ inner: result, }),
      columns,
      column_types,
      nullables: FixedArray::make(column_count, true),
      chunks,
      closed: false,
    }),
  )
}

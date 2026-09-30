///|
// ============================================================================
// Arrow on canonical typed vectors — JS backends (issue #62)
// ============================================================================
//
// The previous JS implementation re-encoded every column through
// `JSON.stringify(vector.toArray())` — losing Int64 precision (JSON has no
// 64-bit ints) and flattening nulls/types — while the Node path called a
// nonexistent `result.arrow()` and produced empty results. `query_arrow`
// now wraps the backend-native result in a `ResultStream`-shaped object and
// drains it through the same pooled `VectorChunk` pipeline as
// `Connection::query_chunks`: browser wasm feeds its Arrow table batches,
// Node feeds the materialized result's data chunks. No JSON round-trip;
// null masks use the shared validity layout.

///|
/// Backend-native handle: the arrow `Table` on wasm, the materialized
/// `DuckDBMaterializedResult` on Node.
#external
type ArrowHandle

///|
extern "js" fn js_query_arrow(
  conn : Connection,
  sql : String,
  on_ok : (ArrowHandle) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const toError = (err) => err && err.message ? err.message : String(err);
  #|  const run = async () => {
  #|    if (conn && conn.closed) { on_err("ERR_CLOSED:connection"); return; }
  #|    if (conn && conn.kind === "wasm") {
  #|      try {
  #|        const table = await conn.conn.query(sql, { returnType: "arrow" });
  #|        on_ok({ kind: "arrow", backend: "wasm", table: table, closed: false });
  #|        return;
  #|      } catch (e) { on_err(toError(e)); return; }
  #|    }
  #|    if (conn && conn.kind === "node") {
  #|      try {
  #|        const res = await conn.connection.run(sql);
  #|        on_ok({ kind: "arrow", backend: "node", result: res, closed: false });
  #|        return;
  #|      } catch (e) { on_err(toError(e)); return; }
  #|    }
  #|    on_err("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(toError(err)));
  #|}

///|
/// Wrap an Arrow handle in a `ResultStream`-shaped object so it drains
/// through the canonical `js_stream_next_vectors_ffi` pipeline.
extern "js" fn js_arrow_to_stream(result : ArrowHandle) -> ResultStream =
  #|(result) => {
  #|  if (result && result.backend === "wasm") {
  #|    const table = result.table;
  #|    const fields = (table && table.schema && table.schema.fields) || [];
  #|    const columns = fields.map((f) => f.name || "");
  #|    const batches = (table && table.batches) || [];
  #|    return {
  #|      kind: "wasm",
  #|      reader: batches[Symbol.iterator](),
  #|      columns: columns,
  #|      done: false,
  #|      closed: !!result.closed,
  #|    };
  #|  }
  #|  if (result && result.backend === "node") {
  #|    const res = result.result;
  #|    const columns = res.columnNames();
  #|    const typeIds = columns.map((_, i) => res.columnTypeId(i));
  #|    const chunks = [];
  #|    const count = res.chunkCount || 0;
  #|    for (let i = 0; i < count; i++) chunks.push(res.getChunk(i));
  #|    const pseudo = {
  #|      fetchChunk: async () =>
  #|        chunks.shift() || { rowCount: 0, getColumns: () => [] },
  #|    };
  #|    return {
  #|      kind: "node",
  #|      result: pseudo,
  #|      columns: columns,
  #|      typeIds: typeIds,
  #|      // Full DuckDBType objects let the pooled decoder decompose
  #|      // nested columns (the pseudo result has no columnType()).
  #|      duckTypes: columns.map((_, i) => {
  #|        try { return res.columnType(i); } catch (e) { return null; }
  #|      }),
  #|      closed: !!result.closed,
  #|    };
  #|  }
  #|  return null;
  #|}

///|
/// Column names declared on the Arrow result.
extern "js" fn js_arrow_columns(result : ArrowHandle) -> FixedArray[String] =
  #|(result) => {
  #|  if (result && result.backend === "wasm") {
  #|    const fields =
  #|      (result.table && result.table.schema && result.table.schema.fields) || [];
  #|    return fields.map((f) => f.name || "");
  #|  }
  #|  if (result && result.backend === "node" && result.result) {
  #|    return result.result.columnNames();
  #|  }
  #|  return [];
  #|}

///|
/// DuckDB type ids per column (arrow schema mapped on wasm, `columnTypeId`
/// on Node; 0 = unknown).
extern "js" fn js_arrow_type_ids(result : ArrowHandle) -> FixedArray[Int] =
  #|(result) => {
  #|  // apache-arrow type -> duckdb type id (mirrors tidForArrowType in
  #|  // duckdb_vector_js.mbt; 0 = unknown).
  #|  const tidForArrowType = (t) => {
  #|    if (!t || t.typeId === undefined || t.typeId === null) return 0;
  #|    switch (t.typeId | 0) {
  #|      case 2: { // Int
  #|        const w = t.bitWidth | 0;
  #|        const s = t.isSigned !== false;
  #|        if (s) return w <= 8 ? 2 : w <= 16 ? 3 : w <= 32 ? 4 : 5;
  #|        return w <= 8 ? 6 : w <= 16 ? 7 : w <= 32 ? 8 : 9;
  #|      }
  #|      case 3: return (t.precision | 0) <= 1 ? 10 : 11; // Float
  #|      case 4: case 15: case 19: case 23: return 18;     // *Binary*
  #|      case 5: case 20: case 24: return 17;             // *Utf8*
  #|      case 6: return 1;                               // Bool
  #|      case 7: return 19;                              // Decimal
  #|      case 8: return 13;                              // Date
  #|      case 9: return 14;                              // Time
  #|      case 10: return 12;                             // Timestamp
  #|      case 11: return 15;                             // Interval
  #|      case 12: case 21: case 25: case 26: return 24;    // *List*
  #|      case 13: return 25;                             // Struct
  #|      case 14: return 28;                             // Union
  #|      case 16: return 33;                             // FixedSizeList
  #|      case 17: return 26;                             // Map
  #|      case 18: return 0;                              // Duration
  #|      case 27: return 23;                             // Dictionary ~ enum
  #|      default: return 0;
  #|    }
  #|  };
  #|  if (result && result.backend === "wasm") {
  #|    const fields =
  #|      (result.table && result.table.schema && result.table.schema.fields) || [];
  #|    return fields.map((f) => tidForArrowType(f && f.type));
  #|  }
  #|  if (result && result.backend === "node" && result.result) {
  #|    const res = result.result;
  #|    return res.columnNames().map((_, i) => res.columnTypeId(i));
  #|  }
  #|  return [];
  #|}

///|
/// Per-column nullability flags (real flags on wasm; Node's materialized
/// result does not expose them, so columns report nullable like the legacy
/// implementation did).
extern "js" fn js_arrow_nullables(result : ArrowHandle) -> FixedArray[Bool] =
  #|(result) => {
  #|  if (result && result.backend === "wasm") {
  #|    const fields =
  #|      (result.table && result.table.schema && result.table.schema.fields) || [];
  #|    return fields.map((f) => !(f && f.nullable === false));
  #|  }
  #|  if (result && result.backend === "node" && result.result) {
  #|    return result.result.columnNames().map(() => true);
  #|  }
  #|  return [];
  #|}

///|
/// Mark the handle closed and drop references so the table/result can be
/// garbage-collected.
extern "js" fn js_arrow_close_handle(result : ArrowHandle) -> Unit =
  #|(result) => {
  #|  if (!result) return;
  #|  result.closed = true;
  #|  result.table = null;
  #|  result.result = null;
  #|}

///|
/// Release the backend-native handle.
fn arrow_result_close_handle(handle : ArrowHandle) -> Unit {
  js_arrow_close_handle(handle)
}

///|
/// Run `sql` and materialize the result as an `ArrowResult` backed by
/// canonical `VectorChunk`s. The drain reuses
/// `js_stream_next_vectors_ffi`'s pooled layout, so primitive columns keep
/// their JS-typed pools and nested columns use the tagged cell encoding —
/// never a `JSON.stringify` of whole columns.
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  js_query_arrow(
    self,
    sql,
    fn(handle) {
      js_drain_stream_chunks(js_arrow_to_stream(handle), [], on_done=fn(
        result,
      ) {
        match result {
          Ok(chunks) => {
            let columns = Array::from_fixed_array(js_arrow_columns(handle))
            let type_ids = js_arrow_type_ids(handle)
            let column_types = Array::makei(type_ids.length(), fn(col) {
              column_type_from_id(type_ids[col])
            })
            on_done(
              Ok(ArrowResult::{
                handle: Some(handle),
                columns,
                column_types,
                nullables: js_arrow_nullables(handle),
                chunks,
                closed: false,
              }),
            )
          }
          Err(err) => on_done(Err(err))
        }
      })
    },
    fn(err) { on_done(Err(query_error(err))) },
  )
}

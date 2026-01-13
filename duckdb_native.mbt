///|
#external
type NativeResult

///|
#borrow(path)
extern "C" fn native_connect(path : Bytes) -> Connection = "duckdb_mb_connect"

///|
#borrow(conn)
extern "C" fn native_disconnect(conn : Connection) = "duckdb_mb_disconnect"

///|
#borrow(conn, sql)
extern "C" fn native_query(conn : Connection, sql : Bytes) -> NativeResult = "duckdb_mb_query"

///|
#borrow(result)
extern "C" fn native_result_destroy(result : NativeResult) = "duckdb_mb_result_destroy"

///|
#borrow(result)
extern "C" fn native_result_column_count(result : NativeResult) -> Int = "duckdb_mb_result_column_count"

///|
#borrow(result)
extern "C" fn native_result_row_count(result : NativeResult) -> Int = "duckdb_mb_result_row_count"

///|
#borrow(result)
extern "C" fn native_result_column_name(
  result : NativeResult,
  col : Int,
) -> Bytes = "duckdb_mb_result_column_name"

///|
#borrow(result)
extern "C" fn native_result_is_null(
  result : NativeResult,
  col : Int,
  row : Int,
) -> Bool = "duckdb_mb_result_is_null"

///|
#borrow(result)
extern "C" fn native_result_value(
  result : NativeResult,
  col : Int,
  row : Int,
) -> Bytes = "duckdb_mb_result_value"

///|
extern "C" fn native_last_error() -> Bytes = "duckdb_mb_last_error"

///|
#borrow(conn)
extern "C" fn native_is_null_conn(conn : Connection) -> Bool = "duckdb_mb_is_null_conn"

///|
#borrow(result)
extern "C" fn native_is_null_result(result : NativeResult) -> Bool = "duckdb_mb_is_null_result"

///|
fn bytes_to_string(bytes : Bytes) -> String {
  @encoding/utf8.decode_lossy(bytes)
}

///|
fn last_error(fallback : String) -> String {
  let msg = bytes_to_string(native_last_error())
  if msg is "" {
    fallback
  } else {
    msg
  }
}

///|
pub fn connect(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  let _ = backend
  let conn = native_connect(@encoding/utf8.encode(path))
  if native_is_null_conn(conn) {
    on_ready(Err(DuckDBError::Message(last_error("duckdb_open failed"))))
  } else {
    on_ready(Ok(conn))
  }
}

///|
pub fn Connection::close(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  native_disconnect(self)
  on_done(Ok(()))
}

///|
pub fn Connection::query(
  self : Connection,
  sql : String,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let result = native_query(self, @encoding/utf8.encode(sql))
  if native_is_null_result(result) {
    on_done(Err(DuckDBError::Message(last_error("duckdb_query failed"))))
  } else {
    let column_count = native_result_column_count(result)
    let row_count = native_result_row_count(result)
    let columns : Array[String] = []
    for col = 0; col < column_count; col = col + 1 {
      columns.push(bytes_to_string(native_result_column_name(result, col)))
    } else {
      ()
    }
    let rows : Array[Array[String]] = []
    let nulls : Array[Array[Bool]] = []
    for row = 0; row < row_count; row = row + 1 {
      let row_values : Array[String] = []
      let row_nulls : Array[Bool] = []
      for col = 0; col < column_count; col = col + 1 {
        let is_null = native_result_is_null(result, col, row)
        row_nulls.push(is_null)
        if is_null {
          row_values.push("")
        } else {
          row_values.push(
            bytes_to_string(native_result_value(result, col, row)),
          )
        }
      } else {
        ()
      }
      rows.push(row_values)
      nulls.push(row_nulls)
    } else {
      ()
    }
    native_result_destroy(result)
    on_done(Ok({ columns, rows, nulls }))
  }
}

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
  touch_public_types(backend)
  let path_bytes = @encoding/utf8.encode(path)
  let conn = native_connect(path_bytes)
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

// ============================================================================
// Prepared Statement FFI Declarations
// ============================================================================

///|
#borrow(conn, sql)
extern "C" fn native_prepare(conn : Connection, sql : Bytes) -> PreparedStatement = "duckdb_mb_prepare"

///|
#borrow(stmt)
extern "C" fn native_statement_destroy(stmt : PreparedStatement) = "duckdb_mb_statement_destroy"

///|
#borrow(stmt)
extern "C" fn native_statement_error(stmt : PreparedStatement) -> Bytes = "duckdb_mb_statement_error"

///|
#borrow(stmt)
extern "C" fn native_bind_int(stmt : PreparedStatement, index : Int, value : Int) -> Bool = "duckdb_mb_bind_int"

///|
#borrow(stmt)
extern "C" fn native_bind_bigint(stmt : PreparedStatement, index : Int, value : Int) -> Bool = "duckdb_mb_bind_bigint"

///|
#borrow(stmt)
extern "C" fn native_bind_double(stmt : PreparedStatement, index : Int, value : Double) -> Bool = "duckdb_mb_bind_double"

///|
#borrow(stmt, val)
extern "C" fn native_bind_varchar(stmt : PreparedStatement, index : Int, val : Bytes) -> Bool = "duckdb_mb_bind_varchar"

///|
#borrow(stmt)
extern "C" fn native_bind_bool(stmt : PreparedStatement, index : Int, value : Bool) -> Bool = "duckdb_mb_bind_bool"

///|
#borrow(stmt)
extern "C" fn native_bind_null(stmt : PreparedStatement, index : Int) -> Bool = "duckdb_mb_bind_null"

///|
#borrow(stmt)
extern "C" fn native_clear_bindings(stmt : PreparedStatement) -> Bool = "duckdb_mb_clear_bindings"

///|
#borrow(stmt)
extern "C" fn native_execute_prepared(stmt : PreparedStatement) -> NativeResult = "duckdb_mb_execute_prepared"

///|
#borrow(stmt)
extern "C" fn native_is_null_statement(stmt : PreparedStatement) -> Bool = "duckdb_mb_is_null_statement"

// ============================================================================
// Prepared Statement API Implementation
// ============================================================================

fn statement_error(stmt : PreparedStatement, fallback : String) -> String {
  let msg = bytes_to_string(native_statement_error(stmt))
  if msg is "" {
    fallback
  } else {
    msg
  }
}

///|
pub fn Connection::prepare(
  self : Connection,
  sql : String,
  on_done~ : (Result[PreparedStatement, DuckDBError]) -> Unit,
) -> Unit {
  let stmt = native_prepare(self, @encoding/utf8.encode(sql))
  if native_is_null_statement(stmt) {
    on_done(Err(DuckDBError::Message(statement_error(stmt, "duckdb_prepare failed"))))
  } else {
    on_done(Ok(stmt))
  }
}

///|
pub fn PreparedStatement::bind_int(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  if native_bind_int(self, index, value) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_int failed")))
  }
}

///|
pub fn PreparedStatement::bind_bigint(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  if native_bind_bigint(self, index, value) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_bigint failed")))
  }
}

///|
pub fn PreparedStatement::bind_double(
  self : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, DuckDBError] {
  if native_bind_double(self, index, value) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_double failed")))
  }
}

///|
pub fn PreparedStatement::bind_varchar(
  self : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, DuckDBError] {
  if native_bind_varchar(self, index, @encoding/utf8.encode(value)) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_varchar failed")))
  }
}

///|
pub fn PreparedStatement::bind_bool(
  self : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  if native_bind_bool(self, index, value) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_bool failed")))
  }
}

///|
pub fn PreparedStatement::bind_null(
  self : PreparedStatement,
  index : Int,
) -> Result[Unit, DuckDBError] {
  if native_bind_null(self, index) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "bind_null failed")))
  }
}

///|
pub fn PreparedStatement::clear_bindings(
  self : PreparedStatement,
) -> Result[Unit, DuckDBError] {
  if native_clear_bindings(self) {
    Ok(())
  } else {
    Err(DuckDBError::Message(statement_error(self, "clear_bindings failed")))
  }
}

///|
pub fn PreparedStatement::execute(
  self : PreparedStatement,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let result = native_execute_prepared(self)
  if native_is_null_result(result) {
    on_done(Err(DuckDBError::Message(statement_error(self, "execute_prepared failed"))))
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

///|
pub fn PreparedStatement::close(
  self : PreparedStatement,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  native_statement_destroy(self)
  on_done(Ok(()))
}


///|
pub fn connect(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  let _ = path
  let _ = backend
  on_ready(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Connection::close(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Connection::query(
  self : Connection,
  sql : String,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

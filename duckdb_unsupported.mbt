///|
pub fn connect(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  path? : String = ":memory:",
  backend? : JsBackend = JsBackend::Auto,
) -> Unit {
  let _ = path
  touch_public_types(backend)
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

// ============================================================================
// Prepared Statement Stubs
// ============================================================================

///|
pub fn Connection::prepare(
  self : Connection,
  sql : String,
  on_done~ : (Result[PreparedStatement, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn PreparedStatement::bind_int(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_bigint(
  self : PreparedStatement,
  index : Int,
  value : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_double(
  self : PreparedStatement,
  index : Int,
  value : Double,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_varchar(
  self : PreparedStatement,
  index : Int,
  value : String,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_bool(
  self : PreparedStatement,
  index : Int,
  value : Bool,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::bind_null(
  self : PreparedStatement,
  index : Int,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = index
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::clear_bindings(
  self : PreparedStatement,
) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn PreparedStatement::execute(
  self : PreparedStatement,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn PreparedStatement::close(
  self : PreparedStatement,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

// ============================================================================
// Configuration Stubs
// ============================================================================

type Config = Connection

///|
pub fn Config::create() -> Config {
  unsafe_cast { }
}

///|
pub fn Config::set(
  self : Config,
  key : String,
  value : String,
) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = key
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn connect_with_config(
  on_ready~ : (Result[Connection, DuckDBError]) -> Unit,
  config : Config?,
  path? : String,
  backend? : JsBackend,
) -> Unit {
  let _ = config
  let _ = path
  let _ = backend
  touch_public_types(backend)
  on_ready(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

// ============================================================================
// Appender Stubs
// ============================================================================

type Appender = Connection

///|
pub fn Connection::create_appender(
  self : Connection,
  schema : String,
  table : String,
  on_done~ : (Result[Appender, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = schema
  let _ = table
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}

///|
pub fn Appender::begin_row(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_int(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bigint(self : Appender, value : Int) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_double(self : Appender, value : Double) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_varchar(self : Appender, value : String) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_bool(self : Appender, value : Bool) -> Result[Unit, DuckDBError] {
  let _ = self
  let _ = value
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::append_null(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::end_row(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::flush(self : Appender) -> Result[Unit, DuckDBError] {
  let _ = self
  Err(DuckDBError::Message("duckdb bindings are not available for this target"))
}

///|
pub fn Appender::close(self : Appender, on_done~ : (Result[Unit, DuckDBError]) -> Unit) -> Unit {
  let _ = self
  on_done(
    Err(
      DuckDBError::Message("duckdb bindings are not available for this target"),
    ),
  )
}


// ============================================================================
// Quack convenience API (issue #67)
//
// Quack helpers moved out of the core `Connection` surface. They compose the
// generic extension primitives (`Connection::install_extension`,
// `load_extension`, `attach`, `query`) and add only the Quack-specific SQL
// generation on top.
//
// Quack is experimental in DuckDB 1.5.x and distributed from DuckDB's
// `core_nightly` extension repository; function names, defaults, and protocol
// details may change before DuckDB 2.0.
// ============================================================================

///|
/// Escape `value` as a single-quoted SQL string literal.
fn sql_string(value : String) -> String {
  let sb = StringBuilder()
  ignore(sb.write_char('\''))
  for c in value {
    if c == '\'' {
      ignore(sb..write_char('\'').write_char('\''))
    } else {
      ignore(sb.write_char(c))
    }
  }
  ignore(sb.write_char('\''))
  sb.to_string()
}

///|
/// Adapt a `QueryResult` callback into a `Unit` callback, discarding rows.
fn unit_done(
  result : Result[@lib.QueryResult, @lib.DuckDBError],
  on_done : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match result {
    Ok(_) => on_done(Ok(()))
    Err(err) => on_done(Err(err))
  }
}

///|
/// `Ok(())` when this backend supports Quack, otherwise the structured
/// `Unsupported` error (for example the browser WASM backend).
fn require_quack(conn : @lib.Connection) -> Result[Unit, @lib.DuckDBError] {
  conn.capabilities().require(@lib.BackendFeature::Quack)
}

///|
/// SQL for `CALL quack_serve('uri'[, token => 'x'][, allow_other_hostname => true])`.
fn serve_sql(
  uri : String,
  token : String,
  allow_other_hostname : Bool,
) -> String {
  let sb = StringBuilder()
  ignore(sb..write_view("CALL quack_serve(").write_view(sql_string(uri)))
  if token != "" {
    ignore(sb..write_view(", token => ").write_view(sql_string(token)))
  }
  if allow_other_hostname {
    ignore(sb.write_view(", allow_other_hostname => true"))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
/// SQL for `CALL quack_stop('uri')`.
fn stop_sql(uri : String) -> String {
  let sb = StringBuilder()
  ignore(sb..write_view("CALL quack_stop(").write_view(sql_string(uri)))
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
/// SQL for `FROM quack_query('uri', 'sql'[, token => 'x'][, disable_ssl => true])`.
fn query_sql(
  uri : String,
  sql : String,
  token : String,
  disable_ssl : Bool,
) -> String {
  let sb = StringBuilder()
  ignore(
    sb
    ..write_view("FROM quack_query(")
    ..write_view(sql_string(uri))
    ..write_view(", ")
    .write_view(sql_string(sql)),
  )
  if token != "" {
    ignore(sb..write_view(", token => ").write_view(sql_string(token)))
  }
  if disable_ssl {
    ignore(sb.write_view(", disable_ssl => true"))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
/// SQL for `CREATE SECRET (TYPE quack, TOKEN 'x'[, SCOPE 's'])`.
fn secret_sql(token : String, scope : String) -> String {
  let sb = StringBuilder()
  ignore(
    sb
    ..write_view("CREATE SECRET (TYPE quack, TOKEN ")
    .write_view(sql_string(token)),
  )
  if scope != "" {
    ignore(sb..write_view(", SCOPE ").write_view(sql_string(scope)))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
/// The parenthesized ATTACH option clause for a Quack catalog:
/// `TYPE quack[, TOKEN 'x'][, DISABLE_SSL true]`.
fn attach_options(token : String, disable_ssl : Bool) -> String {
  let sb = StringBuilder()
  ignore(sb.write_view("TYPE quack"))
  if token != "" {
    ignore(sb..write_view(", TOKEN ").write_view(sql_string(token)))
  }
  if disable_ssl {
    ignore(sb.write_view(", DISABLE_SSL true"))
  }
  sb.to_string()
}

///|
/// Install the experimental Quack extension from DuckDB's `core_nightly`
/// repository (`FORCE INSTALL quack FROM core_nightly`).
///
/// Returns `DuckDBError::Unsupported` without touching the database on
/// backends whose capabilities exclude Quack (browser WASM).
pub fn install(
  conn : @lib.Connection,
  on_done~ : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) =>
      conn.install_extension(
        "quack",
        repository="core_nightly",
        force=true,
        on_done~,
      )
    Err(err) => on_done(Err(err))
  }
}

///|
/// Load the Quack extension in `conn` (`LOAD quack`).
pub fn load(
  conn : @lib.Connection,
  on_done~ : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) => conn.load_extension("quack", on_done~)
    Err(err) => on_done(Err(err))
  }
}

///|
/// Start a Quack server on `conn` (`CALL quack_serve('uri', ...)`).
///
/// `token` authenticates clients of the served endpoint;
/// `allow_other_hostname` permits non-localhost hostnames.
pub fn serve(
  conn : @lib.Connection,
  uri : String,
  token? : String = "",
  allow_other_hostname? : Bool = false,
  on_done~ : (Result[@lib.QueryResult, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) => conn.query(serve_sql(uri, token, allow_other_hostname), on_done~)
    Err(err) => on_done(Err(err))
  }
}

///|
/// Stop a Quack server started on `conn` (`CALL quack_stop('uri')`).
pub fn stop(
  conn : @lib.Connection,
  uri : String,
  on_done~ : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) =>
      conn.query(stop_sql(uri), on_done=fn(result) {
        unit_done(result, on_done)
      })
    Err(err) => on_done(Err(err))
  }
}

///|
/// Run a stateless query against a Quack endpoint
/// (`FROM quack_query('uri', 'sql', ...)`).
///
/// `token` authenticates the client; `disable_ssl` allows plain HTTP for
/// intentionally non-TLS endpoints.
pub fn query(
  conn : @lib.Connection,
  uri : String,
  sql : String,
  token? : String = "",
  disable_ssl? : Bool = false,
  on_done~ : (Result[@lib.QueryResult, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) => conn.query(query_sql(uri, sql, token, disable_ssl), on_done~)
    Err(err) => on_done(Err(err))
  }
}

///|
/// Create a scoped or unscoped Quack secret for client authentication
/// (`CREATE SECRET (TYPE quack, TOKEN 'x' [, SCOPE 's'])`).
pub fn create_secret(
  conn : @lib.Connection,
  token : String,
  scope? : String = "",
  on_done~ : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) =>
      conn.query(secret_sql(token, scope), on_done=fn(result) {
        unit_done(result, on_done)
      })
    Err(err) => on_done(Err(err))
  }
}

///|
/// Attach a Quack endpoint as a remote DuckDB catalog
/// (`ATTACH 'uri' AS "db_alias" (TYPE quack, ...)`).
///
/// `db_alias` must be a plain identifier (letters, digits, underscores);
/// anything else returns `DuckDBError::InvalidArgument`.
pub fn attach(
  conn : @lib.Connection,
  uri : String,
  db_alias : String,
  token? : String = "",
  disable_ssl? : Bool = false,
  on_done~ : (Result[Unit, @lib.DuckDBError]) -> Unit,
) -> Unit {
  match require_quack(conn) {
    Ok(_) =>
      conn.attach(
        uri,
        db_alias~,
        options=attach_options(token, disable_ssl),
        on_done~,
      )
    Err(err) => on_done(Err(err))
  }
}

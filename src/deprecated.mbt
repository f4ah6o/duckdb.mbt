///|
/// Deprecated APIs kept for the 0.6.x -> 0.7.0 migration.

///|
/// Extract the diagnostic text of an error, as matching on the removed
/// `DuckDBError::Message(String)` variant used to.
///
/// Prefer matching on the structured `DuckDBError` variants for categories
/// and calling `DuckDBError::message` for the text.
#deprecated("DuckDBError::Message was replaced by structured variants in 0.7.0; use err.message() for the diagnostic text or match on the Query/Prepare/Bind/Append/Unsupported/Closed/InvalidArgument/Backend/DuckDB variants")
pub fn DuckDBError::to_message(self : DuckDBError) -> String {
  self.message()
}

///|
/// Construct an error from a plain diagnostic message, as constructing
/// `DuckDBError::Message(message)` used to.
///
/// Prefer constructing the structured variant that fits the failure (for
/// example `DuckDBError::Query`, `DuckDBError::Unsupported`, or
/// `DuckDBError::Backend`).
#deprecated("DuckDBError::Message was replaced by structured variants in 0.7.0; construct the matching variant (Query/Prepare/Bind/Append/Unsupported/Closed/InvalidArgument/Backend/DuckDB) instead")
pub fn duckdb_message_error(message : String) -> DuckDBError {
  DuckDBError::Backend(backend="generic", message~)
}

// ============================================================================
// Quack convenience methods (moved to the f4ah6o/duckdb/quack package)
//
// These `Connection` methods are deprecated forwarding facades kept for the
// transition period. New code should import `f4ah6o/duckdb/quack`
// (`quack::install(conn, ...)`, `quack::query(conn, ...)`, ...) or compose
// the generic `Connection::install_extension` / `load_extension` / `attach`
// primitives.
// ============================================================================

///|
fn quack_sql_string(value : String) -> String {
  sql_string_literal(value)
}

///|
fn quack_is_identifier_char(c : Char) -> Bool {
  is_sql_identifier_char(c)
}

///|
fn quack_identifier(value : String) -> Result[String, DuckDBError] {
  if value.length() == 0 {
    Err(
      DuckDBError::InvalidArgument(
        argument="alias",
        reason="Quack alias must not be empty",
      ),
    )
  } else {
    for c in value {
      if !quack_is_identifier_char(c) {
        return Err(
          DuckDBError::InvalidArgument(
            argument="alias",
            reason="Quack alias may only contain letters, digits, and underscores",
          ),
        )
      }
    }
    Ok("\"" + value + "\"")
  }
}

///|
/// The parenthesized ATTACH option clause for a Quack catalog:
/// `TYPE quack[, TOKEN 'x'][, DISABLE_SSL true]`.
fn quack_attach_options(token : String, disable_ssl : Bool) -> String {
  let sb = StringBuilder()
  ignore(sb.write_view("TYPE quack"))
  if token != "" {
    ignore(sb..write_view(", TOKEN ").write_view(quack_sql_string(token)))
  }
  if disable_ssl {
    ignore(sb.write_view(", DISABLE_SSL true"))
  }
  sb.to_string()
}

///|
fn quack_query_sql(
  uri : String,
  sql : String,
  token : String,
  disable_ssl : Bool,
) -> String {
  let sb = StringBuilder()
  ignore(
    sb
    ..write_view("FROM quack_query(")
    ..write_view(quack_sql_string(uri))
    ..write_view(", ")
    .write_view(quack_sql_string(sql)),
  )
  if token != "" {
    ignore(sb..write_view(", token => ").write_view(quack_sql_string(token)))
  }
  if disable_ssl {
    ignore(sb.write_view(", disable_ssl => true"))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
fn quack_secret_sql(token : String, scope : String) -> String {
  let sb = StringBuilder()
  ignore(
    sb
    ..write_view("CREATE SECRET (TYPE quack, TOKEN ")
    .write_view(quack_sql_string(token)),
  )
  if scope != "" {
    ignore(sb..write_view(", SCOPE ").write_view(quack_sql_string(scope)))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
fn quack_serve_sql(
  uri : String,
  token : String,
  allow_other_hostname : Bool,
) -> String {
  let sb = StringBuilder()
  ignore(sb..write_view("CALL quack_serve(").write_view(quack_sql_string(uri)))
  if token != "" {
    ignore(sb..write_view(", token => ").write_view(quack_sql_string(token)))
  }
  if allow_other_hostname {
    ignore(sb.write_view(", allow_other_hostname => true"))
  }
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
fn quack_stop_sql(uri : String) -> String {
  let sb = StringBuilder()
  ignore(sb..write_view("CALL quack_stop(").write_view(quack_sql_string(uri)))
  ignore(sb.write_char(')'))
  sb.to_string()
}

///|
fn quack_unit_done(
  result : Result[QueryResult, DuckDBError],
  on_done : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  unit_done(result, on_done)
}

///|
/// Install the experimental Quack extension from DuckDB's `core_nightly` repository.
///
/// Migration: use `quack::install(conn, on_done=...)` from the
/// `f4ah6o/duckdb/quack` package, or the generic
/// `conn.install_extension("quack", repository="core_nightly", force=true)`.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::install); or use Connection::install_extension(\"quack\", repository=\"core_nightly\", force=true)")
pub fn Connection::install_quack(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  self.install_extension(
    "quack",
    repository="core_nightly",
    force=true,
    on_done~,
  )
}

///|
/// Load the Quack extension in the current DuckDB connection.
///
/// Migration: use `quack::load(conn, on_done=...)` from the
/// `f4ah6o/duckdb/quack` package, or `conn.load_extension("quack")`.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::load); or use Connection::load_extension(\"quack\")")
pub fn Connection::load_quack(
  self : Connection,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  self.load_extension("quack", on_done~)
}

///|
/// Start a Quack server for this connection.
///
/// Migration: use `quack::serve(conn, uri, ...)` from the
/// `f4ah6o/duckdb/quack` package.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::serve)")
pub fn Connection::start_quack_server(
  self : Connection,
  uri : String,
  token? : String = "",
  allow_other_hostname? : Bool = false,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  self.query(quack_serve_sql(uri, token, allow_other_hostname), on_done~)
}

///|
/// Stop a Quack server started for this connection.
///
/// Migration: use `quack::stop(conn, uri, ...)` from the
/// `f4ah6o/duckdb/quack` package.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::stop)")
pub fn Connection::stop_quack_server(
  self : Connection,
  uri : String,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  self.query(quack_stop_sql(uri), on_done=fn(result) {
    quack_unit_done(result, on_done)
  })
}

///|
/// Run a stateless query against a Quack endpoint.
///
/// Migration: use `quack::query(conn, uri, sql, ...)` from the
/// `f4ah6o/duckdb/quack` package.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::query)")
pub fn Connection::quack_query(
  self : Connection,
  uri : String,
  sql : String,
  token? : String = "",
  disable_ssl? : Bool = false,
  on_done~ : (Result[QueryResult, DuckDBError]) -> Unit,
) -> Unit {
  self.query(quack_query_sql(uri, sql, token, disable_ssl), on_done~)
}

///|
/// Create a scoped or unscoped Quack secret for client authentication.
///
/// Migration: use `quack::create_secret(conn, token, ...)` from the
/// `f4ah6o/duckdb/quack` package.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::create_secret)")
pub fn Connection::create_quack_secret(
  self : Connection,
  token : String,
  scope? : String = "",
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  self.query(quack_secret_sql(token, scope), on_done=fn(result) {
    quack_unit_done(result, on_done)
  })
}

///|
/// Attach a Quack endpoint as a remote DuckDB catalog.
///
/// Migration: use `quack::attach(conn, uri, alias, ...)` from the
/// `f4ah6o/duckdb/quack` package, or the generic `conn.attach` primitive.
#deprecated("moved to the f4ah6o/duckdb/quack package (quack::attach); or use Connection::attach with a 'TYPE quack' options clause")
pub fn Connection::attach_quack(
  self : Connection,
  uri : String,
  db_alias : String,
  token? : String = "",
  disable_ssl? : Bool = false,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  match quack_identifier(db_alias) {
    Ok(_) =>
      self.attach(
        uri,
        db_alias~,
        options=quack_attach_options(token, disable_ssl),
        on_done~,
      )
    Err(err) => on_done(Err(err))
  }
}

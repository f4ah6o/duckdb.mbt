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

// ============================================================================
// Arrow per-column accessors (reworked in #62)
//
// `query_arrow` now materializes results into canonical `VectorChunk`s.
// These legacy getters keep working as forwarders that flatten the typed
// vectors into row-aligned arrays (NULL rows yield the type's zero value;
// the `*_nullable` variants also return the validity mask). Prefer
// `ArrowResult::to_chunks` plus the `Vector` accessors for new code.
// ============================================================================

///|
/// Materialized chunks of an open arrow result; `[]` once closed.
fn arrow_open_chunks(result : ArrowResult) -> Array[VectorChunk] {
  match result.to_chunks() {
    Ok(chunks) => chunks
    Err(_) => []
  }
}

///|
/// Row `row` of `vector` coerced to `Int` (0 for NULL/unconvertible rows).
fn arrow_row_int(vector : Vector, row : Int) -> Int {
  match vector.value_at(row) {
    Value::Int(n) => n
    Value::Int64(n) => n.to_int()
    Value::UInt64(n) => n.to_int()
    Value::Double(d) => d.to_int()
    Value::Bool(b) => if b { 1 } else { 0 }
    _ => 0
  }
}

///|
/// Row `row` of `vector` coerced to `Int64` (0 for NULL/unconvertible rows).
fn arrow_row_int64(vector : Vector, row : Int) -> Int64 {
  match vector.value_at(row) {
    Value::Int(n) => n.to_int64()
    Value::Int64(n) => n
    Value::UInt64(n) => n.to_int64()
    Value::Double(d) => d.to_int64()
    Value::Bool(b) => if b { 1L } else { 0L }
    _ => 0L
  }
}

///|
/// Row `row` of `vector` coerced to `Double` (0.0 for NULL/unconvertible).
fn arrow_row_double(vector : Vector, row : Int) -> Double {
  match vector.value_at(row) {
    Value::Double(d) => d
    Value::Int(n) => n.to_double()
    Value::Int64(n) => n.to_double()
    Value::UInt64(n) => n.to_double()
    Value::Bool(b) => if b { 1.0 } else { 0.0 }
    _ => 0.0
  }
}

///|
/// Row `row` of `vector` coerced to `Bool` (false for NULL/unconvertible).
fn arrow_row_bool(vector : Vector, row : Int) -> Bool {
  match vector.value_at(row) {
    Value::Bool(b) => b
    Value::Int(n) => n != 0
    Value::Int64(n) => n != 0L
    _ => false
  }
}

///|
/// Row `row` of `vector` rendered to `String` ("" for NULL rows), matching
/// the legacy `duckdb_value_varchar` coercion semantics.
fn arrow_row_string(vector : Vector, row : Int) -> String {
  if !vector.validity[row] {
    ""
  } else {
    vector.string_at(row)
  }
}

///|
/// Column `col` flattened to `Int` values across all chunks (NULL rows
/// yield 0).
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::ints/value_at for new code")
pub fn ArrowResult::get_column_int32(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let values : Array[Int] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_int(vector, row))
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  values
}

///|
/// Column `col` flattened to `Int64` values across all chunks (NULL rows
/// yield 0). Note: this getter previously returned `Array[Int]`, which
/// truncated BIGINT to 32 bits; it now keeps full Int64 precision.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::int64s/value_at for new code")
pub fn ArrowResult::get_column_int64(
  self : ArrowResult,
  col : Int,
) -> Array[Int64] {
  let values : Array[Int64] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_int64(vector, row))
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  values
}

///|
/// Column `col` flattened to `Double` values across all chunks (NULL rows
/// yield 0.0).
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::doubles/value_at for new code")
pub fn ArrowResult::get_column_double(
  self : ArrowResult,
  col : Int,
) -> Array[Double] {
  let values : Array[Double] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_double(vector, row))
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  values
}

///|
/// Column `col` flattened to `Bool` values across all chunks (NULL rows
/// yield false).
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::bools/value_at for new code")
pub fn ArrowResult::get_column_bool(
  self : ArrowResult,
  col : Int,
) -> Array[Bool] {
  let values : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_bool(vector, row))
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  values
}

///|
/// Column `col` flattened to rendered strings across all chunks (NULL rows
/// yield "").
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::strings/string_at for new code")
pub fn ArrowResult::get_column_string(
  self : ArrowResult,
  col : Int,
) -> Array[String] {
  let values : Array[String] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_string(vector, row))
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  values
}

///|
/// Column `col` flattened to `(values, validity)` across all chunks.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::ints and Vector::is_null for new code")
pub fn ArrowResult::get_column_int32_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Int], Array[Bool]) {
  let values : Array[Int] = []
  let validity : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_int(vector, row))
          validity.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  (values, validity)
}

///|
/// Column `col` flattened to `(values, validity)` across all chunks.
/// See `get_column_int64` for the Int -> Int64 precision fix.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::int64s and Vector::is_null for new code")
pub fn ArrowResult::get_column_int64_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Int64], Array[Bool]) {
  let values : Array[Int64] = []
  let validity : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_int64(vector, row))
          validity.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  (values, validity)
}

///|
/// Column `col` flattened to `(values, validity)` across all chunks.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::doubles and Vector::is_null for new code")
pub fn ArrowResult::get_column_double_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Double], Array[Bool]) {
  let values : Array[Double] = []
  let validity : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_double(vector, row))
          validity.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  (values, validity)
}

///|
/// Column `col` flattened to `(values, validity)` across all chunks.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::strings and Vector::is_null for new code")
pub fn ArrowResult::get_column_string_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[String], Array[Bool]) {
  let values : Array[String] = []
  let validity : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_string(vector, row))
          validity.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  (values, validity)
}

///|
/// Column `col` flattened to `(values, validity)` across all chunks.
#deprecated("arrow getters now read canonical typed vectors; use ArrowResult::to_chunks with Vector::bools and Vector::is_null for new code")
pub fn ArrowResult::get_column_bool_nullable(
  self : ArrowResult,
  col : Int,
) -> (Array[Bool], Array[Bool]) {
  let values : Array[Bool] = []
  let validity : Array[Bool] = []
  for chunk in arrow_open_chunks(self) {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          values.push(arrow_row_bool(vector, row))
          validity.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => ()
    }
  } nobreak {
    ()
  }
  (values, validity)
}

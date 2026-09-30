// ============================================================================
// Generic extension primitives (issue #67)
//
// Extension-agnostic building blocks for DuckDB extensions: INSTALL, LOAD,
// and ATTACH. Extension-specific convenience helpers (Quack, Iceberg, ...)
// live in their own packages (e.g. `f4ah6o/duckdb/quack`) and compose these
// primitives instead of growing the `Connection` surface.
// ============================================================================

///|
/// Escape `value` as a single-quoted SQL string literal.
fn sql_string_literal(value : String) -> String {
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
/// Whether `c` is valid in a bare SQL identifier (letters, digits, underscore).
fn is_sql_identifier_char(c : Char) -> Bool {
  (c >= 'a' && c <= 'z') ||
  (c >= 'A' && c <= 'Z') ||
  (c >= '0' && c <= '9') ||
  c == '_'
}

///|
/// Whether `c` may start a bare SQL identifier (letter or underscore —
/// leading digits are only legal inside a quoted identifier).
fn is_sql_identifier_start_char(c : Char) -> Bool {
  (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

///|
/// Validate `value` as a non-empty SQL identifier made of
/// `is_sql_identifier_char` characters, returning it verbatim on success.
/// Usable for double-quoted contexts (e.g. ATTACH aliases); bare
/// identifiers additionally need `bare_sql_identifier`.
fn sql_identifier(
  value : String,
  argument~ : String,
) -> Result[String, DuckDBError] {
  if value.length() == 0 {
    Err(DuckDBError::InvalidArgument(argument~, reason="must not be empty"))
  } else {
    for c in value {
      if !is_sql_identifier_char(c) {
        return Err(
          DuckDBError::InvalidArgument(
            argument~,
            reason="may only contain letters, digits, and underscores",
          ),
        )
      }
    }
    Ok(value)
  }
}

///|
/// Validate `value` as a bare SQL identifier rendered unquoted (extension
/// names, repositories): `sql_identifier` plus a letter/underscore first
/// character, since a leading digit is not valid bare.
fn bare_sql_identifier(
  value : String,
  argument~ : String,
) -> Result[String, DuckDBError] {
  match sql_identifier(value, argument~) {
    Err(err) => Err(err)
    Ok(value) => {
      let mut start_ok = false
      for c in value {
        start_ok = is_sql_identifier_start_char(c)
        break
      }
      if start_ok {
        Ok(value)
      } else {
        Err(
          DuckDBError::InvalidArgument(
            argument~,
            reason="must start with a letter or underscore",
          ),
        )
      }
    }
  }
}

///|
/// Adapt a `QueryResult` callback into a `Unit` callback, discarding rows.
fn unit_done(
  result : Result[QueryResult, DuckDBError],
  on_done : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  match result {
    Ok(_) => on_done(Ok(()))
    Err(err) => on_done(Err(err))
  }
}

///|
/// SQL for `INSTALL name` / `FORCE INSTALL name [FROM repository]`.
fn install_extension_sql(
  name : String,
  repository : String,
  force : Bool,
) -> Result[String, DuckDBError] {
  match bare_sql_identifier(name, argument="name") {
    Err(err) => Err(err)
    Ok(name) =>
      if repository == "" {
        Ok((if force { "FORCE " } else { "" }) + "INSTALL " + name)
      } else {
        match bare_sql_identifier(repository, argument="repository") {
          Err(err) => Err(err)
          Ok(repository) =>
            Ok(
              (if force { "FORCE " } else { "" }) +
              "INSTALL " +
              name +
              " FROM " +
              repository,
            )
        }
      }
  }
}

///|
/// SQL for `ATTACH 'uri' [AS "alias"] [(options)]`.
fn attach_sql(
  uri : String,
  db_alias : String,
  options : String,
) -> Result[String, DuckDBError] {
  let sb = StringBuilder()
  ignore(sb..write_view("ATTACH ").write_view(sql_string_literal(uri)))
  if db_alias != "" {
    match sql_identifier(db_alias, argument="alias") {
      Ok(db_alias) =>
        ignore(sb..write_view(" AS \"")..write_view(db_alias).write_char('"'))
      Err(err) => return Err(err)
    }
  }
  if options != "" {
    ignore(sb..write_view(" (")..write_view(options).write_char(')'))
  }
  Ok(sb.to_string())
}

///|
/// Install a DuckDB extension by `name` (`INSTALL` / `FORCE INSTALL`).
///
/// `repository` optionally names the extension repository to install from
/// (for example `core_nightly`); `force` re-installs even when the extension
/// is already installed. Names are validated as plain identifiers — anything
/// else returns `InvalidArgument` without touching the database.
pub fn Connection::install_extension(
  self : Connection,
  name : String,
  repository? : String = "",
  force? : Bool = false,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  match install_extension_sql(name, repository, force) {
    Ok(sql) =>
      self.query(sql, on_done=fn(result) { unit_done(result, on_done) })
    Err(err) => on_done(Err(err))
  }
}

///|
/// SQL for `LOAD name`.
fn load_extension_sql(name : String) -> Result[String, DuckDBError] {
  match bare_sql_identifier(name, argument="name") {
    Ok(name) => Ok("LOAD " + name)
    Err(err) => Err(err)
  }
}

///|
/// Load an installed DuckDB extension into this connection (`LOAD name`).
pub fn Connection::load_extension(
  self : Connection,
  name : String,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  match load_extension_sql(name) {
    Ok(sql) =>
      self.query(sql, on_done=fn(result) { unit_done(result, on_done) })
    Err(err) => on_done(Err(err))
  }
}

///|
/// Attach a database file or endpoint to this connection (`ATTACH 'uri'`).
///
/// `db_alias` optionally names the attached catalog; it is validated as a
/// plain identifier and rendered double-quoted. `options` is rendered
/// verbatim inside the trailing parenthesized clause — the escape hatch for
/// extension-specific attach options (for example
/// `TYPE quack, TOKEN 'secret'`). Callers building `options` from user input
/// must validate and escape values themselves.
pub fn Connection::attach(
  self : Connection,
  uri : String,
  db_alias? : String = "",
  options? : String = "",
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  match attach_sql(uri, db_alias, options) {
    Ok(sql) =>
      self.query(sql, on_done=fn(result) { unit_done(result, on_done) })
    Err(err) => on_done(Err(err))
  }
}

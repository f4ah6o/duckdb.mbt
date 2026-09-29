///|
/// Whitebox coverage for structured `DuckDBError` classification. Runs on
/// every supported target since it exercises message translation only.
test "error classification: engine error type from message prefix" {
  match query_error("Parser Error: syntax error at or near \"SELEC\"") {
    DuckDB(error_type~, message~) => {
      inspect(error_type, content="parser")
      inspect(
        message,
        content="Parser Error: syntax error at or near \"SELEC\"",
      )
    }
    err => fail("expected DuckDB variant, got \{err.message()}")
  }
}

///|
test "error classification: engine type normalizes and strips Error suffix" {
  match prepare_error("Invalid Input Error: bad value") {
    DuckDB(error_type~, ..) => inspect(error_type, content="invalid_input")
    err => fail("expected DuckDB variant, got \{err.message()}")
  }
}

///|
test "error classification: closed handle sentinel" {
  match backend_error("js", "ERR_CLOSED:connection") {
    Closed(resource~) => {
      inspect(resource, content="connection")
      inspect(Closed(resource~).message(), content="connection is closed")
    }
    err => fail("expected Closed variant, got \{err.message()}")
  }
}

///|
test "error classification: unsupported feature sentinels" {
  match
    bind_error("WASM backend does not support direct BLOB prepared parameters") {
    Unsupported(feature~, backend~) => {
      inspect(feature, content="direct blob prepared parameters")
      inspect(backend, content="wasm")
    }
    err => fail("expected Unsupported variant, got \{err.message()}")
  }
  match
    append_error(
      "Appender is not supported for WASM backend. Use INSERT statements instead.",
    ) {
    Unsupported(feature~, backend~) => {
      inspect(feature, content="appender")
      inspect(backend, content="wasm")
    }
    err => fail("expected Unsupported variant, got \{err.message()}")
  }
  match bind_error("Appender not yet supported for JS backend") {
    Unsupported(feature~, backend~) => {
      inspect(feature, content="appender")
      inspect(backend, content="js")
    }
    err => fail("expected Unsupported variant, got \{err.message()}")
  }
}

///|
test "error classification: unclassified messages keep the stage variant" {
  match query_error("duckdb_query failed") {
    Query(message) => inspect(message, content="duckdb_query failed")
    err => fail("expected Query variant, got \{err.message()}")
  }
  match prepare_error("duckdb_prepare failed") {
    Prepare(message) => inspect(message, content="duckdb_prepare failed")
    err => fail("expected Prepare variant, got \{err.message()}")
  }
  match bind_error("bind_int failed") {
    Bind(message) => inspect(message, content="bind_int failed")
    err => fail("expected Bind variant, got \{err.message()}")
  }
  match append_error("append_int failed") {
    Append(message) => inspect(message, content="append_int failed")
    err => fail("expected Append variant, got \{err.message()}")
  }
  match backend_error("native", "duckdb_open failed") {
    Backend(backend~, message~) => {
      inspect(backend, content="native")
      inspect(message, content="duckdb_open failed")
    }
    err => fail("expected Backend variant, got \{err.message()}")
  }
}

///|
test "error classification: native null-handle sentinels" {
  match query_error("connection is null") {
    Closed(resource~) => inspect(resource, content="connection")
    err => fail("expected Closed variant, got \{err.message()}")
  }
  match bind_error("statement is null") {
    Closed(resource~) => inspect(resource, content="statement")
    err => fail("expected Closed variant, got \{err.message()}")
  }
  match append_error("appender is null") {
    Closed(resource~) => inspect(resource, content="appender")
    err => fail("expected Closed variant, got \{err.message()}")
  }
}

///|
test "error accessors: message and error_type" {
  let variants = [
    Query("q"),
    Prepare("p"),
    Bind("b"),
    Append("a"),
    Unsupported(feature="appender", backend="wasm"),
    Closed(resource="stream"),
    InvalidArgument(argument="alias", reason="must not be empty"),
    Backend(backend="native", message="boom"),
    DuckDB(error_type="catalog", message="table not found"),
  ]
  let messages = variants.map(fn(err) { err.message() })
  inspect(
    messages,
    content=(
      #|[q, p, b, a, appender is not supported on the wasm backend, stream is closed, invalid argument alias: must not be empty, boom, table not found]
    ),
  )
  inspect(
    DuckDBError::DuckDB(error_type="catalog", message="x").error_type(),
    content="Some(catalog)",
  )
  inspect(Query("x").error_type(), content="None")
}

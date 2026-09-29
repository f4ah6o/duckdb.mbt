///|
// Runtime conformance for `Connection::capabilities` / capability gating on
// the JS target. Fabricated handle objects carry the same `kind` tags the
// real FFI shims produce, so the MoonBit-side detection and guards run
// against node and wasm shapes without needing a live wasm engine.

///|
extern "js" fn js_fake_connection(backend : Int) -> @duckdb.Connection =
  #|(backend) => ({ kind: backend === 2 ? "wasm" : "node" })

///|
extern "js" fn js_fake_statement(backend : Int) -> @duckdb.PreparedStatement =
  #|(backend) => ({
  #|  kind: "prepared",
  #|  backend: backend === 2 ? "wasm" : "node",
  #|  connection: { kind: backend === 2 ? "wasm" : "node" },
  #|  params: [],
  #|  statement: {
  #|    bindBlob: () => { throw new Error("fake bind") },
  #|    bind: () => { throw new Error("fake bind") },
  #|    bindStruct: () => { throw new Error("fake bind") },
  #|  },
  #|})

///|
extern "js" fn js_fake_appender(backend : Int) -> @duckdb.Appender =
  #|(backend) => ({
  #|  kind: "appender",
  #|  connection: { kind: backend === 2 ? "wasm" : "node" },
  #|})

///|
fn assert_unsupported(
  result : Result[Unit, @duckdb.DuckDBError],
  feature~ : String,
  backend~ : String,
) -> Unit raise {
  match result {
    Err(
      @duckdb.DuckDBError::Unsupported(
        feature=actual_feature,
        backend=actual_backend
      )
    ) => {
      assert_eq(actual_feature, feature)
      assert_eq(actual_backend, backend)
    }
    Err(err) => fail("expected Unsupported, got \{err.message()}")
    Ok(_) => fail("expected Unsupported for \{feature} on \{backend}")
  }
}

///|
test "js backend detection for connections, statements, appenders" {
  inspect(js_fake_connection(1).backend(), content="Node")
  inspect(js_fake_connection(2).backend(), content="Wasm")
  inspect(js_fake_statement(1).backend(), content="Node")
  inspect(js_fake_statement(2).backend(), content="Wasm")
  inspect(js_fake_appender(1).backend(), content="Node")
  inspect(js_fake_appender(2).backend(), content="Wasm")
  let wasm_caps = js_fake_connection(2).capabilities()
  inspect(wasm_caps.appender, content="false")
  inspect(wasm_caps.blob_bind, content="false")
  inspect(wasm_caps.decimal_bind, content="true")
}

///|
test "wasm appender is gated by capabilities before reaching the FFI" {
  let result_ref : Ref[Result[@duckdb.Appender, @duckdb.DuckDBError]?] = Ref::new(
    None,
  )
  js_fake_connection(2).create_appender("", "t", on_done=result => {
    result_ref.val = Some(result)
  })
  match result_ref.val {
    Some(Err(err)) => {
      inspect(
        err.message(),
        content="appender is not supported on the wasm backend",
      )
      match err {
        @duckdb.DuckDBError::Unsupported(feature~, backend~) => {
          inspect(feature, content="appender")
          inspect(backend, content="wasm")
        }
        _ => fail("expected Unsupported, got \{err.message()}")
      }
    }
    Some(Ok(_)) => fail("wasm appender must be unsupported")
    None => fail("create_appender produced no result")
  }
}

///|
test "wasm advanced-type binds are gated by capabilities" {
  let stmt = js_fake_statement(2)
  inspect(stmt.capabilities().backend, content="Wasm")
  assert_unsupported(
    stmt.bind_blob(1, b"xy"),
    feature="direct blob prepared parameters",
    backend="wasm",
  )
  assert_unsupported(
    stmt.bind_list_varchar(1, ["a"]),
    feature="direct list prepared parameters",
    backend="wasm",
  )
  assert_unsupported(
    stmt.bind_struct(1, @duckdb.struct_from_arrays(["a"], ["1"])),
    feature="direct struct prepared parameters",
    backend="wasm",
  )
  assert_unsupported(
    stmt.bind_map(1, @duckdb.map_from_arrays(["k"], ["v"])),
    feature="direct map prepared parameters",
    backend="wasm",
  )
}

///|
test "node capabilities do not gate supported binds" {
  // The same require() check the bind/appender wrappers run before the FFI:
  // it must succeed for every capability node reports as supported.
  let caps = js_fake_statement(1).capabilities()
  inspect(caps.require(BackendFeature::BlobBind) is Ok(_), content="true")
  inspect(caps.require(BackendFeature::ListBind) is Ok(_), content="true")
  inspect(caps.require(BackendFeature::Appender) is Ok(_), content="true")
  let wasm_caps = js_fake_statement(2).capabilities()
  inspect(wasm_caps.require(BackendFeature::BlobBind) is Ok(_), content="false")
}

///|
test "js appender date/timestamp errors name the concrete backend" {
  match js_fake_appender(1).append_date(0) {
    Err(@duckdb.DuckDBError::Unsupported(feature~, backend~)) => {
      inspect(feature, content="appender append_date")
      inspect(backend, content="node")
    }
    _ => fail("expected Unsupported error")
  }
  match js_fake_appender(2).append_timestamp(0L) {
    Err(@duckdb.DuckDBError::Unsupported(feature~, backend~)) => {
      inspect(feature, content="appender append_timestamp")
      inspect(backend, content="wasm")
    }
    _ => fail("expected Unsupported error")
  }
}

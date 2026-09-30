///|
/// Whitebox conformance tests for the backend capability table. The matrix is
/// pure data, so these run on native and js while documenting the wasm and
/// unsupported-target rows from the same source of truth.
test "capability table: per-backend flags" {
  let native = Backend::Native.capabilities()
  let node = Backend::Node.capabilities()
  let wasm = Backend::Wasm.capabilities()
  let unsupported = Backend::Unsupported.capabilities()
  inspect(native.backend, content="Native")
  inspect(node.backend, content="Node")
  inspect(wasm.backend, content="Wasm")
  inspect(unsupported.backend, content="Unsupported")
  inspect(native.appender, content="true")
  inspect(node.appender, content="true")
  inspect(wasm.appender, content="false")
  inspect(node.appender_date_timestamp, content="false")
  inspect(native.persistent_storage, content="true")
  inspect(node.persistent_storage, content="true")
  inspect(wasm.persistent_storage, content="false")
  inspect(wasm.quack, content="false")
  inspect(wasm.decimal_bind, content="true")
  inspect(wasm.interval_bind, content="true")
  inspect(wasm.blob_bind, content="false")
  inspect(node.nested_bind, content="true")
  inspect(node.nested_append, content="false")
  inspect(unsupported.connect, content="false")
}

///|
test "capability supports() agrees with the table" {
  let wasm = Backend::Wasm.capabilities()
  inspect(wasm.supports(BackendFeature::BlobBind), content="false")
  inspect(wasm.supports(BackendFeature::ListBind), content="false")
  inspect(wasm.supports(BackendFeature::StructBind), content="false")
  inspect(wasm.supports(BackendFeature::MapBind), content="false")
  inspect(wasm.supports(BackendFeature::DecimalBind), content="true")
  inspect(wasm.supports(BackendFeature::Streaming), content="true")
  let node = Backend::Node.capabilities()
  inspect(node.supports(BackendFeature::NestedAppend), content="false")
  inspect(node.supports(BackendFeature::ListBind), content="true")
  inspect(node.supports(BackendFeature::AppenderDateTimestamp), content="false")
  for backend in Backend::all() {
    let caps = backend.capabilities()
    for feature in BackendFeature::all() {
      let supported = caps.supports(feature)
      if backend == Backend::Unsupported && supported {
        fail("unsupported backend must not support \{feature.name()}")
      }
      if !supported {
        match caps.require(feature) {
          Err(DuckDBError::Unsupported(backend=name, ..)) =>
            if name != backend.name() {
              fail(
                "feature \{feature.name()} reported backend \{name}, expected \{backend.name()}",
              )
            }
          _ => fail("require(\{feature.name()}) must fail on \{backend.name()}")
        }
      }
    }
  }
}

///|
test "capabilities: require returns structured Unsupported errors" {
  match Backend::Wasm.capabilities().require(BackendFeature::Appender) {
    Err(DuckDBError::Unsupported(feature~, backend~, message~)) => {
      inspect(feature, content="appender")
      inspect(backend, content="wasm")
      inspect(message, content="appender is not supported on the wasm backend")
    }
    _ => fail("expected Unsupported error")
  }
  match Backend::Wasm.capabilities().require(BackendFeature::BlobBind) {
    Err(DuckDBError::Unsupported(feature~, backend~, ..)) => {
      inspect(feature, content="direct blob prepared parameters")
      inspect(backend, content="wasm")
    }
    _ => fail("expected Unsupported error")
  }
  match Backend::Node.capabilities().require(BackendFeature::NestedAppend) {
    Err(DuckDBError::Unsupported(feature~, backend~, ..)) => {
      inspect(feature, content="nested appender appends")
      inspect(backend, content="node")
    }
    _ => fail("expected Unsupported error")
  }
  match Backend::Native.capabilities().require(BackendFeature::Appender) {
    Ok(_) => ()
    Err(err) => fail("native appender should be supported: \{err.message()}")
  }
}

///|
/// The README feature matrix is generated from `BackendCapabilities`; this
/// snapshot doubles as the generated output reviewed in tests.
test "capabilities: README support matrix is generated from this table" {
  inspect(
    support_matrix_markdown(),
    content=(
      #|| Feature | Native | JS (Node) | JS (WASM) |
      #||---------|--------|-----------|-----------|
      #|| Connection & Query | ✅ | ✅ | ✅ |
      #|| Prepared Statements | ✅ | ✅ | ✅ |
      #|| Streaming Results | ✅ | ✅ | ✅ |
      #|| Appender | ✅ | ✅ (Node only) | ❌ |
      #|| Arrow Integration | ✅ | ✅ | ✅ |
      #|| Advanced Types | ⚠️ | ⚠️ | ⚠️ |
      #|
      #|**Legend:** ✅ Full support | ⚠️ Partial support | ❌ Not supported
      #|
      #|### Advanced Types Detailed Support
      #|
      #|| Type | Native Bind | Native Append | Node Bind | Node Append | WASM Bind |
      #||------|-------------|---------------|-----------|-------------|-----------|
      #|| Decimal | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ✅ direct string param + SQL cast |
      #|| Interval | ✅ | ✅ | ✅ | ✅ | ✅ direct string param + SQL cast |
      #|| Blob | ✅ | ✅ | ✅ | ✅ | ❌ unsupported |
      #|| List | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
      #|| Struct | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
      #|| Map | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
      #|
    ),
  )
}

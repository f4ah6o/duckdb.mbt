///|
/// Runtime backend capability model (see issue #65).
///
/// `Backend` names the concrete engine a connection runs on, and
/// `BackendCapabilities` records which features that engine supports.
/// The same table drives `DuckDBError::Unsupported` for gated operations and
/// the generated README support matrix (`support_matrix_markdown`).

// ============================================================================
// Backend identity
// ============================================================================

///|
/// A concrete backend a `Connection` runs on. Unlike `JsBackend` this has no
/// `Auto` selector — every variant names a real engine.
pub(all) enum Backend {
  /// Native target, linked against `libduckdb`.
  Native
  /// MoonBit JS target using `@duckdb/node-api`.
  Node
  /// MoonBit JS target using `@duckdb/duckdb-wasm` in the browser.
  Wasm
  /// A MoonBit target with no working binding (wasm/wasm-gc stubs).
  Unsupported
} derive(Eq, Compare, Show)

///|
/// All concrete backends, in display order.
pub fn Backend::all() -> Array[Backend] {
  [Native, Node, Wasm, Unsupported]
}

///|
/// Short stable name used in `DuckDBError::Unsupported(backend~)` and
/// generated documentation.
pub fn Backend::name(self : Backend) -> String {
  match self {
    Native => "native"
    Node => "node"
    Wasm => "wasm"
    Unsupported => "unsupported target"
  }
}

///|
/// The capabilities this backend provides.
pub fn Backend::capabilities(self : Backend) -> BackendCapabilities {
  match self {
    Native =>
      {
        backend: Native,
        connect: true,
        prepared_statements: true,
        streaming: true,
        appender: true,
        appender_date_timestamp: true,
        arrow: true,
        persistent_storage: true,
        quack: true,
        decimal_bind: true,
        decimal_append: true,
        interval_bind: true,
        interval_append: true,
        blob_bind: true,
        blob_append: true,
        nested_bind: true,
        nested_append: true,
        nested_typed: false,
      }
    Node =>
      {
        backend: Node,
        connect: true,
        prepared_statements: true,
        streaming: true,
        appender: true,
        appender_date_timestamp: false,
        arrow: true,
        persistent_storage: true,
        quack: true,
        decimal_bind: true,
        decimal_append: true,
        interval_bind: true,
        interval_append: true,
        blob_bind: true,
        blob_append: true,
        nested_bind: true,
        nested_append: false,
        nested_typed: false,
      }
    Wasm =>
      {
        backend: Wasm,
        connect: true,
        prepared_statements: true,
        streaming: true,
        appender: false,
        appender_date_timestamp: false,
        arrow: true,
        persistent_storage: false,
        quack: false,
        decimal_bind: true,
        decimal_append: false,
        interval_bind: true,
        interval_append: false,
        blob_bind: false,
        blob_append: false,
        nested_bind: false,
        nested_append: false,
        nested_typed: false,
      }
    Unsupported =>
      {
        backend: Unsupported,
        connect: false,
        prepared_statements: false,
        streaming: false,
        appender: false,
        appender_date_timestamp: false,
        arrow: false,
        persistent_storage: false,
        quack: false,
        decimal_bind: false,
        decimal_append: false,
        interval_bind: false,
        interval_append: false,
        blob_bind: false,
        blob_append: false,
        nested_bind: false,
        nested_append: false,
        nested_typed: false,
      }
  }
}

// ============================================================================
// Backend capabilities
// ============================================================================

///|
/// A single capability flag, for feature checks that need a stable name
/// (error reporting, conformance tests, documentation generation).
/// List/Struct/Map prepared binds share the `nested_bind` capability but keep
/// distinct feature variants so error messages stay specific.
pub(all) enum BackendFeature {
  /// `connect` can open a connection at all.
  Connect
  /// Prepared statements.
  PreparedStatements
  /// `query_stream` / `ResultStream` chunked reads.
  Streaming
  /// `Connection::create_appender` bulk loading.
  Appender
  /// `Appender::append_date` / `Appender::append_timestamp` helpers.
  AppenderDateTimestamp
  /// Arrow query results (`query_arrow`).
  Arrow
  /// File-backed databases (any path other than `:memory:`).
  PersistentStorage
  /// Quack extension helpers (`install_quack`, `load_quack`, ...).
  Quack
  /// Decimal prepared-statement binds (WASM via string params + SQL cast).
  DecimalBind
  /// Decimal appender appends.
  DecimalAppend
  /// Interval prepared-statement binds (WASM via string params + SQL cast).
  IntervalBind
  /// Interval appender appends.
  IntervalAppend
  /// Blob prepared-statement binds.
  BlobBind
  /// Blob appender appends.
  BlobAppend
  /// Direct LIST prepared binds (VARCHAR elements).
  ListBind
  /// Direct STRUCT prepared binds (VARCHAR fields).
  StructBind
  /// Direct MAP prepared binds (VARCHAR keys/values).
  MapBind
  /// LIST/STRUCT/MAP appender appends (VARCHAR elements).
  NestedAppend
  /// Nested element types beyond VARCHAR.
  NestedTyped
} derive(Eq, Compare, Show)

///|
/// All capability features, in display order.
pub fn BackendFeature::all() -> Array[BackendFeature] {
  [
    Connect,
    PreparedStatements,
    Streaming,
    Appender,
    AppenderDateTimestamp,
    Arrow,
    PersistentStorage,
    Quack,
    DecimalBind,
    DecimalAppend,
    IntervalBind,
    IntervalAppend,
    BlobBind,
    BlobAppend,
    ListBind,
    StructBind,
    MapBind,
    NestedAppend,
    NestedTyped,
  ]
}

///|
/// Human-readable feature name used in `DuckDBError::Unsupported(feature~)`.
pub fn BackendFeature::name(self : BackendFeature) -> String {
  match self {
    Connect => "connect"
    PreparedStatements => "prepared statements"
    Streaming => "streaming results"
    Appender => "appender"
    AppenderDateTimestamp => "appender date/timestamp helpers"
    Arrow => "arrow integration"
    PersistentStorage => "persistent storage"
    Quack => "quack extension"
    DecimalBind => "decimal prepared parameters"
    DecimalAppend => "decimal appender appends"
    IntervalBind => "interval prepared parameters"
    IntervalAppend => "interval appender appends"
    BlobBind => "direct blob prepared parameters"
    BlobAppend => "blob appender appends"
    ListBind => "direct list prepared parameters"
    StructBind => "direct struct prepared parameters"
    MapBind => "direct map prepared parameters"
    NestedAppend => "nested appender appends"
    NestedTyped => "nested types beyond varchar"
  }
}

///|
/// What a backend supports. The bool fields map 1:1 to `BackendFeature`
/// variants via `supports`; the struct is the single source of truth for the
/// README feature matrix and for structured `Unsupported` errors.
pub struct BackendCapabilities {
  /// The backend these capabilities describe.
  backend : Backend
  /// `connect` can open a connection at all.
  connect : Bool
  /// Prepared statements.
  prepared_statements : Bool
  /// `query_stream` / `ResultStream` chunked reads.
  streaming : Bool
  /// `Connection::create_appender` bulk loading.
  appender : Bool
  /// `Appender::append_date` / `Appender::append_timestamp` helpers.
  appender_date_timestamp : Bool
  /// Arrow query results (`query_arrow`).
  arrow : Bool
  /// File-backed databases (any path other than `:memory:`).
  persistent_storage : Bool
  /// Quack extension helpers (`install_quack`, `load_quack`, ...).
  quack : Bool
  /// Decimal prepared-statement binds (WASM via string params + SQL cast).
  decimal_bind : Bool
  /// Decimal appender appends.
  decimal_append : Bool
  /// Interval prepared-statement binds (WASM via string params + SQL cast).
  interval_bind : Bool
  /// Interval appender appends.
  interval_append : Bool
  /// Blob prepared-statement binds.
  blob_bind : Bool
  /// Blob appender appends.
  blob_append : Bool
  /// Direct LIST/STRUCT/MAP prepared binds (VARCHAR elements only).
  nested_bind : Bool
  /// LIST/STRUCT/MAP appender appends (VARCHAR elements only).
  nested_append : Bool
  /// Nested element types beyond VARCHAR.
  nested_typed : Bool
}

///|
/// Whether this backend supports `feature`.
pub fn BackendCapabilities::supports(
  self : BackendCapabilities,
  feature : BackendFeature,
) -> Bool {
  match feature {
    Connect => self.connect
    PreparedStatements => self.prepared_statements
    Streaming => self.streaming
    Appender => self.appender
    AppenderDateTimestamp => self.appender_date_timestamp
    Arrow => self.arrow
    PersistentStorage => self.persistent_storage
    Quack => self.quack
    DecimalBind => self.decimal_bind
    DecimalAppend => self.decimal_append
    IntervalBind => self.interval_bind
    IntervalAppend => self.interval_append
    BlobBind => self.blob_bind
    BlobAppend => self.blob_append
    ListBind | StructBind | MapBind => self.nested_bind
    NestedAppend => self.nested_append
    NestedTyped => self.nested_typed
  }
}

///|
/// A structured `Unsupported` error for `feature` on this backend.
pub fn BackendCapabilities::unsupported_error(
  self : BackendCapabilities,
  feature : BackendFeature,
) -> DuckDBError {
  unsupported_error(feature.name(), self.backend.name())
}

///|
/// A structured `Unsupported` error for a named operation on this backend,
/// for sites that carry a more specific feature string than `BackendFeature`
/// variants (e.g. "appender append_date").
pub fn BackendCapabilities::unsupported(
  self : BackendCapabilities,
  feature : String,
) -> DuckDBError {
  unsupported_error(feature, self.backend.name())
}

///|
/// `Ok(())` when this backend supports `feature`, otherwise the structured
/// `Unsupported` error — used to gate operations before hitting the FFI
/// layer.
pub fn BackendCapabilities::require(
  self : BackendCapabilities,
  feature : BackendFeature,
) -> Result[Unit, DuckDBError] {
  if self.supports(feature) {
    Ok(())
  } else {
    Err(self.unsupported_error(feature))
  }
}

// ============================================================================
// Runtime inspection
// ============================================================================

///|
/// The capabilities of the backend `self` is connected to. Runtime access to
/// the matrix: `conn.capabilities()` answers "can I use feature X here?"
/// without reaching for the docs.
pub fn Connection::capabilities(self : Connection) -> BackendCapabilities {
  self.backend().capabilities()
}

///|
/// The capabilities of the backend `self` was prepared on.
pub fn PreparedStatement::capabilities(
  self : PreparedStatement,
) -> BackendCapabilities {
  self.backend().capabilities()
}

///|
/// The capabilities of the backend `self` appends to.
pub fn Appender::capabilities(self : Appender) -> BackendCapabilities {
  self.backend().capabilities()
}

// ============================================================================
// Support matrix rendering
// ============================================================================

///|
/// Markdown cell for a supported/unsupported flag with per-cell notes.
fn matrix_cell(ok : Bool, yes : String, no : String) -> String {
  if ok {
    yes
  } else {
    no
  }
}

///|
/// Summary status for the "Advanced Types" row: full support requires every
/// bind/append flag plus non-VARCHAR nested types; partial when at least one
/// advanced bind works.
fn matrix_advanced_status(caps : BackendCapabilities) -> String {
  let binds = caps.decimal_bind &&
    caps.interval_bind &&
    caps.blob_bind &&
    caps.nested_bind
  let appends = !caps.appender ||
    (
      caps.decimal_append &&
      caps.interval_append &&
      caps.blob_append &&
      caps.nested_append
    )
  if binds && appends && caps.nested_typed {
    "✅"
  } else if caps.decimal_bind ||
    caps.interval_bind ||
    caps.blob_bind ||
    caps.nested_bind {
    "⚠️"
  } else {
    "❌"
  }
}

///|
/// One row of the top-level feature table.
fn matrix_feature_row(
  name : String,
  native : String,
  node : String,
  wasm : String,
) -> String {
  "| \{name} | \{native} | \{node} | \{wasm} |\n"
}

///|
/// One row of the advanced types table.
fn matrix_type_row(
  name : String,
  native_bind : String,
  native_append : String,
  node_bind : String,
  node_append : String,
  wasm_bind : String,
) -> String {
  "| \{name} | \{native_bind} | \{native_append} | \{node_bind} | \{node_append} | \{wasm_bind} |\n"
}

///|
/// Render the README "Feature Support Matrix" section from
/// `BackendCapabilities`. `src/cmd/support_matrix` prints this and
/// `scripts/support_matrix.sh` splices it between the README's
/// `support-matrix` markers; CI diffs the result so the docs cannot drift
/// from the runtime capability table.
pub fn support_matrix_markdown() -> String {
  let native = Backend::Native.capabilities()
  let node = Backend::Node.capabilities()
  let wasm = Backend::Wasm.capabilities()
  let sb = StringBuilder::new()
  ignore(
    sb
    ..write_view("| Feature | Native | JS (Node) | JS (WASM) |\n")
    ..write_view("|---------|--------|-----------|-----------|\n")
    .write_view(
      matrix_feature_row(
        "Connection & Query",
        matrix_cell(native.connect, "✅", "❌"),
        matrix_cell(node.connect, "✅", "❌"),
        matrix_cell(wasm.connect, "✅", "❌"),
      ),
    ),
  )
  ignore(
    sb
    ..write_view(
      matrix_feature_row(
        "Prepared Statements",
        matrix_cell(native.prepared_statements, "✅", "❌"),
        matrix_cell(node.prepared_statements, "✅", "❌"),
        matrix_cell(wasm.prepared_statements, "✅", "❌"),
      ),
    )
    ..write_view(
      matrix_feature_row(
        "Streaming Results",
        matrix_cell(native.streaming, "✅", "❌"),
        matrix_cell(node.streaming, "✅", "❌"),
        matrix_cell(wasm.streaming, "✅", "❌"),
      ),
    )
    ..write_view(
      matrix_feature_row(
        "Appender",
        matrix_cell(native.appender, "✅", "❌"),
        matrix_cell(node.appender, "✅ (Node only)", "❌"),
        matrix_cell(wasm.appender, "✅", "❌"),
      ),
    )
    ..write_view(
      matrix_feature_row(
        "Arrow Integration",
        matrix_cell(native.arrow, "✅", "❌"),
        matrix_cell(node.arrow, "✅", "❌"),
        matrix_cell(wasm.arrow, "✅", "❌"),
      ),
    )
    .write_view(
      matrix_feature_row(
        "Advanced Types",
        matrix_advanced_status(native),
        matrix_advanced_status(node),
        matrix_advanced_status(wasm),
      ),
    ),
  )
  ignore(
    sb
    ..write_view("\n")
    ..write_view(
      "**Legend:** ✅ Full support | ⚠️ Partial support | ❌ Not supported\n",
    )
    ..write_view("\n### Advanced Types Detailed Support\n\n")
    ..write_view(
      "| Type | Native Bind | Native Append | Node Bind | Node Append | WASM Bind |\n",
    )
    ..write_view(
      "|------|-------------|---------------|-----------|-------------|-----------|\n",
    )
    ..write_view(
      matrix_type_row(
        "Decimal",
        matrix_cell(native.decimal_bind, "✅ 128-bit", "❌"),
        matrix_cell(native.decimal_append, "✅ 128-bit", "❌"),
        matrix_cell(node.decimal_bind, "✅ 128-bit", "❌"),
        matrix_cell(node.decimal_append, "✅ 128-bit", "❌"),
        matrix_cell(
          wasm.decimal_bind,
          "✅ direct string param + SQL cast",
          "❌ unsupported",
        ),
      ),
    )
    ..write_view(
      matrix_type_row(
        "Interval",
        matrix_cell(native.interval_bind, "✅", "❌"),
        matrix_cell(native.interval_append, "✅", "❌"),
        matrix_cell(node.interval_bind, "✅", "❌"),
        matrix_cell(node.interval_append, "✅", "❌"),
        matrix_cell(
          wasm.interval_bind,
          "✅ direct string param + SQL cast",
          "❌ unsupported",
        ),
      ),
    )
    ..write_view(
      matrix_type_row(
        "Blob",
        matrix_cell(native.blob_bind, "✅", "❌"),
        matrix_cell(native.blob_append, "✅", "❌"),
        matrix_cell(node.blob_bind, "✅", "❌"),
        matrix_cell(node.blob_append, "✅", "❌"),
        matrix_cell(wasm.blob_bind, "✅", "❌ unsupported"),
      ),
    )
    ..write_view(
      matrix_type_row(
        "List",
        matrix_cell(native.nested_bind, "✅ VARCHAR", "❌"),
        matrix_cell(native.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(node.nested_bind, "✅ VARCHAR (Node only)", "❌"),
        matrix_cell(node.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(wasm.nested_bind, "✅ VARCHAR", "❌ unsupported"),
      ),
    )
    ..write_view(
      matrix_type_row(
        "Struct",
        matrix_cell(native.nested_bind, "✅ VARCHAR", "❌"),
        matrix_cell(native.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(node.nested_bind, "✅ VARCHAR (Node only)", "❌"),
        matrix_cell(node.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(wasm.nested_bind, "✅ VARCHAR", "❌ unsupported"),
      ),
    )
    .write_view(
      matrix_type_row(
        "Map",
        matrix_cell(native.nested_bind, "✅ VARCHAR", "❌"),
        matrix_cell(native.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(node.nested_bind, "✅ VARCHAR (Node only)", "❌"),
        matrix_cell(node.nested_append, "✅ VARCHAR", "❌"),
        matrix_cell(wasm.nested_bind, "✅ VARCHAR", "❌ unsupported"),
      ),
    ),
  )
  sb.to_string()
}

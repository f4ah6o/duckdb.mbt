# f4ah6o/duckdb

MoonBit bindings for DuckDB on native and JavaScript targets.

## Targets

- **Native**: links against `libduckdb` via the DuckDB C API.
- **JavaScript**: compile with the MoonBit JS target and pick a backend at runtime:
  - `JsBackend::Node` uses `@duckdb/node-api`.
  - `JsBackend::Wasm` uses `@duckdb/duckdb-wasm` in the browser.
- **MoonBit wasm/wasm-gc targets are not supported** (they use stub implementations).

## Feature Support Matrix

The tables below are generated from the runtime `BackendCapabilities` table
(`src/duckdb_capabilities.mbt`) — the same data `conn.capabilities()` exposes
and capability-gated operations consult. Run `scripts/support_matrix.sh` after
changing capabilities; CI regenerates the section and fails on drift.

<!-- support-matrix:begin -->
<!-- Generated from BackendCapabilities (src/duckdb_capabilities.mbt) by scripts/support_matrix.sh -- do not edit by hand. -->
| Feature | Native | JS (Node) | JS (WASM) |
|---------|--------|-----------|-----------|
| Connection & Query | ✅ | ✅ | ✅ |
| Prepared Statements | ✅ | ✅ | ✅ |
| Streaming Results | ✅ | ✅ | ✅ |
| Appender | ✅ | ✅ (Node only) | ❌ |
| Appender DataChunk | ✅ | ✅ (Node only) | ❌ |
| Arrow Integration | ✅ | ✅ | ✅ |
| Advanced Types | ⚠️ | ⚠️ | ⚠️ |

**Legend:** ✅ Full support | ⚠️ Partial support | ❌ Not supported

### Advanced Types Detailed Support

| Type | Native Bind | Native Append | Node Bind | Node Append | WASM Bind |
|------|-------------|---------------|-----------|-------------|-----------|
| Decimal | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ✅ direct string param + SQL cast |
| Interval | ✅ | ✅ | ✅ | ✅ | ✅ direct string param + SQL cast |
| Blob | ✅ | ✅ | ✅ | ✅ | ❌ unsupported |
| List | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
| Struct | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
| Map | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR (Node only) | ❌ | ❌ unsupported |
<!-- support-matrix:end -->

**Notes:**
- Decimal carries the full DuckDB 128-bit scaled integer as `lower : UInt64` and
  `upper : Int64` halves (two's complement). `decimal_from_hugeint` builds a
  `Decimal` from halves, `decimal_from_parts`/`decimal_to_parts` convert
  to/from whole+fractional parts (`decimal_to_parts` returns `BigInt` values so
  whole parts beyond 64 bits are preserved, with a signed fractional remainder
  so negative sub-unit values like `-0.50` round-trip), and
  `decimal_to_double` may lose precision above 2^53, matching DuckDB semantics.
- List/Struct/Map are represented as string arrays (VARCHAR-only) and rely on DuckDB casting.
- JS (WASM) uses direct duckdb-wasm prepared parameters only. Verified advanced prepared-statement bind support is limited to Decimal and Interval string parameters with an explicit SQL cast, for example `?::DECIMAL(10,2)` or `?::INTERVAL`.
- JS (WASM) Blob, List, Struct, and Map direct prepared parameters are explicitly unsupported: the browser smoke test fails for them against `@duckdb/duckdb-wasm` 1.33.1-dev18.0 through 1.33.1-dev65.0.
- Appender date/timestamp helpers are only implemented for native targets.

### Arrow Integration

Basic support is available on all targets:
- Arrow query result type
- Schema extraction
- Column-based data access (native also exposes nullable getters)
- Supported types: BOOLEAN, INTEGER, VARCHAR, DOUBLE, BIGINT

**Note:** Complex types (List, Struct, Map) are not yet supported.

## CI Coverage

[GitHub Actions](.github/workflows/ci.yml) exercises the full public support
matrix on every PR. DuckDB versions under test are pinned and printed in the
job logs so failures can be attributed to a backend/version combination.

| Backend | Command | Runner | DuckDB under test |
|---------|---------|--------|-------------------|
| Native | `moon check --target native` + `moon test --target native` | `macos-latest`, `ubuntu-latest` | libduckdb `1.4.5`, `1.5.6` |
| JS (Node) | `moon check --target js` + `moon test --target js` | `ubuntu-latest`, Node 24 | `@duckdb/node-api` `1.4.3-r.3` (minimum), `1.5.6-r.1` (pinned) |
| JS (WASM) | `pnpm test:wasm-browser` (Playwright Chromium) | `ubuntu-latest` | `@duckdb/duckdb-wasm` `1.33.1-dev18.0` (minimum), `1.33.1-dev65.0` (pinned) |

### Updating DuckDB dependencies

All DuckDB versions — `libduckdb`, `@duckdb/node-api`, and
`@duckdb/duckdb-wasm` — are pinned and bumped deliberately; CI never tests
`latest`.

- **Verification path.** A version bump lands as a PR that updates the pin in
  `package.json` (JS) or `ci.yml` (native) *and* adds the new version to the
  CI matrix — the matrix install step is what actually exercises a version.
  The oldest matrix entry stays as the minimum supported version until
  intentionally dropped; the newest entry matches the `package.json` pin.
  The `duckdb-pins` CI job
  (`scripts/check_duckdb_pins.mjs`, runnable locally via
  `pnpm check:dep-pins`) enforces this: it fails when a `package.json` pin
  does not equal the newest matrix entry, so a bump that forgets the matrix
  update cannot look green while the pinned version goes untested.
- **Automated bumps.** [Dependabot](.github/dependabot.yml) opens grouped
  weekly PRs for `@duckdb/*` npm packages and GitHub Actions. A Dependabot PR
  is merged only after the new version is added to the `ci.yml` matrix (the
  `duckdb-pins` job fails otherwise) and the full backend suite is green:
  `moon test --target js` + `pnpm test:wasm-browser` for JS bumps,
  `moon test --target native` for native bumps.
- **Advanced-type re-check.** The browser smoke check exercises direct
  prepared parameters for Decimal, Interval, Blob, List, Struct, and Map. If a
  bump flips any of them between supported and unsupported, update the
  `BackendCapabilities` table in `src/duckdb_capabilities.mbt`, regenerate the
  Advanced Types table with `scripts/support_matrix.sh`, and update the
  expected-unsupported set in `scripts/wasm_browser_smoke.mjs` in the same PR.
- A DuckDB 2.0 lane will be added once a 2.0 libduckdb build is published on
  the [DuckDB releases page](https://github.com/duckdb/duckdb/releases).

## Installation

### Native Target

The native target links against `libduckdb` using the DuckDB C API.

#### Install libduckdb

**macOS (Homebrew):**
```bash
brew install duckdb
```

**Ubuntu/Debian:**
```bash
# Download a release that matches your platform
wget https://github.com/duckdb/duckdb/releases/download/<version>/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip
sudo cp libduckdb.so /usr/local/lib/
sudo ldconfig
```

**From Source:**
```bash
git clone https://github.com/duckdb/duckdb.git
cd duckdb
mkdir build && cd build
cmake ..
make -j$(nproc)
sudo make install
sudo ldconfig
```

#### Linker Configuration

When compiling, you may need to specify the library path:

```bash
moon build --target-native -- -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lduckdb
```

Or set `PKG_CONFIG_PATH` if libduckdb provides a pkg-config file. The default
`src/moon.pkg` includes common include/library search paths for both macOS and
Ubuntu:

- Include: `/opt/homebrew/include`, `/usr/local/include`, `/usr/include`
- Library: `/opt/homebrew/lib`, `/usr/local/lib`, `/usr/lib`

The linker still requires `-lduckdb`, so `libduckdb` must be installed on the
machine.

#### Troubleshooting Native Link Errors

If you hit errors like `Undefined symbols ... _duckdb_*`, check:

1. `duckdb.h` exists in one of the include paths above.
2. `libduckdb.dylib` (macOS) or `libduckdb.so` (Linux) exists in one of the
   library paths above.
3. `moon test --target native` runs in an environment where those paths are
   visible to the linker.

### JavaScript Targets

#### Node.js

The Node.js backend uses `@duckdb/node-api`. Install JavaScript dependencies
with pnpm:

```bash
pnpm install
```

#### Browser (WASM)

The browser backend uses `@duckdb/duckdb-wasm` and requires browser Worker
support. The package is installed by the same pnpm setup:

```bash
pnpm install
```

Run the browser smoke check to verify the local setup:

```bash
pnpm test:wasm-browser
```

If Chromium is not installed for Playwright yet, install it once:

```bash
pnpm test:wasm-browser:install
```

The smoke check serves the local `@duckdb/duckdb-wasm` bundle over HTTP, verifies
`Worker` support, and exercises direct duckdb-wasm prepared parameters for
Decimal, Blob, Interval, List, Struct, and Map. Decimal and Interval are
expected to pass; Blob, List, Struct, and Map are expected to remain unsupported.
Cross-origin isolation may be required for future pthread/SharedArrayBuffer
paths, but the current smoke check uses the MVP worker bundle.

### JavaScript Limitations

- **WASM Appender** - Not supported for WASM backend (use INSERT statements instead)
- **WASM Advanced Types** - Decimal and Interval prepared-statement binds are supported as direct string parameters with explicit SQL casts; Blob, List, Struct, and Map direct prepared parameters are unsupported
- **Node.js Advanced Types** - Decimal, Interval, Blob are supported for bind/append; List/Struct/Map are VARCHAR-only
- **JS Appender Date/Timestamp** - Not implemented (native only)

## Quack Remote Protocol

Quack helpers live in the `f4ah6o/duckdb/quack` package and are thin SQL
wrappers built on the generic extension primitives
(`Connection::install_extension` / `load_extension` / `attach` / `query`).
They use DuckDB's `quack` extension instead of implementing the low-level
`application/duckdb` wire format in MoonBit.

Quack is experimental in DuckDB 1.5.x and is distributed from DuckDB's
`core_nightly` extension repository. Function names, defaults, and protocol
details may change before DuckDB 2.0.

The older `Connection::install_quack` / `load_quack` / `start_quack_server` /
`stop_quack_server` / `quack_query` / `create_quack_secret` / `attach_quack`
methods remain as deprecated facades during the transition — new code should
import the `quack` package instead.

```mbt nocheck
connect(on_ready=fn(result) {
  match result {
    Ok(conn) => {
      @quack.install(conn, on_done=fn(_) { () })
      @quack.load(conn, on_done=fn(_) { () })
      @quack.serve(
        conn,
        "quack:localhost",
        token="super_secret",
        on_done=fn(started) {
          match started {
            Ok(info) => println("quack server: \{info.rows}")
            Err(err) => println("quack serve failed: \{err}")
          }
        },
      )
    }
    Err(err) => println("connect failed: \{err}")
  }
})
```

Client helpers cover scoped secrets, stateless remote queries, and attached
remote catalogs:

```mbt nocheck
@quack.create_secret(
  conn,
  "super_secret",
  scope="quack:localhost",
  on_done=fn(_) { () },
)

@quack.query(
  conn,
  "quack:localhost",
  "SELECT 42 AS answer",
  token="super_secret",
  on_done=fn(result) {
    match result {
      Ok(rows) => println("\{rows.rows}")
      Err(err) => println("quack query failed: \{err}")
    }
  },
)

@quack.attach(
  conn,
  "quack:localhost",
  "remote_db",
  token="super_secret",
  on_done=fn(_) { () },
)
```

For non-local endpoints, DuckDB's Quack client assumes HTTPS by default. Use
`disable_ssl=true` only when a remote endpoint is intentionally served over
plain HTTP, and prefer a TLS-terminating reverse proxy for exposed services.

## Usage

```mbt nocheck
connect(on_ready=fn (result) {
  match result {
    Ok(conn) => {
      conn.query(
        "select 1 as a, NULL as b, 'duck' as c",
        on_done=fn (query_result) {
          match query_result {
            Ok(result) => {
              println("columns: \{result.columns}")
              println("rows: \{result.rows}")
              println("nulls: \{result.nulls}")
            }
            Err(err) => println("query failed: \{err}")
          }
        },
      )
      conn.close(on_done=fn (closed) {
        match closed {
          Ok(_) => ()
          Err(err) => println("close failed: \{err}")
        }
      })
    }
    Err(err) => println("connect failed: \{err}")
  }
})
```

## Typed Results

`QueryResult` stores rows as strings plus a null mask. Use the typed helpers for
convenience, or convert to a `TypedQueryResult` for repeated access:

```mbt nocheck
conn.query("select 1 as a, 2.5 as b, NULL as c", on_done=fn (query_result) {
  match query_result {
    Ok(result) => {
      let value = result.get_int(0, 0) // Some(1)
      let typed = result.to_typed()
      let b0 = typed.get_double(0, 1)
      let c0 = typed.get_string(0, 2) // None
      println("\{value} \{b0} \{c0}")
    }
    Err(err) => println("query failed: \{err}")
  }
})
```

## Streaming Results

Use `query_stream` to process large datasets in chunks without materializing
the full result in MoonBit memory:

### Basic Streaming (Count Rows)

```mbt nocheck
connect(on_ready=fn (result) {
  match result {
    Ok(conn) => {
      conn.query_stream(
        "SELECT i FROM RANGE(1000000) tbl(i)",
        on_done=fn (stream_result) {
          match stream_result {
            Ok(stream) => {
              let mut total = 0
              let done = Ref::new(false)
              while !done.val {
                stream.next(on_done=fn (chunk_result) {
                  match chunk_result {
                    Ok(Some(chunk)) => total = total + chunk.row_count()
                    Ok(None) => done.val = true
                    Err(err) => {
                      done.val = true
                      println("stream failed: \{err}")
                    }
                  }
                })
              }
              stream.close(on_done=fn (_) { () })
              println("rows: \{total}")
            }
            Err(err) => println("stream failed: \{err}")
          }
        },
      )
    }
    Err(err) => println("connect failed: \{err}")
  }
})
```

### Aggregation Example

For more advanced use cases, you can aggregate data while streaming:

```mbt nocheck
// Aggregate state to track running totals
pub struct Aggregates {
  mut total_rows : Int
  mut sum_values : Int
  mut min_value : Int?
  mut max_value : Int?
}

let agg_ref = Ref::new({ total_rows: 0, sum_values: 0, min_value: None, max_value: None })
let done_ref = Ref::new(false)

conn.query_stream(
  "SELECT value FROM measurements",
  on_done=fn (stream_result) {
    match stream_result {
      Ok(stream) => {
        while !done_ref.val {
          stream.next(on_done=fn (chunk_result) {
            match chunk_result {
              Ok(Some(chunk)) => {
                // Process each row in the chunk
                for row = 0; row < chunk.row_count(); row = row + 1 {
                  match chunk.cell(row, 0) {
                    Some(v) => {
                      let value = parse_int(v)
                      agg_ref.val.total_rows = agg_ref.val.total_rows + 1
                      agg_ref.val.sum_values = agg_ref.val.sum_values + value
                      // Update min/max...
                    }
                    None => ()
                  }
                }
              }
              Ok(None) => done_ref.val = true
              Err(err) => { done_ref.val = true; println("error: \{err}") }
            }
          })
        }
        stream.close(on_done=fn (_) {
          println("Total: \{agg_ref.val.total_rows}")
          println("Sum: \{agg_ref.val.sum_values}")
        })
      }
      Err(err) => println("stream failed: \{err}")
    }
  },
)
```

### Streaming Limitations

- Streamed `DataChunk` values are strings plus a null mask, consistent with `QueryResult`.
- Always call `ResultStream::close` when finished to release resources.

## JS Backend Selection

Use `JsBackend::Auto` (default), `JsBackend::Node`, or `JsBackend::Wasm`:

```mbt nocheck
connect(
  on_ready=fn (result) { /* ... */ },
  backend=JsBackend::Wasm,
)
```

- `Auto` - Detects environment (Node.js uses Node, browser uses WASM)
- `Node` - Forces `@duckdb/node-api`
- `Wasm` - Forces `@duckdb/duckdb-wasm`

## Backend Capabilities

Every handle reports which backend it runs on and what that backend supports.
`conn.capabilities()` returns a `BackendCapabilities` struct (the same table
that generates the Feature Support Matrix above); `conn.backend()` reports the
resolved `Backend` (`Native`, `Node`, `Wasm`, or `Unsupported`).

```mbt nocheck
connect(on_ready=fn (result) {
  match result {
    Ok(conn) => {
      let caps = conn.capabilities()
      if caps.appender {
        // safe to call conn.create_appender(...)
      }
      match caps.require(BackendFeature::BlobBind) {
        Ok(_) => () // bind_blob is available
        Err(err) => println("gated: \{err.message()}")
      }
    }
    Err(err) => println("connect failed: \{err}")
  }
})
```

Operations that a backend cannot provide fail with the structured
`DuckDBError::Unsupported(feature~, backend~)` error, so callers can match on
`feature`/`backend` instead of parsing message text. Capability-gated checks
run before the FFI layer, so e.g. `create_appender` on a WASM connection
fails immediately with `Unsupported(feature="appender", backend="wasm")`.

## Configuration

Configuration is only available on native/JS targets (not wasm/wasm-gc).
Create a config, set options, then connect with it:

```mbt nocheck
let config = Config::create()
match config.set("memory_limit", "1GB") {
  Ok(_) => ()
  Err(err) => println("config set failed: \{err}")
}

connect_with_config(
  on_ready=fn (result) { /* ... */ },
  config=Some(config),
  path=":memory:",
)
```

## Error Handling

All `bind_*` methods and `Config::set` return `Result[Unit, DuckDBError]` on both native and JS targets:
- **Success**: Returns `Ok(())`
- **Failure**: Returns `Err(DuckDBError::Message(reason))`

On JS targets, bind operations are synchronous and errors are properly propagated. Use pattern matching to handle errors:

```mbt nocheck
match stmt.bind_int(1, 42) {
  Ok(_) =>
    match stmt.bind_varchar(2, "hello") {
      Ok(_) => ()
      Err(e) => println("bind failed: \{e}")
    }
  Err(e) => println("bind failed: \{e}")
}
```

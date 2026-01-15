# f4ah6o/duckdb

MoonBit bindings for DuckDB on native and JavaScript targets.

## Feature Support Matrix

| Feature | Native | Node.js | Browser/WASM |
|---------|--------|--------|--------------|
| Connection & Query | ✅ | ✅ | ✅ |
| Prepared Statements | ✅ | ✅ | ✅ |
| Streaming Results | ⚠️ | ✅ | ✅ |
| Appender | ✅ | ✅ | ❌ |
| Arrow Integration | ⚠️ | ⚠️ | ⚠️ |
| Advanced Types | ⚠️ | ⚠️ | ❌ |

**Legend:** ✅ Full support | ⚠️ Partial support | ❌ Not supported

### Advanced Types Detailed Support

| Type | Native Bind | Native Append | Node Bind | Node Append | WASM |
|------|-------------|---------------|-----------|-------------|------|
| Decimal | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ✅ 128-bit | ❌ |
| Interval | ✅ | ✅ | ✅ | ✅ | ❌ |
| Blob | ✅ | ✅ | ✅ | ✅ | ❌ |
| List | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ❌ | ❌ |
| Struct | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ❌ | ❌ |
| Map | ✅ VARCHAR | ✅ VARCHAR | ✅ VARCHAR | ❌ | ❌ |

**Notes:**
- Native appender supports all advanced types (VARCHAR-only for List/Struct/Map)
- Node.js backend supports bind/append for Decimal, Interval, Blob
- List/Struct/Map are VARCHAR-only (serialized as JSON strings)
- WASM backend does not support advanced types - use INSERT statements with type literals instead

### Arrow Integration

Basic support is available on all targets:
- Arrow query result type
- Schema extraction
- Column-based data access with nullable support
- Supported types: BOOLEAN, INTEGER, VARCHAR, DOUBLE, BIGINT

**Note:** Complex types (List, Struct, Map) are not yet supported.

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
# Download from GitHub releases
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/libduckdb-linux-amd64.zip
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

Or set `PKG_CONFIG_PATH` if libduckdb provides a pkg-config file.

### JavaScript Targets

#### Node.js

The Node.js backend uses `@duckdb/node-api`. Install dependencies:

```bash
npm install @duckdb/node-api@^1.4.3-r.3
```

#### Browser (WASM)

The browser backend uses `@duckdb/duckdb-wasm`. Install dependencies:

```bash
npm install @duckdb/duckdb-wasm@^1.33.1-dev18.0
```

**Note:** WASM requires browser Worker support. Cross-origin isolation may be required for optimal performance.

### JavaScript Limitations

- **WASM Appender** - Not supported for WASM backend (use INSERT statements, insertCSVFromPath(), insertJSONFromPath(), or insertArrowTable() instead)
- **WASM Advanced Types** - Blob, Decimal, Interval, List, Struct, Map are not supported for WASM backend (use INSERT statements with type literals instead)
- **Node.js Advanced Types** - Decimal (128-bit), Interval, Blob are supported for bind/append; List/Struct/Map are VARCHAR-only
- **Date/Time types** - Limited support in prepared statements

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

- **Native**: Supports scalar types (numeric/bool/string/date/time/uuid/interval).
  Complex types (list/struct/map/union) are NOT supported and return an error at stream creation.
- **JS (Node.js & WASM)**: Full support via Arrow batches with no type limitations.
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

## Configuration

Pass a `DuckDBConfig` to configure the connection:

```mbt nocheck
let config = DuckDBConfig::{
  memory_limit: "1GB",
  threads: "4",
  max_memory: "2GB",
}

connect_with_config(
  on_ready=fn (result) { /* ... */ },
  config=config,
  path=":memory:",
)
```

## Error Handling

All `bind_*` methods and `Config::set` return `Result[Unit, DuckDBError]` on both native and JS targets:
- **Success**: Returns `Ok(())`
- **Failure**: Returns `Err(DuckDBError::Message(reason))`

On JS targets, bind operations are synchronous and errors are properly propagated. Use the `?` operator or pattern matching to handle errors:

```mbt nocheck
match stmt.bind_int(1, 42) {
  Ok(_) => stmt.bind_varchar(2, "hello")?  // Chain binds with ?
  Err(e) => println("bind failed")
}
```

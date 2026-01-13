# f4ah6o/duckdb

MoonBit bindings for DuckDB on native and JavaScript targets.

## Targets

- native: links against `libduckdb` and uses the DuckDB C API.
- js (Node): uses `@duckdb/node-api`.
- js (browser): uses `@duckdb/duckdb-wasm`.

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

## JS backend selection

Use `JsBackend::Auto` (default), `JsBackend::Node`, or `JsBackend::Wasm`:

```mbt nocheck
connect(
  on_ready=fn (result) { /* ... */ },
  backend=JsBackend::Wasm,
)
```

MoonBit bindings for DuckDB on native and JavaScript targets.

## Targets

- native: links against `libduckdb` and uses the DuckDB C API.
- js (Node): uses `@duckdb/node-api`.
- js (browser): uses `@duckdb/duckdb-wasm`.

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

## JS backend selection

Use `JsBackend::Auto` (default), `JsBackend::Node`, or `JsBackend::Wasm`:

```mbt nocheck
connect(
  on_ready=fn (result) { /* ... */ },
  backend=JsBackend::Wasm,
)
```

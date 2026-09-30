# Migration Guide: 0.6.4 → 0.7.0

`f4ah6o/duckdb` 0.7.0 ships the breaking interface changes that landed on
`main` after the Mooncakes 0.6.4 release: a 128-bit `Decimal`, structured
`DuckDBError` variants, the Arrow result rework, the move of the Quack
helpers into the `f4ah6o/duckdb/quack` package, the typed-vector /
capabilities additions, and the canonical-vector JS facade that makes
`query`/`execute`/`ResultStream::next` render results identically to the
native backend.

This guide was produced by diffing the published `f4ah6o/duckdb@0.6.4`
`pkg.generated.mbti` files (from Mooncakes) against the 0.7.0 interfaces, so
every symbol-level change listed below is verified against the actual
published package.

## At a glance

| 0.6.4 | 0.7.0 | Severity |
|-------|-------|----------|
| `DuckDBError::Message(String)` | structured variants + `err.message()` | **breaking** (deprecated shims available) |
| `decimal_from_parts(Int, Int, Int)` | `decimal_from_parts(Int64, Int64, Int)` | **breaking** |
| `decimal_to_parts(_) -> (Int, Int)` | `-> (@bigint.BigInt, @bigint.BigInt)` | **breaking** |
| `Decimal { lower : Int, upper : Int }` | `lower : UInt64, upper : Int64` | **breaking** |
| `conn.install_quack(...)` etc. | `@quack.install(conn, ...)` etc. | deprecated facade (still compiles) |
| `ArrowResult::get_column_int64 -> Array[Int]` | `-> Array[Int64]` | **breaking** (function is also deprecated) |
| `ArrowResult::get_column_*` | `ArrowResult::to_chunks` + `Vector` accessors | deprecated facade (still compiles) |
| JS `rows`/`column_types` payloads | DuckDB-style nested rendering, `5.0` integral doubles, real WASM `column_types`, `columns=[]` on zero chunks | **behavior change** (JS only) |
| `pub enum Value` (9 variants) | `pub(all) enum Value` (17 variants) | **breaking** for exhaustive `match` |
| `array_of`, `assert_check`, `check_with_stats`, `shrink_int`, `CheckConfig`, `CheckResult` | removed from the public interface | **breaking** |
| `pub type LogicalType`, `pub type Vector` | `NativeLogicalType`, `NativeVector` (native FFI); `Vector` is now the public typed-vector struct | **breaking** if referenced |

## Errors: `DuckDBError::Message` is gone

`DuckDBError` is now a structured `suberror` enum instead of a single
`Message(String)` variant:

```mbt nocheck
// 0.6.4
match result {
  Ok(_) => ()
  Err(DuckDBError::Message(msg)) => println("failed: \{msg}")
}
```

```mbt nocheck
// 0.7.0 — read the diagnostic text via message()
match result {
  Ok(_) => ()
  Err(err) => println("failed: \{err.message()}")
}
```

To branch on the error *category*, match the structured variants instead of
parsing text:

```mbt nocheck
// 0.7.0
match result {
  Ok(_) => ()
  Err(DuckDBError::Closed(resource~)) => println("\{resource} was closed")
  Err(DuckDBError::Unsupported(feature~, backend~, ..)) =>
    println("\{backend} does not support \{feature}")
  Err(DuckDBError::DuckDB(error_type~, message~)) =>
    println("engine error (\{error_type}): \{message}")
  Err(err) => println("failed: \{err.message()}")
}
```

The full variant set:

- `Query(String)` / `Prepare(String)` / `Bind(String)` / `Append(String)` —
  the binding detected a failure at that stage with no engine classification.
- `DuckDB(error_type~, message~)` — an engine-reported error; `error_type` is
  the engine's own classification normalized to snake_case (`"parser"`,
  `"catalog"`, `"invalid_input"`, ... or `"unknown"`).
- `Backend(backend~, message~)` — a backend/runtime failure outside the query
  path (connect, close, configuration, host exceptions).
- `Unsupported(feature~, backend~, message~)` — the feature exists but this
  backend cannot provide it.
- `Closed(resource~)` — the operation was attempted on a closed handle.
- `InvalidArgument(argument~, reason~)` — a caller-supplied argument failed
  validation.

New accessors:

- `err.message() -> String` — the diagnostic text (engine/backend text
  verbatim, or synthesized from the structured fields).
- `err.error_type() -> String?` — the engine classification, only for the
  `DuckDB` variant.

Compatibility shims (deprecated — they keep 0.6.4 code compiling while you
migrate):

- `err.to_message()` — same as `err.message()`.
- `duckdb_message_error(msg)` — constructs `Backend(backend="generic")` where
  `DuckDBError::Message(msg)` used to be constructed.

```mbt nocheck
// Still compiles on 0.7.0 (deprecated):
let msg = err.to_message()
let e = duckdb_message_error("boom")
```

## Decimal: full 128-bit payload

`Decimal` now carries the full DuckDB 128-bit scaled integer instead of two
32-bit `Int` halves:

```mbt nocheck
// 0.6.4
pub struct Decimal {
  width : Int
  scale : Int
  lower : Int   // low bits
  upper : Int   // high bits
}

// 0.7.0
pub struct Decimal {
  width : Int
  scale : Int
  lower : UInt64  // low 64 bits (unsigned)
  upper : Int64   // high 64 bits (signed, two's complement)
}
```

`decimal_from_parts` takes `Int64` for both parts; either part being negative
makes the result negative (a negative `fractional` alone encodes sub-unit
negatives like `-0.50`):

```mbt nocheck
// 0.6.4
let d = decimal_from_parts(123, 45, 2) // 123.45

// 0.7.0 — Int64 literals
let d = decimal_from_parts(123L, 45L, 2) // 123.45
let neg = decimal_from_parts(0L, -50L, 2) // -0.50
```

`decimal_to_parts` returns `BigInt` values so whole parts beyond 64 bits are
preserved:

```mbt nocheck
// 0.6.4
let (whole, frac) = decimal_to_parts(d) // (Int, Int)

// 0.7.0 — (@bigint.BigInt, @bigint.BigInt); import "moonbitlang/core/bigint"
let (whole, frac) = decimal_to_parts(d)
println(whole.to_string())
```

New constructor for raw halves matching `duckdb_hugeint` exactly:

```mbt nocheck
// 0.7.0 — decimal_from_hugeint(lower : UInt64, upper : Int64, width, scale)
let d = decimal_from_hugeint(lower, upper, 38, 2)
```

## Arrow results: typed vectors

`query_arrow` now eagerly materializes results into canonical `VectorChunk`s
on every backend — no JSON or packed-byte re-encoding. `ArrowResult` changed
from `#external` to a struct with private fields, but it is still an opaque
handle for callers: `close` releases it, and `to_chunks`/`get_schema` report
`DuckDBError::Closed` afterwards.

The legacy per-column getters are deprecated forwarders. One of them is a
hard signature break:

- `ArrowResult::get_column_int64` now returns `Array[Int64]` instead of
  `Array[Int]` — the old return type truncated BIGINT to 32 bits.
- `get_column_int32` / `get_column_double` / `get_column_bool` /
  `get_column_string` keep their signatures but are deprecated.
- `get_column_*_nullable` variants return `(Array[T], Array[Bool])`
  (values + validity mask); also deprecated.

```mbt nocheck
// 0.6.4
conn.query_arrow("SELECT i::BIGINT AS i FROM RANGE(3) tbl(i)", on_done=fn(r) {
  match r {
    Ok(arrow) => {
      let ints = arrow.get_column_int64(0) // Array[Int] — truncated!
      println("\{ints}")
      arrow.close(on_done=fn(_) { () })
    }
    Err(err) => println("\{err}")
  }
})
```

```mbt nocheck
// 0.7.0 — materialize typed vectors, read them with Vector accessors
conn.query_arrow("SELECT i::BIGINT AS i FROM RANGE(3) tbl(i)", on_done=fn(r) {
  match r {
    Ok(arrow) => {
      match arrow.to_chunks() {
        Ok(chunks) =>
          for chunk in chunks {
            match chunk.column_by_name("i") {
              Some(vector) =>
                match vector.int64s() { // FixedArray[Int64]?
                  Some(values) => println("\{values}")
                  None => ()
                }
              None => ()
            }
          } nobreak {
            ()
          }
        Err(err) => println("\{err.message()}")
      }
      arrow.close(on_done=fn(_) { () })
    }
    Err(err) => println("\{err.message()}")
  }
})
```

Use `vector.is_null(row)` (or the `vector.validity` mask) for nullability, and
`vector.value_at(row)` for a generic `Value`. Non-deprecated getters include
`Vector::ints` / `int64s` / `uint64s` / `doubles` / `bools` / `strings` /
`blobs` / `decimals` / `intervals` (each returns `None` on a physical-type
mismatch), `list_parts` / `struct_parts` / `map_parts` for nested columns,
`any_cells`, `len`, `string_at`, and `value_at`.

## JS facades: canonical vectors and DuckDB-style strings

On the JS backends (Node via `@duckdb/node-api`, browser via
`duckdb-wasm` Arrow), `conn.query`, `PreparedStatement::execute`, and
`ResultStream::next` are now thin facades over the typed-vector path
(`query_chunks`/`execute_chunks`/`next_chunk` materialization plus the
canonical renderers shared with the native backend). Signatures are
unchanged, but several observable behaviors change — audit code that
string-compares cell output or inspects `column_types`:

- **Nested cells render DuckDB-style** instead of `JSON.stringify` output:
  lists `[1, 2, 3]` (spaced, `NULL` for null elements), structs
  `{a: 1, b: x}`, maps `{k=v}` — previously `[1,2,3]` and `{"a":1}`-style
  JSON blobs.
- **Integral `DOUBLE` renders with a decimal point**: `5.0` (was `5`).
  `nan`/`inf`/`-inf`, booleans, and integer columns are unchanged.
- **`QueryResult.column_types` on WASM** reports the real `ColumnType` per
  column (previously every column was `Unknown(-1)` — the WASM path
  carried no type IDs).
- **Zero-chunk results**: a query that produces no result chunks returns
  `columns=[]`, matching the native path (previously JS populated
  `columns` from Arrow schema / `columnNames()` metadata).

```mbt nocheck
// conn.query("SELECT [1,2] AS l, {'a': 1} AS s, 5.0::DOUBLE AS d", ...)

// 0.6.4 JS (Node + WASM)
// result.rows[0] == ["[1,2]", "{\"a\":1}", "5"]
// WASM:  result.column_types == [Unknown(-1), Unknown(-1), Unknown(-1)]
// Node:  result.column_types == [List, Struct, Double]  (already real)

// 0.7.0 JS — same query, both backends
// result.rows[0] == ["[1, 2]", "{a: 1}", "5.0"]
// result.column_types == [List, Struct, Double]
```

The typed-chunk APIs benefit too: `query_chunks`, `execute_chunks`,
`next_chunk`, and `ArrowResult::to_chunks` now produce real
`VectorData::List`/`Struct`/`Map` on JS, so `vector.list_parts()`,
`vector.struct_parts()`, `vector.map_parts()`, and nested
`vector.value_at(row)` work on JS the same way they do on native.
Previously nested columns fell into a tagged-JSON `Any` fallback per cell
(it remains only for UNION/BIT/TIME_TZ/BIGNUM). `conn.execute` and
`stream.columns`/`stream.column_types` are unchanged.

## Quack: moved to `f4ah6o/duckdb/quack`

The Quack convenience methods on `Connection` are deprecated forwarding
facades. The canonical API now lives in the `quack` sub-package — add
`"f4ah6o/duckdb/quack"` to your `moon.pkg` imports and call the functions
with the connection as the first argument:

| 0.6.4 (deprecated on 0.7.0) | 0.7.0 |
|------------------------------|-------|
| `conn.install_quack(on_done=...)` | `@quack.install(conn, on_done=...)` |
| `conn.load_quack(on_done=...)` | `@quack.load(conn, on_done=...)` |
| `conn.start_quack_server(uri, token?=, allow_other_hostname?=, on_done=...)` | `@quack.serve(conn, uri, token?=, allow_other_hostname?=, on_done=...)` |
| `conn.stop_quack_server(uri, on_done=...)` | `@quack.stop(conn, uri, on_done=...)` |
| `conn.quack_query(uri, sql, token?=, disable_ssl?=, on_done=...)` | `@quack.query(conn, uri, sql, token?=, disable_ssl?=, on_done=...)` |
| `conn.create_quack_secret(token, scope?=, on_done=...)` | `@quack.create_secret(conn, token, scope?=, on_done=...)` |
| `conn.attach_quack(uri, alias, token?=, disable_ssl?=, on_done=...)` | `@quack.attach(conn, uri, alias, token?=, disable_ssl?=, on_done=...)` |

```mbt nocheck
// 0.6.4
conn.install_quack(on_done=fn(_) { () })
conn.quack_query("quack:localhost", "SELECT 42", on_done=fn(r) { ... })

// 0.7.0 — moon.pkg: import { "f4ah6o/duckdb" @lib, "f4ah6o/duckdb/quack" @quack }
@quack.install(conn, on_done=fn(_) { () })
@quack.query(conn, "quack:localhost", "SELECT 42", on_done=fn(r) { ... })
```

The old method names still compile (they forward and warn), so this is a soft
break. The `quack` package functions additionally check
`conn.capabilities()` first and return `DuckDBError::Unsupported` on backends
without Quack support (e.g. browser WASM) instead of issuing the SQL.

You can also compose the new generic extension primitives directly:
`conn.install_extension(name, repository?=, force?=)`,
`conn.load_extension(name)`, and `conn.attach(uri, db_alias?=, options?=)`.

## `Value` and `ColumnType`: new variants (exhaustive `match` breaks)

`Value` gained variants for the wider type surface now materialized by the
vector paths:

```mbt nocheck
// 0.7.0 — `pub(all) enum Value` (was `pub enum` at 0.6.4)
pub enum Value {
  Int(Int)
  Int64(Int64)          // NEW — BIGINT payloads from vectors
  UInt64(UInt64)        // NEW — UBIGINT payloads
  Double(Double)
  Bool(Bool)
  String(String)
  Date(Int)
  Timestamp(Int64)
  TimestampNs(Int64)    // NEW — TIMESTAMP_NS payloads
  Decimal(Decimal)
  Interval(Interval)    // NEW
  HugeInt(lower~ : UInt64, upper~ : Int64) // NEW — 128-bit ints, UUID
  Blob(Bytes)
  List(Array[Value])    // NEW
  Struct(fields~ : Array[String], values~ : Array[Value]) // NEW
  Map(keys~ : Array[Value], values~ : Array[Value])       // NEW
  Null
}
```

Action needed:

- **Exhaustive `match` on `Value` no longer compiles.** Add arms for the new
  variants or a wildcard `_`.
- `Value` and `ColumnType` are now `pub(all)`, so you can construct them in
  your own code (e.g. build a `VectorChunk` for `Appender::append_chunk`).
- `ColumnType` now derives `Eq` and has `equal`/`not_equal`; a new
  `column_type_to_id(ColumnType) -> Int` complements `column_type_from_id`.
- New accessor `Value::as_timestamp_ns`. Note that `as_int` /
  `TypedQueryResult::get_int` only unwrap `Value::Int` — values arriving as
  `Int64`/`UInt64` need their own match arms.

## Removed: PBT helper leaks

These QuickCheck convenience helpers were accidentally public at 0.6.4 and are
no longer part of the interface (the underlying test-only code remains inside
the package):

- `array_of`, `assert_check`, `check_with_stats`, `shrink_int`
- `CheckConfig` (`default`/`new`/`seed`), `CheckResult` (`passed`/`stats`)

Migration: depend on `moonbitlang/quickcheck` directly (`@gen.sized` +
`@gen.Gen::array_with_size` for `array_of`, `@pbt.forall` /
`@pbt.forall_shrink` + `@pbt.quick_check` for `assert_check` /
`check_with_stats`, `@pbt.classify` / `@pbt.counterexample` for stats) — the
removed helpers were thin wrappers around these.

## Opaque type cleanups

- `pub type LogicalType` (external) → renamed `NativeLogicalType`;
  `NativeVector` added for the native FFI vector handle. Both are
  native-target externals — only relevant if you referenced them by name.
- `pub type Vector` was an `#external` handle at 0.6.4; at 0.7.0 `Vector` is
  the public typed-column struct described above.
- `Appender`, `Connection`, `PreparedStatement`, `ResultStream`, `Config`,
  `ArrowResult` lost their `#external` marker; they remain opaque `pub type`s
  (or a private-field struct for `ArrowResult`). No caller action needed.

## New in 0.7.0 (additive)

- **Runtime capabilities**: `conn.backend()` / `conn.capabilities()` (also on
  `PreparedStatement` and `Appender`), the `Backend` enum
  (`Native`/`Node`/`Wasm`/`Unsupported`), the `BackendCapabilities` struct,
  the `BackendFeature` enum, `caps.supports(feature)`,
  `caps.require(feature)`, `caps.unsupported(feature)` /
  `unsupported_error(feature)`, and `support_matrix_markdown()` which emits
  the same table as the README feature matrix.
- **Typed-vector results**: `conn.query_chunks`,
  `stmt.execute_chunks`, `stream.next_chunk`, `ArrowResult::to_chunks`, and
  the `VectorChunk` / `Vector` / `VectorData` model.
- **Vectorized appender**: `appender.append_chunk(chunk)` ingests a
  `VectorChunk` through DuckDB's `DataChunk` API (native + Node).
- **Conversion helpers**: `chunks_to_query_result(chunks)`,
  `VectorChunk::to_query_result` / `to_data_chunk` / `to_typed`,
  `VectorChunk::new(columns, vectors)` for building chunks by hand.
- **Generic extension primitives**: `conn.install_extension`,
  `conn.load_extension`, `conn.attach`.

## Deprecated but still working (0.7.0 facades)

| Deprecated symbol | Replacement |
|-------------------|-------------|
| `DuckDBError::to_message` | `err.message()` |
| `duckdb_message_error` | construct the matching `DuckDBError` variant |
| `Connection::install_quack` / `load_quack` | `f4ah6o/duckdb/quack`: `install` / `load` |
| `Connection::start_quack_server` / `stop_quack_server` | `quack::serve` / `quack::stop` |
| `Connection::quack_query` | `quack::query` |
| `Connection::create_quack_secret` | `quack::create_secret` |
| `Connection::attach_quack` | `quack::attach` (or `conn.attach` with a `TYPE quack` options clause) |
| `ArrowResult::get_column_int32` / `int64` / `double` / `bool` / `string` | `ArrowResult::to_chunks` + `Vector` accessors |
| `ArrowResult::get_column_*_nullable` | `ArrowResult::to_chunks` + `Vector::is_null` / `validity` |

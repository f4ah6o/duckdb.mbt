///|
// Whitebox tests for `js_pack_chunk` (issue #68): verify the pool layout the
// `js_appender_append_chunk_ffi` boundary consumes — kind tags, per-kind
// offsets, packed payloads, and the row*ncols+col null grid.

///|
test "js_pack_chunk pools flat columns" {
  let chunk : VectorChunk = {
    columns: ["b", "i", "s", "u"],
    vectors: [
      Vector::all_valid(
        ColumnType::Boolean,
        VectorData::Bool(FixedArray::from_array([true, false])),
      ),
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array([10, 20])),
      ),
      Vector::all_valid(ColumnType::Varchar, VectorData::Varchar(["x", "y"])),
      Vector::{
        logical_type: ColumnType::BigInt,
        data: VectorData::Int64(FixedArray::from_array([1L, 2L])),
        validity: FixedArray::from_array([true, false]),
      },
    ],
  }
  let (
    kinds,
    offsets,
    null_flags,
    pool_i32,
    pool_i64,
    pool_f64,
    pool_str,
    pool_bytes,
  ) = js_pack_chunk(chunk, 0, 2)
  inspect(Array::from_fixed_array(kinds), content="[0, 1, 6, 2]")
  inspect(Array::from_fixed_array(offsets), content="[0, 2, 0, 0]")
  inspect(
    Array::from_fixed_array(null_flags),
    content="[0, 0, 0, 0, 0, 0, 0, 1]",
  )
  inspect(Array::from_fixed_array(pool_i32), content="[1, 0, 10, 20]")
  inspect(Array::from_fixed_array(pool_i64), content="[1, 2]")
  inspect(Array::from_fixed_array(pool_f64), content="[]")
  inspect(Array::from_fixed_array(pool_str), content="[x, y]")
  inspect(pool_bytes.length(), content="0")
}

///|
test "js_pack_chunk packs 128-bit and structured payload columns" {
  let decimals = FixedArray::from_array([
    decimal_from_hugeint(10UL, -1L, 18, 2), // negative 128-bit payload
    decimal_from_hugeint(5UL, 0L, 18, 2),
  ])
  let intervals = FixedArray::from_array([
    interval_from_parts(3, 4, 500L),
    interval_from_parts(0, 1, 0L),
  ])
  let chunk : VectorChunk = {
    columns: ["d", "iv", "h"],
    vectors: [
      Vector::all_valid(ColumnType::Decimal, VectorData::Decimal(decimals)),
      Vector::all_valid(ColumnType::Interval, VectorData::Interval(intervals)),
      Vector::all_valid(
        ColumnType::HugeInt,
        VectorData::HugeInt(
          lower=FixedArray::from_array([9UL, 8UL]),
          upper=FixedArray::from_array([7L, 6L]),
        ),
      ),
    ],
  }
  let (
    kinds,
    offsets,
    _null_flags,
    _pool_i32,
    pool_i64,
    _pool_f64,
    _pool_str,
    _pool_bytes,
  ) = js_pack_chunk(chunk, 0, 2)
  inspect(Array::from_fixed_array(kinds), content="[9, 10, 8]")
  // Decimal pairs, interval triples, then hugeint pairs share pool_i64.
  inspect(Array::from_fixed_array(offsets), content="[0, 4, 10]")
  inspect(
    Array::from_fixed_array(pool_i64),
    content="[-1, 10, 0, 5, 3, 4, 500, 0, 1, 0, 7, 9, 6, 8]",
  )
}

///|
test "js_pack_chunk packs a row slice at an offset" {
  // append_chunk slices inputs larger than the data-chunk capacity; each
  // slice packs only rows [offset, offset + rows) of the source vectors.
  let chunk : VectorChunk = {
    columns: ["i", "s"],
    vectors: [
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array([10, 20, 30, 40])),
      ),
      Vector::{
        logical_type: ColumnType::Varchar,
        data: VectorData::Varchar(["a", "b", "c", "d"]),
        validity: FixedArray::from_array([true, true, false, true]),
      },
    ],
  }
  let (
    kinds,
    offsets,
    null_flags,
    pool_i32,
    _pool_i64,
    _pool_f64,
    pool_str,
    _pool_bytes,
  ) = js_pack_chunk(chunk, 2, 2)
  inspect(Array::from_fixed_array(kinds), content="[1, 6]")
  inspect(Array::from_fixed_array(offsets), content="[0, 0]")
  inspect(Array::from_fixed_array(null_flags), content="[0, 1, 0, 0]")
  inspect(Array::from_fixed_array(pool_i32), content="[30, 40]")
  inspect(Array::from_fixed_array(pool_str), content="[c, d]")
}

///|
// JS tests for `Appender::append_chunk` (issue #68).
//
// The public `connect`/`create_appender` path is promise-based, so it cannot
// run synchronously inside `moon test`. These tests spawn a node subprocess
// exercising the exact `DuckDBDataChunk.create` + `setColumns` +
// `DuckDBAppender.appendDataChunk` contract the js implementation drives,
// covering typed values and NULLs end to end.

///|
extern "js" fn js_run_append_chunk_test(
  setup_sql : String,
  table : String,
) -> String =
  #|(setup_sql, table) => {
  #|  const { execFileSync } = require("child_process");
  #|  const script = [
  #|    "const setupSql = " + JSON.stringify(setup_sql) + ";",
  #|    "const table = " + JSON.stringify(table) + ";",
  #|    "const toError = (err) => err && err.message ? err.message : String(err);",
  #|    "const run = async () => {",
  #|    "  const api = await import('@duckdb/node-api');",
  #|    "  const instance = await api.DuckDBInstance.create(':memory:');",
  #|    "  const connection = await instance.connect();",
  #|    "  await connection.run(setupSql);",
  #|    "  const appender = await connection.createAppender(table);",
  #|    // Mirror js_appender_append_chunk_ffi: column types + pooled values.
  #|    "  const chunk = api.DuckDBDataChunk.create([",
  #|    "    new api.DuckDBIntegerType(),",
  #|    "    new api.DuckDBVarCharType(),",
  #|    "    new api.DuckDBBooleanType(),",
  #|    "    new api.DuckDBBigIntType(),",
  #|    "    new api.DuckDBDoubleType(),",
  #|    "    new api.DuckDBDateType(),",
  #|    "    new api.DuckDBTimestampType(),",
  #|    "    new api.DuckDBBlobType(),",
  #|    "    new api.DuckDBDecimalType(10, 2),",
  #|    "    new api.DuckDBIntervalType(),",
  #|    "    new api.DuckDBHugeIntType(),",
  #|    "    new api.DuckDBUUIDType(),",
  #|    "    new api.DuckDBTimestampSecondsType(),",
  #|    "    new api.DuckDBTimestampMillisecondsType(),",
  #|    "  ]);",
  #|    "  chunk.setColumns([",
  #|    "    [1, 2, 3],",
  #|    "    ['one', null, 'three'],",
  #|    "    [true, false, true],",
  #|    "    [10n, 20n, 30n],",
  #|    "    [0.5, 1.5, 2.5],",
  #|    "    [new api.DuckDBDateValue(20000), new api.DuckDBDateValue(19000), new api.DuckDBDateValue(18000)],",
  #|    "    [new api.DuckDBTimestampValue(1000000n), new api.DuckDBTimestampValue(2000000n), new api.DuckDBTimestampValue(3000000n)],",
  #|    "    [new api.DuckDBBlobValue(new Uint8Array([65,66])), null, new api.DuckDBBlobValue(new Uint8Array([67]))],",
  #|    "    [new api.DuckDBDecimalValue(12345n, 10, 2), new api.DuckDBDecimalValue(100n, 10, 2), new api.DuckDBDecimalValue(0n, 10, 2)],",
  #|    "    [new api.DuckDBIntervalValue(1, 2, 3000000n), new api.DuckDBIntervalValue(0, 0, 0n), new api.DuckDBIntervalValue(6, 0, 0n)],",
  #|    "    [5n, 6n, 7n],",
  #|    "    [api.DuckDBUUIDValue.fromUint128(1n << 96n | 0x550e8400e29b41d4n << 32n | 0xa716446655440000n),",
  #|    "     api.DuckDBUUIDValue.fromUint128(0n),",
  #|    "     api.DuckDBUUIDValue.fromUint128((1n << 128n) - 1n)],",
  #|    // Canonical Int64 payloads hold microseconds; the impl divides back
  #|    // to each variant's native unit before constructing the value.
  #|    "    [new api.DuckDBTimestampSecondsValue(1000000n / 1000000n),",
  #|    "     new api.DuckDBTimestampSecondsValue(3600000000n / 1000000n),",
  #|    "     new api.DuckDBTimestampSecondsValue(2000000n / 1000000n)],",
  #|    "    [new api.DuckDBTimestampMillisecondsValue(1500000n / 1000n),",
  #|    "     new api.DuckDBTimestampMillisecondsValue(3600123000n / 1000n),",
  #|    "     new api.DuckDBTimestampMillisecondsValue(2000000n / 1000n)],",
  #|    "  ]);",
  #|    "  appender.appendDataChunk(chunk);",
  #|    "  appender.flushSync();",
  #|    "  appender.closeSync();",
  #|    "  const result = await connection.run(",
  #|    "    'SELECT i, v, b, g, f, d::VARCHAR, ts::VARCHAR, blob::VARCHAR, dec::VARCHAR, iv::VARCHAR, h::VARCHAR, u::VARCHAR, tss::VARCHAR, tsms::VARCHAR FROM ' + table + ' ORDER BY i');",
  #|    "  const columns = result.columnNames();",
  #|    "  const rowsJson = await result.getRowsJson();",
  #|    "  const rows = []; const nulls = [];",
  #|    "  for (const row of rowsJson) {",
  #|    "    const vals = Array.isArray(row) ? row : columns.map((n) => row[n]);",
  #|    "    const cells = []; const rowNulls = [];",
  #|    "    for (const v of vals) {",
  #|    "      cells.push(v === null || v === undefined ? '' : String(v));",
  #|    "      rowNulls.push(v === null || v === undefined);",
  #|    "    }",
  #|    "    rows.push(cells); nulls.push(rowNulls);",
  #|    "  }",
  #|    "  if (connection.closeSync) connection.closeSync();",
  #|    "  return { columns, rows, nulls };",
  #|    "};",
  #|    "run()",
  #|    "  .then((r) => console.log(JSON.stringify({ ok: true, columns: r.columns, rows: r.rows, nulls: r.nulls })))",
  #|    "  .catch((err) => console.log(JSON.stringify({ ok: false, error: toError(err) })));",
  #|  ].join("\n");
  #|  try {
  #|    const output = execFileSync(process.execPath, ["-e", script], {
  #|      encoding: "utf8",
  #|      stdio: ["ignore", "pipe", "pipe"],
  #|    });
  #|    const lines = output.split(/\r?\n/).filter((line) => line.trim().length > 0);
  #|    return lines.length ? lines[lines.length - 1] : "";
  #|  } catch (err) {
  #|    const message = err && err.stderr
  #|      ? err.stderr.toString()
  #|      : (err && err.message ? err.message : String(err));
  #|    return JSON.stringify({ ok: false, error: message });
  #|  }
  #|}

///|
// End-to-end coverage through the public `Appender::append_chunk` wrapper:
// a fake "node" appender object drives the real capability check, spec
// validation, pooling, FFI boundary, and Result decoding. The fake
// `appendDataChunk` records rendered column values so the test can assert
// what the MoonBit side actually produced — including the canonical-micros
// conversion for TIMESTAMP_S/TIMESTAMP_MS and the >2048-row slicing.

///|
extern "js" fn js_make_fake_node_appender() -> Appender =
  #|() => {
  #|  const repr = (v) => {
  #|    if (v === null || v === undefined) return "";
  #|    if (v && v.__tag) return v.__tag + ":" + String(v.value);
  #|    return String(v);
  #|  };
  #|  const fakeAppender = {
  #|    appended: [],
  #|    appendDataChunk(chunk) {
  #|      this.appended.push(chunk.columns.map((col) => col.map(repr)));
  #|    },
  #|  };
  #|  const T = () => class {};
  #|  const api = {
  #|    DuckDBDataChunk: {
  #|      create: (types) => ({
  #|        types,
  #|        columns: null,
  #|        setColumns(c) {
  #|          this.columns = c;
  #|        },
  #|      }),
  #|    },
  #|    DuckDBBooleanType: T(),
  #|    DuckDBIntegerType: T(),
  #|    DuckDBBigIntType: T(),
  #|    DuckDBVarCharType: T(),
  #|    DuckDBTimestampSecondsType: T(),
  #|    DuckDBTimestampMillisecondsType: T(),
  #|    DuckDBTimestampSecondsValue: class {
  #|      constructor(seconds) {
  #|        this.__tag = "ts_s";
  #|        this.value = seconds;
  #|      }
  #|    },
  #|    DuckDBTimestampMillisecondsValue: class {
  #|      constructor(millis) {
  #|        this.__tag = "ts_ms";
  #|        this.value = millis;
  #|      }
  #|    },
  #|  };
  #|  return {
  #|    kind: "appender",
  #|    connection: { kind: "node" },
  #|    appender: fakeAppender,
  #|    api,
  #|  };
  #|}

///|
extern "js" fn js_make_closed_node_appender() -> Appender =
  #|() => ({
  #|    kind: "appender",
  #|    closed: true,
  #|    connection: { kind: "node" },
  #|    appender: {},
  #|    api: {},
  #|  })

///|
/// Appended rows across all `appendDataChunk` calls, in the
/// `decode_query_result` payload shape.
extern "js" fn js_fake_appended_json(appender : Appender) -> String =
  #|(appender) => {
  #|  const appended = appender.appender.appended;
  #|  const ncols = appended.length ? appended[0].length : 0;
  #|  const rows = [];
  #|  const nulls = [];
  #|  for (const columns of appended) {
  #|    const nrows = ncols ? columns[0].length : 0;
  #|    for (let r = 0; r < nrows; r++) {
  #|      const row = [];
  #|      const rowNulls = [];
  #|      for (let c = 0; c < ncols; c++) {
  #|        row.push(columns[c][r]);
  #|        rowNulls.push(columns[c][r] === "");
  #|      }
  #|      rows.push(row);
  #|      nulls.push(rowNulls);
  #|    }
  #|  }
  #|  const columns = [];
  #|  for (let c = 0; c < ncols; c++) columns.push("c" + c);
  #|  return JSON.stringify({ ok: true, columns, rows, nulls });
  #|}

///|
/// Row count passed to each `appendDataChunk` call, comma-joined.
extern "js" fn js_fake_call_rows(appender : Appender) -> String =
  #|(appender) =>
  #|    appender.appender.appended
  #|      .map((columns) => String(columns.length ? columns[0].length : 0))
  #|      .join(",")

///|
test "js append_chunk end-to-end through the MoonBit wrapper" {
  let appender = js_make_fake_node_appender()
  let chunk : VectorChunk = {
    columns: ["i", "s", "tss", "tsms"],
    vectors: [
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array([1, 2, 3])),
      ),
      Vector::all_valid(
        ColumnType::Varchar,
        VectorData::Varchar(["a", "b", "c"]),
      ),
      Vector::all_valid(
        ColumnType::TimestampS,
        VectorData::Int64(
          FixedArray::from_array([1000000L, 2000000L, 3600000000L]),
        ),
      ),
      Vector::all_valid(
        ColumnType::TimestampMs,
        VectorData::Int64(
          FixedArray::from_array([1500000L, 2000000L, 3600123000L]),
        ),
      ),
    ],
  }
  match appender.append_chunk(chunk) {
    Ok(_) => ()
    Err(err) => fail("append_chunk failed: \{err.message()}")
  }
  match decode_query_result(js_fake_appended_json(appender)) {
    Ok((_columns, rows, _nulls)) => {
      if rows.length() != 3 {
        fail("expected 3 appended rows, got \{rows.length()}")
      }
      inspect(rows[0][0], content="1")
      inspect(rows[0][1], content="a")
      // Canonical micros converted to each variant's native unit.
      inspect(rows[0][2], content="ts_s:1")
      inspect(rows[1][2], content="ts_s:2")
      inspect(rows[2][2], content="ts_s:3600")
      inspect(rows[0][3], content="ts_ms:1500")
      inspect(rows[1][3], content="ts_ms:2000")
      inspect(rows[2][3], content="ts_ms:3600123")
    }
    Err(message) => fail("decode failed: \{message}")
  }
}

///|
test "js append_chunk slices oversized input across appendDataChunk calls" {
  let appender = js_make_fake_node_appender()
  let rows = 5000
  let ids : Array[Int] = []
  for i in 0..<rows {
    ids.push(i)
  }
  let chunk : VectorChunk = {
    columns: ["id"],
    vectors: [
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array(ids)),
      ),
    ],
  }
  match appender.append_chunk(chunk) {
    Ok(_) => ()
    Err(err) => fail("append_chunk failed: \{err.message()}")
  }
  // 5000 rows split at the 2048-row data chunk capacity.
  inspect(js_fake_call_rows(appender), content="2048,2048,904")
}

///|
test "js append_chunk on a closed appender reports Closed" {
  let appender = js_make_closed_node_appender()
  let chunk : VectorChunk = {
    columns: ["id"],
    vectors: [
      Vector::all_valid(
        ColumnType::Integer,
        VectorData::Int32(FixedArray::from_array([1])),
      ),
    ],
  }
  match appender.append_chunk(chunk) {
    Ok(_) => fail("expected append_chunk on closed appender to fail")
    Err(err) => inspect(err.message(), content="appender is closed")
  }
}

///|
test "js node appendDataChunk contract" {
  let result = match
    js_run_append_chunk_test(
      "CREATE TABLE test_chunk (i INTEGER, v VARCHAR, b BOOLEAN, g BIGINT, f DOUBLE, d DATE, ts TIMESTAMP, blob BLOB, dec DECIMAL(10,2), iv INTERVAL, h HUGEINT, u UUID, tss TIMESTAMP_S, tsms TIMESTAMP_MS)",
      "test_chunk",
    ) {
    "" => return ()
    payload => decode_query_result(payload)
  }
  match result {
    Ok((_columns, rows, nulls)) => {
      if rows.length() != 3 {
        fail("expected 3 rows, got \{rows.length()}")
      }
      inspect(rows[0][0], content="1")
      inspect(rows[1][0], content="2")
      inspect(rows[2][0], content="3")
      inspect(nulls[1][1], content="true") // varchar NULL
      inspect(nulls[1][7], content="true") // blob NULL
      inspect(rows[0][2], content="true")
      inspect(rows[0][3], content="10")
      inspect(rows[0][4], content="0.5")
      inspect(rows[0][5], content="2024-10-04") // 20000 days
      inspect(rows[0][6], content="1970-01-01 00:00:01")
      inspect(rows[0][7], content="AB")
      inspect(rows[0][8], content="123.45")
      inspect(rows[0][9], content="1 month 2 days 00:00:03")
      inspect(rows[0][10], content="5")
      inspect(rows[0][11], content="00000001-550e-8400-e79f-45f655440000")
      inspect(rows[2][11], content="ffffffff-ffff-ffff-ffff-ffffffffffff")
      // TIMESTAMP_S: canonical 1000000us -> 1s -> 1970-01-01 00:00:01.
      inspect(rows[0][12], content="1970-01-01 00:00:01")
      inspect(rows[1][12], content="1970-01-01 01:00:00")
      // TIMESTAMP_MS: canonical 1500000us -> 1500ms -> 00:00:01.5.
      inspect(rows[0][13], content="1970-01-01 00:00:01.5")
      inspect(rows[1][13], content="1970-01-01 01:00:00.123")
    }
    Err(message) =>
      if !message.contains("@duckdb/node-api") {
        fail("node appendDataChunk contract failed: \{message}")
      }
  }
}

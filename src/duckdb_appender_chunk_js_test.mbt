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

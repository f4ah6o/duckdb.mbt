///|
/// Benchmark: legacy string materialization (`Connection::query` /
/// `chunks_to_query_result`) vs the typed chunk path (`Connection::query_chunks`
/// + columnar `Vector` accessors) for a large numeric result set.
///
/// Usage: `moon run src/cmd/vector_bench --target native --release -- [rows]`
/// Prints wall-clock ms and rows/s for each path. The comparison measures the
/// result-consumption path only; DuckDB-side query execution is identical.
fn main {
  let rows = 1_000_000
  let sql = "SELECT i::BIGINT AS i, i::DOUBLE * 1.5 AS d, (i % 7) AS f FROM range(\{rows}) t(i)"
  @lib.connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        bench_string_path(conn, sql, rows)
        bench_typed_path(conn, sql, rows)
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => println("connect failed: \{err.message()}")
    }
  })
}

///|
fn bench_string_path(conn : @lib.Connection, sql : String, rows : Int) -> Unit {
  let start = @bench.monotonic_clock_start()
  let mut checksum = 0.0
  let mut cells = 0
  conn.query(sql, on_done=fn(result) {
    match result {
      Ok(qr) =>
        for row in qr.rows {
          for cell in row {
            checksum = checksum + cell.length().to_double()
            cells = cells + 1
          }
        }
      Err(err) => println("query failed: \{err.message()}")
    }
  })
  let elapsed = @bench.monotonic_clock_end(start) / 1000.0
  println(
    "string path: \{elapsed}ms, \{cells} cells materialized, \{fmt_rows_per_s(rows, elapsed)} rows/s (checksum \{checksum})",
  )
}

///|
fn bench_typed_path(conn : @lib.Connection, sql : String, rows : Int) -> Unit {
  let start = @bench.monotonic_clock_start()
  let mut checksum = 0.0
  let mut cells = 0
  conn.query_chunks(sql, on_done=fn(result) {
    match result {
      Ok(chunks) =>
        for chunk in chunks {
          for col = 0; col < chunk.column_count(); col = col + 1 {
            let vector = chunk.column(col).unwrap()
            if vector.int64s() is Some(xs) {
              for x in xs {
                checksum = checksum + x.to_double()
                cells = cells + 1
              }
            } else if vector.ints() is Some(xs) {
              for x in xs {
                checksum = checksum + x.to_double()
                cells = cells + 1
              }
            } else if vector.doubles() is Some(xs) {
              for x in xs {
                checksum = checksum + x
                cells = cells + 1
              }
            }
          }
        }
      Err(err) => println("query_chunks failed: \{err.message()}")
    }
  })
  let elapsed = @bench.monotonic_clock_end(start) / 1000.0
  println(
    "typed path:  \{elapsed}ms, \{cells} cells consumed,  \{fmt_rows_per_s(rows, elapsed)} rows/s (checksum \{checksum})",
  )
}

///|
fn fmt_rows_per_s(rows : Int, elapsed_ms : Double) -> String {
  if elapsed_ms <= 0.0 {
    return "inf"
  }
  (rows.to_double() / elapsed_ms * 1000.0).round().to_string()
}

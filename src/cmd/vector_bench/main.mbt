///|
/// Benchmark: legacy string materialization (`Connection::query` /
/// `chunks_to_query_result`) vs the typed chunk path (`Connection::query_chunks`
/// + columnar `Vector` accessors) for a large numeric result set.
///
/// Usage:
///   `moon run src/cmd/vector_bench --target native --release -- [mode] [rows]`
///   - mode: `all` (default), `string`, or `typed` — pick one path per
///     process for a fair peak-RSS comparison.
///   - rows: row count (default 1_000_000).
/// Prints wall-clock ms, rows/s, a deterministic logical-allocation count,
/// and peak RSS in KiB (`ru_maxrss` — KiB on Linux, bytes on macOS, so
/// `peak_rss_kb` normalizes by 1024 there) for each path. The comparison
/// measures the result-consumption path only; DuckDB-side query execution
/// is identical.

///|
/// Parse a positive decimal integer, or `None` for anything else.
fn parse_rows(s : String) -> Int? {
  if s.length() == 0 {
    return None
  }
  let mut n = 0
  for c in s {
    if c < '0' || c > '9' {
      return None
    }
    n = n * 10 + (c.to_int() - '0'.to_int())
  }
  if n <= 0 {
    None
  } else {
    Some(n)
  }
}

///|
fn main {
  let mut rows = 1_000_000
  let mut mode = "all"
  for arg in @env.args() {
    if arg == "all" || arg == "string" || arg == "typed" {
      mode = arg
    } else {
      match parse_rows(arg) {
        Some(n) => rows = n
        None => ()
      }
    }
  }
  let sql = "SELECT i::BIGINT AS i, i::DOUBLE * 1.5 AS d, (i % 7) AS f FROM range(\{rows}) t(i)"
  println("vector_bench: rows=\{rows} mode=\{mode} sql=\{sql}")
  println("peak RSS at start: \{peak_rss_kb()} KiB")
  @lib.connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        if mode == "all" || mode == "string" {
          bench_string_path(conn, sql, rows)
        }
        if mode == "all" || mode == "typed" {
          bench_typed_path(conn, sql, rows)
        }
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => println("connect failed: \{err.message()}")
    }
  })
}

///|
fn bench_string_path(conn : @lib.Connection, sql : String, rows : Int) -> Unit {
  let rss_before = peak_rss_kb()
  let start = @bench.monotonic_clock_start()
  let mut checksum = 0.0
  let mut cells = 0
  let mut row_arrays = 0
  conn.query(sql, on_done=fn(result) {
    match result {
      Ok(qr) =>
        for row in qr.rows {
          row_arrays = row_arrays + 1
          for cell in row {
            checksum = checksum + cell.length().to_double()
            cells = cells + 1
          }
        }
      Err(err) => println("query failed: \{err.message()}")
    }
  })
  let elapsed = @bench.monotonic_clock_end(start) / 1000.0
  let rss_after = peak_rss_kb()
  println(
    "string path: \{elapsed}ms, \{cells} cells materialized, \{fmt_rows_per_s(rows, elapsed)} rows/s (checksum \{checksum})",
  )
  println(
    "  logical allocs: \{cells} cell Strings + \{cells} cell Bytes, \{row_arrays} Array[String] rows + \{row_arrays} Array[Bool] null rows (+ per-column scratch arrays)",
  )
  println(
    "  peak RSS: \{rss_before} -> \{rss_after} KiB (delta \{rss_after - rss_before} KiB)",
  )
}

///|
fn bench_typed_path(conn : @lib.Connection, sql : String, rows : Int) -> Unit {
  let rss_before = peak_rss_kb()
  let start = @bench.monotonic_clock_start()
  let mut checksum = 0.0
  let mut cells = 0
  let mut chunk_count = 0
  let mut vector_count = 0
  conn.query_chunks(sql, on_done=fn(result) {
    match result {
      Ok(chunks) =>
        for chunk in chunks {
          chunk_count = chunk_count + 1
          for col = 0; col < chunk.column_count(); col = col + 1 {
            vector_count = vector_count + 1
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
  let rss_after = peak_rss_kb()
  println(
    "typed path:  \{elapsed}ms, \{cells} cells consumed,  \{fmt_rows_per_s(rows, elapsed)} rows/s (checksum \{checksum})",
  )
  println(
    "  logical allocs: \{chunk_count} chunks -> \{vector_count} vectors (\{vector_count} data arrays + \{vector_count * 2} validity arrays), 0 boxed cell objects",
  )
  println(
    "  peak RSS: \{rss_before} -> \{rss_after} KiB (delta \{rss_after - rss_before} KiB)",
  )
}

///|
fn fmt_rows_per_s(rows : Int, elapsed_ms : Double) -> String {
  if elapsed_ms <= 0.0 {
    return "inf"
  }
  (rows.to_double() / elapsed_ms * 1000.0).round().to_string()
}

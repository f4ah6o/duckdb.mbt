///|
/// Benchmark: row-wise appender (`begin_row`/`append_*`/`end_row` per cell)
/// vs vectorized `Appender::append_chunk` bulk ingestion (issue #68).
///
/// Usage: `moon run src/cmd/appender_bench --target native --release --`
/// Prints wall-clock ms and rows/s for each path over the same row count.
fn main {
  let rows = 500_000
  @lib.connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query(
          "CREATE TABLE bench_row (id INTEGER, val DOUBLE, tag VARCHAR)",
          on_done=fn(_) {
            conn.query(
              "CREATE TABLE bench_chunk (id INTEGER, val DOUBLE, tag VARCHAR)",
              on_done=fn(_) {
                bench_row_wise(conn, rows)
                bench_chunk_wise(conn, rows)
                conn.close(on_done=fn(_) { () })
              },
            )
          },
        )
      Err(err) => println("connect failed: \{err.message()}")
    }
  })
}

///|
fn fmt_rows_per_s(rows : Int, elapsed : Double) -> String {
  if elapsed <= 0.0 {
    "inf"
  } else {
    "\{(rows.to_double() / (elapsed / 1000.0)).to_int()}"
  }
}

///|
fn bench_row_wise(conn : @lib.Connection, rows : Int) -> Unit {
  let start = @bench.monotonic_clock_start()
  conn.create_appender("main", "bench_row", on_done=fn(result) {
    match result {
      Ok(app) => {
        let mut failed = false
        for i in 0..<rows {
          if failed {
            continue
          }
          match app.begin_row() {
            Ok(_) =>
              match app.append_int(i) {
                Ok(_) =>
                  match app.append_double(i.to_double() * 1.5) {
                    Ok(_) =>
                      match app.append_varchar("row_\{i}") {
                        Ok(_) =>
                          match app.end_row() {
                            Ok(_) => ()
                            Err(e) => {
                              println("end_row failed: \{e.message()}")
                              failed = true
                            }
                          }
                        Err(e) => {
                          println("append_varchar failed: \{e.message()}")
                          failed = true
                        }
                      }
                    Err(e) => {
                      println("append_double failed: \{e.message()}")
                      failed = true
                    }
                  }
                Err(e) => {
                  println("append_int failed: \{e.message()}")
                  failed = true
                }
              }
            Err(e) => {
              println("begin_row failed: \{e.message()}")
              failed = true
            }
          }
        }
        match app.flush() {
          Ok(_) => ()
          Err(e) => println("flush failed: \{e.message()}")
        }
      }
      Err(err) => println("create_appender failed: \{err.message()}")
    }
  })
  let elapsed = @bench.monotonic_clock_end(start) / 1000.0
  println(
    "row-wise appender: \{elapsed}ms, \{rows} rows, \{fmt_rows_per_s(rows, elapsed)} rows/s",
  )
}

///|
fn bench_chunk_wise(conn : @lib.Connection, rows : Int) -> Unit {
  let chunk_rows = 2048
  // Prebuild the reusable chunk payloads once; the benchmark measures append,
  // not VectorData construction.
  let ids = FixedArray::make(chunk_rows, 0)
  let vals = FixedArray::make(chunk_rows, 0.0)
  let tags : Array[String] = Array::make(chunk_rows, "")
  for i in 0..<chunk_rows {
    ids[i] = i
    vals[i] = i.to_double() * 1.5
    tags[i] = "row_\{i}"
  }
  let vector_chunk : @lib.VectorChunk = {
    columns: ["id", "val", "tag"],
    vectors: [
      @lib.Vector::all_valid(
        @lib.ColumnType::Integer,
        @lib.VectorData::Int32(ids),
      ),
      @lib.Vector::all_valid(
        @lib.ColumnType::Double,
        @lib.VectorData::Double(vals),
      ),
      @lib.Vector::all_valid(
        @lib.ColumnType::Varchar,
        @lib.VectorData::Varchar(tags),
      ),
    ],
  }
  let start = @bench.monotonic_clock_start()
  conn.create_appender("main", "bench_chunk", on_done=fn(result) {
    match result {
      Ok(app) => {
        let mut appended = 0
        let mut failed = false
        while appended < rows && !failed {
          match app.append_chunk(vector_chunk) {
            Ok(_) => appended = appended + chunk_rows
            Err(e) => {
              println("append_chunk failed: \{e.message()}")
              failed = true
            }
          }
        }
        match app.flush() {
          Ok(_) => ()
          Err(e) => println("flush failed: \{e.message()}")
        }
      }
      Err(err) => println("create_appender failed: \{err.message()}")
    }
  })
  let elapsed = @bench.monotonic_clock_end(start) / 1000.0
  println(
    "chunk appender (\{chunk_rows} rows/chunk): \{elapsed}ms, \{rows} rows, \{fmt_rows_per_s(rows, elapsed)} rows/s",
  )
}

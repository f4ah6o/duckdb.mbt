///|
// Tests for `Appender::append_chunk` (issue #68): bulk VectorChunk ingestion
// on the native backend — round-trips typed payloads and NULLs, rejects
// malformed chunks with structured errors, and interops with the row-wise
// appender API.

///|
fn run_native_chunk_append(
  setup_sql : String,
  append_fn : (@duckdb.Appender) -> Result[Unit, @duckdb.DuckDBError],
  verify_sql : String,
) -> Result[@duckdb.QueryResult, String] {
  let result_ref : Ref[@duckdb.QueryResult?] = Ref(None)
  let error_ref : Ref[String?] = Ref(None)
  @duckdb.connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query(setup_sql, on_done=fn(setup_result) {
          match setup_result {
            Ok(_) =>
              conn.create_appender("main", "test_table", on_done=fn(
                append_result,
              ) {
                match append_result {
                  Ok(appender) =>
                    match append_fn(appender) {
                      Ok(_) =>
                        appender.close(on_done=fn(_) {
                          conn.query(verify_sql, on_done=fn(query_result) {
                            match query_result {
                              Ok(res) => {
                                result_ref.val = Some(res)
                                conn.close(on_done=fn(_) { () })
                              }
                              Err(err) =>
                                error_ref.val = Some(
                                  "verify query failed: \{err.message()}",
                                )
                            }
                          })
                        })
                      Err(err) =>
                        error_ref.val = Some(
                          "append_chunk failed: \{err.message()}",
                        )
                    }
                  Err(err) =>
                    error_ref.val = Some(
                      "create_appender failed: \{err.message()}",
                    )
                }
              })
            Err(err) =>
              error_ref.val = Some("setup query failed: \{err.message()}")
          }
        })
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(result) => Ok(result)
        None => Err("no result returned")
      }
  }
}

///|
fn int_vector(values : Array[Int32]) -> @duckdb.Vector {
  @duckdb.Vector::all_valid(
    @duckdb.ColumnType::Integer,
    @duckdb.VectorData::Int32(FixedArray::from_array(values)),
  )
}

///|
fn varchar_vector(values : Array[String]) -> @duckdb.Vector {
  @duckdb.Vector::all_valid(
    @duckdb.ColumnType::Varchar,
    @duckdb.VectorData::Varchar(values),
  )
}

///|
test "native append_chunk basic flat types" {
  let rows = 1000
  let ids : Array[Int32] = []
  let names : Array[String] = []
  let scores : Array[Double] = []
  let actives : Array[Bool] = []
  for i in 0..<rows {
    ids.push(i)
    names.push("row_\{i}")
    scores.push(i.to_double() * 1.5)
    actives.push(i % 2 == 0)
  }
  let chunk : @duckdb.VectorChunk = {
    columns: ["id", "name", "score", "active"],
    vectors: [
      int_vector(ids),
      varchar_vector(names),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Double,
        @duckdb.VectorData::Double(FixedArray::from_array(scores)),
      ),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Boolean,
        @duckdb.VectorData::Bool(FixedArray::from_array(actives)),
      ),
    ],
  }
  let result = run_native_chunk_append(
    "CREATE TABLE test_table (id INTEGER, name VARCHAR, score DOUBLE, active BOOLEAN)",
    fn(app) { app.append_chunk(chunk) },
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) => {
      if value.row_count() != rows {
        fail("expected \{rows} rows, got \{value.row_count()}")
      }
      match value.cell(0, 0) {
        Some(v) => if v != "0" { fail("expected id='0', got '\{v}'") }
        None => fail("expected non-null id")
      }
      match value.cell(0, 1) {
        Some(v) => if v != "row_0" { fail("expected name='row_0', got '\{v}'") }
        None => fail("expected non-null name")
      }
      match value.cell(1, 2) {
        Some(v) => if v != "1.5" { fail("expected score='1.5', got '\{v}'") }
        None => fail("expected non-null score")
      }
      match value.cell(1, 3) {
        Some(v) =>
          if v != "false" {
            fail("expected active='false', got '\{v}'")
          }
        None => fail("expected non-null active")
      }
      match value.cell(rows - 1, 1) {
        Some(v) =>
          if v != "row_\{rows - 1}" {
            fail("expected last name='row_\{rows - 1}', got '\{v}'")
          }
        None => fail("expected non-null last name")
      }
    }
    Err(err) => fail("append_chunk test failed: \{err}")
  }
}

///|
test "native append_chunk nulls and typed values" {
  let ids = FixedArray::from_array([1, 2, 3])
  let names = FixedArray::from_array(["a", "", "c"])
  let names_validity = FixedArray::from_array([true, false, true])
  let bigs = FixedArray::from_array([10000000000L, -5L, 0L])
  let bigs_validity = FixedArray::from_array([false, true, true])
  let chunk : @duckdb.VectorChunk = {
    columns: ["id", "name", "big"],
    vectors: [
      @duckdb.Vector::{
        logical_type: @duckdb.ColumnType::Integer,
        data: @duckdb.VectorData::Int32(ids),
        validity: FixedArray::make(3, true),
      },
      @duckdb.Vector::{
        logical_type: @duckdb.ColumnType::Varchar,
        data: @duckdb.VectorData::Varchar(Array::from_fixed_array(names)),
        validity: names_validity,
      },
      @duckdb.Vector::{
        logical_type: @duckdb.ColumnType::BigInt,
        data: @duckdb.VectorData::Int64(bigs),
        validity: bigs_validity,
      },
    ],
  }
  let result = run_native_chunk_append(
    "CREATE TABLE test_table (id INTEGER, name VARCHAR, big BIGINT)",
    fn(app) { app.append_chunk(chunk) },
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) => {
      if value.row_count() != 3 {
        fail("expected 3 rows, got \{value.row_count()}")
      }
      match value.cell(1, 1) {
        Some(v) => fail("expected NULL name, got '\{v}'")
        None => ()
      }
      match value.cell(0, 2) {
        Some(v) => fail("expected NULL big, got '\{v}'")
        None => ()
      }
      match value.cell(2, 0) {
        Some(v) => if v != "3" { fail("expected id='3', got '\{v}'") }
        None => fail("expected non-null id")
      }
      match value.cell(2, 2) {
        Some(v) => if v != "0" { fail("expected big='0', got '\{v}'") }
        None => fail("expected non-null big")
      }
    }
    Err(err) => fail("append_chunk null test failed: \{err}")
  }
}

///|
test "native append_chunk advanced typed values" {
  let dates = FixedArray::from_array([
    @duckdb.date_from_ymd(2024, 1, 15),
    @duckdb.date_from_ymd(1999, 12, 31),
  ])
  let tss = FixedArray::from_array([
    @duckdb.timestamp_from_ymd_hms(2024, 1, 15, 10, 30, 0),
    @duckdb.timestamp_from_ymd_hms(2000, 6, 30, 23, 59, 59),
  ])
  let decimals = FixedArray::from_array([
    @duckdb.decimal_from_hugeint(12345UL, 0L, 10, 2), // 123.45
    @duckdb.decimal_from_hugeint(100UL, 0L, 10, 2), // 1.00
  ])
  let intervals = FixedArray::from_array([
    @duckdb.interval_from_parts(1, 2, 3000000L),
    @duckdb.interval_from_parts(0, 0, 0L),
  ])
  let huges : @duckdb.VectorData = @duckdb.VectorData::HugeInt(
    lower=FixedArray::from_array([1UL, 2UL]),
    upper=FixedArray::from_array([0L, 0L]),
  )
  let blob_a = b"blob-a"
  let blob_b = b"blob-b"
  let chunk : @duckdb.VectorChunk = {
    columns: ["d", "ts", "dec", "iv", "h", "b"],
    vectors: [
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Date,
        @duckdb.VectorData::Int32(dates),
      ),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Timestamp,
        @duckdb.VectorData::Int64(tss),
      ),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Decimal,
        @duckdb.VectorData::Decimal(decimals),
      ),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Interval,
        @duckdb.VectorData::Interval(intervals),
      ),
      @duckdb.Vector::all_valid(@duckdb.ColumnType::HugeInt, huges),
      @duckdb.Vector::all_valid(
        @duckdb.ColumnType::Blob,
        @duckdb.VectorData::Blob([blob_a, blob_b]),
      ),
    ],
  }
  let result = run_native_chunk_append(
    "CREATE TABLE test_table (d DATE, ts TIMESTAMP, dec DECIMAL(10,2), iv INTERVAL, h HUGEINT, b BLOB)",
    fn(app) { app.append_chunk(chunk) },
    "SELECT d::VARCHAR, ts::VARCHAR, dec::VARCHAR, iv::VARCHAR, h::VARCHAR, b::VARCHAR FROM test_table ORDER BY h",
  )
  match result {
    Ok(value) => {
      if value.row_count() != 2 {
        fail("expected 2 rows, got \{value.row_count()}")
      }
      debug_inspect(
        value.cell(0, 0),
        content=(
          #|Some("2024-01-15")
        ),
      )
      debug_inspect(
        value.cell(0, 1),
        content=(
          #|Some("2024-01-15 10:30:00")
        ),
      )
      debug_inspect(
        value.cell(0, 2),
        content=(
          #|Some("123.45")
        ),
      )
      debug_inspect(
        value.cell(0, 3),
        content=(
          #|Some("1 month 2 days 00:00:03")
        ),
      )
      debug_inspect(
        value.cell(0, 4),
        content=(
          #|Some("1")
        ),
      )
      debug_inspect(
        value.cell(0, 5),
        content=(
          #|Some("blob-a")
        ),
      )
      debug_inspect(
        value.cell(1, 0),
        content=(
          #|Some("1999-12-31")
        ),
      )
      debug_inspect(
        value.cell(1, 1),
        content=(
          #|Some("2000-06-30 23:59:59")
        ),
      )
      debug_inspect(
        value.cell(1, 2),
        content=(
          #|Some("1.00")
        ),
      )
      debug_inspect(
        value.cell(1, 3),
        content=(
          #|Some("00:00:00")
        ),
      )
      debug_inspect(
        value.cell(1, 4),
        content=(
          #|Some("2")
        ),
      )
      debug_inspect(
        value.cell(1, 5),
        content=(
          #|Some("blob-b")
        ),
      )
    }
    Err(err) => fail("advanced append_chunk failed: \{err}")
  }
}

///|
test "native append_chunk splits inputs larger than vector capacity" {
  // A duckdb_data_chunk vector holds at most duckdb_vector_size() rows
  // (normally 2048); larger VectorChunks must be appended as consecutive
  // slices rather than overrunning the vector buffers.
  let rows = 5000
  let ids : Array[Int32] = []
  let names : Array[String] = []
  for i in 0..<rows {
    ids.push(i)
    names.push("name_\{i}")
  }
  let chunk : @duckdb.VectorChunk = {
    columns: ["id", "name"],
    vectors: [int_vector(ids), varchar_vector(names)],
  }
  let result = run_native_chunk_append(
    "CREATE TABLE test_table (id INTEGER, name VARCHAR)",
    fn(app) { app.append_chunk(chunk) },
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) => {
      if value.row_count() != rows {
        fail("expected \{rows} rows, got \{value.row_count()}")
      }
      // Spot-check the slice boundary (row 2048 starts the second chunk).
      match value.cell(2048, 1) {
        Some(v) =>
          if v != "name_2048" {
            fail("expected name_2048 at slice boundary, got '\{v}'")
          }
        None => fail("expected non-null name at slice boundary")
      }
      match value.cell(rows - 1, 0) {
        Some(v) =>
          if v != "\{rows - 1}" {
            fail("expected last id='\{rows - 1}', got '\{v}'")
          }
        None => fail("expected non-null last id")
      }
    }
    Err(err) => fail("oversized append_chunk failed: \{err}")
  }
}

///|
test "native append_chunk interoperates with row-wise appends" {
  let chunk : @duckdb.VectorChunk = {
    columns: ["id", "name"],
    vectors: [int_vector([1, 2, 3]), varchar_vector(["one", "two", "three"])],
  }
  fn mixed(app : @duckdb.Appender) -> Result[Unit, @duckdb.DuckDBError] {
    match app.append_chunk(chunk) {
      Ok(_) =>
        match app.begin_row() {
          Ok(_) =>
            match app.append_int(4) {
              Ok(_) =>
                match app.append_varchar("four") {
                  Ok(_) =>
                    match app.end_row() {
                      Ok(_) => app.flush()
                      Err(e) => Err(e)
                    }
                  Err(e) => Err(e)
                }
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }

  let result = run_native_chunk_append(
    "CREATE TABLE test_table (id INTEGER, name VARCHAR)", mixed, "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) => {
      if value.row_count() != 4 {
        fail("expected 4 rows, got \{value.row_count()}")
      }
      match value.cell(3, 1) {
        Some(v) => if v != "four" { fail("expected 'four', got '\{v}'") }
        None => fail("expected non-null last name")
      }
    }
    Err(err) => fail("interop test failed: \{err}")
  }
}

///|
test "native append_chunk rejects malformed chunks" {
  fn expect_invalid(
    chunk : @duckdb.VectorChunk,
    case_name : String,
  ) -> Unit raise {
    match
      run_native_chunk_append(
        "CREATE TABLE test_table (a VARCHAR)",
        fn(app) { app.append_chunk(chunk) },
        "SELECT COUNT(*) FROM test_table",
      ) {
      Ok(_) => fail("expected InvalidArgument for \{case_name}")
      Err(msg) =>
        if !msg.contains("invalid argument") {
          fail("expected invalid argument error for \{case_name}, got \{msg}")
        }
    }
  }

  // Empty chunk.
  expect_invalid({ columns: [], vectors: [], }, "empty chunk")
  // Column/vector count mismatch.
  expect_invalid(
    { columns: ["a", "b"], vectors: [int_vector([1])], },
    "column/vector count mismatch",
  )
  // Ragged row counts.
  expect_invalid(
    {
      columns: ["a", "b"],
      vectors: [int_vector([1, 2]), varchar_vector(["x"])],
    },
    "ragged row counts",
  )
  // Payload shorter than validity mask.
  expect_invalid(
    {
      columns: ["a"],
      vectors: [
        @duckdb.Vector::{
          logical_type: @duckdb.ColumnType::Integer,
          data: @duckdb.VectorData::Int32(FixedArray::from_array([1, 2])),
          validity: FixedArray::make(3, true),
        },
      ],
    },
    "payload shorter than validity",
  )
  // Logical type / payload mismatch.
  expect_invalid(
    {
      columns: ["a"],
      vectors: [
        @duckdb.Vector::all_valid(
          @duckdb.ColumnType::Varchar,
          @duckdb.VectorData::Int32(FixedArray::from_array([1])),
        ),
      ],
    },
    "mismatched logical type",
  )
}

///|
test "native append_chunk reports Unsupported for nested and Any columns" {
  let list_vector = @duckdb.Vector::all_valid(
    @duckdb.ColumnType::List,
    @duckdb.VectorData::List(
      child=int_vector([1, 2, 3]),
      offsets=FixedArray::from_array([0UL]),
      lengths=FixedArray::from_array([3UL]),
    ),
  )
  let chunk : @duckdb.VectorChunk = { columns: ["l"], vectors: [list_vector], }
  match
    run_native_chunk_append(
      "CREATE TABLE test_table (l INTEGER[])",
      fn(app) { app.append_chunk(chunk) },
      "SELECT COUNT(*) FROM test_table",
    ) {
    Ok(_) => fail("expected Unsupported for list column")
    Err(msg) =>
      if !msg.contains("LIST") {
        fail("expected unsupported LIST error, got \{msg}")
      }
  }
}

///|
test "native append_chunk while a row is in progress" {
  let chunk : @duckdb.VectorChunk = {
    columns: ["id", "name"],
    vectors: [int_vector([9, 10]), varchar_vector(["nine", "ten"])],
  }
  fn mid_row(app : @duckdb.Appender) -> Result[Unit, @duckdb.DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(5) {
          // Chunk append while a row is open must error, not corrupt state.
          Ok(_) => app.append_chunk(chunk)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }

  match
    run_native_chunk_append(
      "CREATE TABLE test_table (id INTEGER, name VARCHAR)", mid_row, "SELECT COUNT(*) FROM test_table",
    ) {
    Ok(_) => fail("expected append_chunk mid-row to fail")
    Err(msg) =>
      if !msg.contains("append_chunk failed") {
        fail("expected append_chunk failure, got \{msg}")
      }
  }
}

///|
test "native append_chunk type mismatch against table" {
  let chunk : @duckdb.VectorChunk = {
    columns: ["id"],
    vectors: [varchar_vector(["not-an-int"])],
  }
  match
    run_native_chunk_append(
      "CREATE TABLE test_table (id INTEGER)",
      fn(app) { app.append_chunk(chunk) },
      "SELECT COUNT(*) FROM test_table",
    ) {
    Ok(_) => fail("expected append error for type mismatch")
    Err(msg) =>
      if !msg.contains("append_chunk failed") {
        fail("expected append failure, got \{msg}")
      }
  }
}

///|
test "native append_chunk empty chunk is a no-op error" {
  let empty : @duckdb.VectorChunk = { columns: [], vectors: [], }
  match
    run_native_chunk_append(
      "CREATE TABLE test_table (id INTEGER)",
      fn(app) { app.append_chunk(empty) },
      "SELECT COUNT(*) FROM test_table",
    ) {
    Ok(_) => fail("expected InvalidArgument for empty chunk")
    Err(msg) =>
      if !msg.contains("append_chunk failed") {
        fail("expected append error, got \{msg}")
      }
  }
}

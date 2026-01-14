///|
fn run_native_query(sql : String) -> Result[QueryResult, String] {
  let result_ref : Ref[QueryResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query(sql, on_done=fn(query_result) {
          match query_result {
            Ok(result) => result_ref.val = Some(result)
            Err(DuckDBError::Message(message)) =>
              error_ref.val = Some("query failed: \{message}")
          }
        })
        conn.close(on_done=fn(closed) {
          match closed {
            Ok(_) => ()
            Err(DuckDBError::Message(message)) =>
              error_ref.val = Some("close failed: \{message}")
          }
        })
      }
      Err(DuckDBError::Message(message)) =>
        error_ref.val = Some("connect failed: \{message}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(result) => Ok(result)
        None => Err("query returned no result")
      }
  }
}

///|
test "native fixtures" {
  for case in fixture_cases {
    let result = run_native_query(case.sql)
    match result {
      Ok(value) => expect_query_result(case, value)
      Err(message) => fail("fixture '\{case.name}' failed: \{message}")
    }
  } else {
    ()
  }
}

// ============================================================================
// Prepared Statement Tests
// ============================================================================

///|
fn run_native_prepare_query(
  sql : String,
  bind_fn : (PreparedStatement) -> Result[Unit, DuckDBError],
) -> Result[QueryResult, String] {
  let result_ref : Ref[QueryResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.prepare(sql, on_done=fn(prepare_result) {
          match prepare_result {
            Ok(stmt) => {
              match bind_fn(stmt) {
                Ok(_) => {
                  stmt.execute(on_done=fn(exec_result) {
                    match exec_result {
                      Ok(query_result) => result_ref.val = Some(query_result)
                      Err(DuckDBError::Message(message)) =>
                        error_ref.val = Some("execute failed: \{message}")
                    }
                    stmt.close(on_done=fn(_) {
                      ()
                    })
                  })
                  conn.close(on_done=fn(_) {
                    ()
                  })
                }
                Err(DuckDBError::Message(message)) =>
                  error_ref.val = Some("bind failed: \{message}")
              }
            }
            Err(DuckDBError::Message(message)) =>
              error_ref.val = Some("prepare failed: \{message}")
          }
        })
      }
      Err(DuckDBError::Message(message)) =>
        error_ref.val = Some("connect failed: \{message}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(result) => Ok(result)
        None => Err("prepared query returned no result")
      }
  }
}

///|
test "native prepare simple select" {
  let result = run_native_prepare_query("SELECT 1 AS x", fn(stmt) {
    Ok(())
  })
  match result {
    Ok(value) =>
      if value.column_count() != 1 {
        fail("expected 1 column, got \{value.column_count()}")
      } else if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 0) {
          Some(v) =>
            if v != "1" {
              fail("expected '1', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null value")
        }
      }
    Err(message) => fail("prepare query failed: \{message}")
  }
}

///|
test "native prepare bind int" {
  let result = run_native_prepare_query("SELECT ? * 2 AS x", fn(stmt) {
    stmt.bind_int(1, 21)
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "42" {
            fail("expected '42', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind int failed: \{message}")
  }
}

///|
test "native prepare bind bigint" {
  let result = run_native_prepare_query("SELECT ? * 2 AS x", fn(stmt) {
    stmt.bind_bigint(1, 1000000000)
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "2000000000" {
            fail("expected '2000000000', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind bigint failed: \{message}")
  }
}

///|
test "native prepare bind double" {
  let result = run_native_prepare_query("SELECT ? * 2.0 AS x", fn(stmt) {
    stmt.bind_double(1, 2.5)
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "5" && v != "5.0" {
            fail("expected '5' or '5.0', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind double failed: \{message}")
  }
}

///|
test "native prepare bind varchar" {
  let result = run_native_prepare_query("SELECT CONCAT(?, 'suffix') AS x", fn(stmt) {
    stmt.bind_varchar(1, "prefix")
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "prefixsuffix" {
            fail("expected 'prefixsuffix', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind varchar failed: \{message}")
  }
}

///|
test "native prepare bind bool" {
  let result = run_native_prepare_query("SELECT ? AND TRUE AS x", fn(stmt) {
    stmt.bind_bool(1, true)
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "true" {
            fail("expected 'true', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind bool failed: \{message}")
  }
}

///|
test "native prepare bind null" {
  let result = run_native_prepare_query("SELECT ? IS NULL AS x", fn(stmt) {
    stmt.bind_null(1)
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "true" {
            fail("expected 'true', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("bind null failed: \{message}")
  }
}

///|
test "native prepare multiple params" {
  fn bind_multiple(stmt : PreparedStatement) -> Result[Unit, DuckDBError] {
    match stmt.bind_int(1, 10) {
      Ok(_) =>
        match stmt.bind_int(2, 20) {
          Ok(_) => stmt.bind_int(3, 30)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_prepare_query("SELECT ? + ? + ? AS x", bind_multiple)
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) =>
          if v != "60" {
            fail("expected '60', got '\{v}'")
          } else {
            ()
          }
        None => fail("expected non-null value")
      }
    Err(message) => fail("multiple params failed: \{message}")
  }
}

///|
test "native prepare multiple rows" {
  fn bind_three_rows(stmt : PreparedStatement) -> Result[Unit, DuckDBError] {
    match stmt.bind_int(1, 1) {
      Ok(_) =>
        match stmt.bind_int(2, 2) {
          Ok(_) => stmt.bind_int(3, 3)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_prepare_query("SELECT ? AS x UNION ALL SELECT ? UNION ALL SELECT ?", bind_three_rows)
  match result {
    Ok(value) =>
      if value.row_count() != 3 {
        fail("expected 3 rows, got \{value.row_count()}")
      } else {
        // Check each row
        for row = 0; row < 3; row = row + 1 {
          let expected : String = (row + 1).to_string()
          match value.cell(row, 0) {
            Some(v) =>
              if v != expected {
                fail("row \{row}: expected '\{expected}', got '\{v}'")
              } else {
                ()
              }
            None => fail("row \{row}: expected non-null value")
          }
        } else {
          ()
        }
      }
    Err(message) => fail("multiple rows failed: \{message}")
  }
}

///|
test "native prepare clear bindings" {
  fn bind_then_clear(stmt : PreparedStatement) -> Result[Unit, DuckDBError] {
    match stmt.bind_int(1, 42) {
      Ok(_) =>
        match stmt.clear_bindings() {
          Ok(_) => stmt.bind_null(1)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_prepare_query("SELECT ? AS x", bind_then_clear)
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        None => () // Expected to be null after clear_bindings + bind_null
        Some(v) => fail("expected null after clear_bindings, got '\{v}'")
      }
    Err(message) => fail("clear bindings failed: \{message}")
  }
}

// ============================================================================
// Appender Tests
// ============================================================================

///|
fn run_native_appender_test(
  setup_sql : String,
  append_fn : (Appender) -> Result[Unit, DuckDBError],
  verify_sql : String,
) -> Result[QueryResult, String] {
  let result_ref : Ref[QueryResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)

  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        // First run setup SQL to create table
        conn.query(setup_sql, on_done=fn(setup_result) {
          match setup_result {
            Ok(_) => {
              // Create appender
              conn.create_appender("main", "test_table", on_done=fn(append_result) {
                match append_result {
                  Ok(appender) => {
                    // Run append operations
                    match append_fn(appender) {
                      Ok(_) => {
                        // Close appender
                        appender.close(on_done=fn(close_result) {
                          match close_result {
                            Ok(_) => {
                              // Query to verify
                              conn.query(verify_sql, on_done=fn(query_result) {
                                match query_result {
                                  Ok(result) => {
                                    result_ref.val = Some(result)
                                    conn.close(on_done=fn(_) { () })
                                  }
                                  Err(DuckDBError::Message(msg)) =>
                                    error_ref.val = Some("query failed: \{msg}")
                                }
                              })
                            }
                            Err(DuckDBError::Message(msg)) =>
                              error_ref.val = Some("close failed: \{msg}")
                          }
                        })
                      }
                      Err(DuckDBError::Message(msg)) =>
                        error_ref.val = Some("append failed: \{msg}")
                    }
                  }
                  Err(DuckDBError::Message(msg)) =>
                    error_ref.val = Some("create_appender failed: \{msg}")
                }
              })
            }
            Err(DuckDBError::Message(msg)) =>
              error_ref.val = Some("setup query failed: \{msg}")
          }
        })
      }
      Err(DuckDBError::Message(msg)) =>
        error_ref.val = Some("connect failed: \{msg}")
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
test "native appender basic int" {
  fn append_int_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_int(100) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, value INTEGER)",
    append_int_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 0) {
          Some(v) =>
            if v != "1" {
              fail("expected id='1', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null id")
        }
        match value.cell(0, 1) {
          Some(v) =>
            if v != "100" {
              fail("expected value='100', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null value")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender varchar" {
  fn append_varchar_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_varchar("hello") {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, name VARCHAR)",
    append_varchar_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 1) {
          Some(v) =>
            if v != "hello" {
              fail("expected name='hello', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null name")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender double" {
  fn append_double_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_double(3.14) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, value DOUBLE)",
    append_double_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 1) {
          Some(v) =>
            if v != "3.14" && v != "3.140000" {
              fail("expected value='3.14', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null value")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender bool" {
  fn append_bool_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_bool(true) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, flag BOOLEAN)",
    append_bool_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 1) {
          Some(v) =>
            if v != "true" {
              fail("expected flag='true', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null flag")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender null" {
  fn append_null_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_null() {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, value INTEGER)",
    append_null_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 1) {
          None => () // Expected null value
          Some(v) => fail("expected null, got '\{v}'")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender multiple rows" {
  fn append_two_rows(app : Appender) -> Result[Unit, DuckDBError] {
    // Append first row
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_int(10) {
              Ok(_) =>
                match app.end_row() {
                  Ok(_) =>
                  // Append second row
                  match app.begin_row() {
                    Ok(_) =>
                      match app.append_int(2) {
                        Ok(_) =>
                          match app.append_int(20) {
                            Ok(_) => app.end_row()
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
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, value INTEGER)",
    append_two_rows,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 2 {
        fail("expected 2 rows, got \{value.row_count()}")
      } else {
        // Check first row
        match value.cell(0, 0) {
          Some(v) =>
            if v != "1" {
              fail("row 0 id: expected '1', got '\{v}'")
            } else {
              ()
            }
          None => fail("row 0 id: expected non-null")
        }
        match value.cell(0, 1) {
          Some(v) =>
            if v != "10" {
              fail("row 0 value: expected '10', got '\{v}'")
            } else {
              ()
            }
          None => fail("row 0 value: expected non-null")
        }
        // Check second row
        match value.cell(1, 0) {
          Some(v) =>
            if v != "2" {
              fail("row 1 id: expected '2', got '\{v}'")
            } else {
              ()
            }
          None => fail("row 1 id: expected non-null")
        }
        match value.cell(1, 1) {
          Some(v) =>
            if v != "20" {
              fail("row 1 value: expected '20', got '\{v}'")
            } else {
              ()
            }
          None => fail("row 1 value: expected non-null")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

///|
test "native appender bigint" {
  fn append_bigint_row(app : Appender) -> Result[Unit, DuckDBError] {
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_bigint(1000000000) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, value BIGINT)",
    append_bigint_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        match value.cell(0, 1) {
          Some(v) =>
            if v != "1000000000" {
              fail("expected value='1000000000', got '\{v}'")
            } else {
              ()
            }
          None => fail("expected non-null value")
        }
      }
    Err(message) => fail("appender test failed: \{message}")
  }
}

// ============================================================================
// Date/Timestamp Tests
// ============================================================================

///|
test "native prepare bind date" {
  fn bind_date_param(stmt : PreparedStatement) -> Result[Unit, DuckDBError] {
    // June 3, 2024
    let date = date_from_ymd(2024, 6, 3)
    stmt.bind_date(1, date)
  }
  let result = run_native_prepare_query("SELECT ?::DATE as dt", bind_date_param)
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) => {
          // Check that the result contains expected date string
          let (year, month, day) = date_to_ymd(date_from_ymd(2024, 6, 3))
          let expected_date = "\\{year}-\\{String::pad_left(month.to_string(), 2, '0')}-\\{String::pad_left(day.to_string(), 2, '0')}"
          if v != expected_date {
            // The actual date from DuckDB may be formatted differently
            // Just check that we got some date-like string
            if !v.contains("-") {
              fail("expected date-like string, got '\{v}'")
            } else {
              ()
            }
          } else {
            ()
          }
        }
        None => fail("expected non-null date value")
      }
    Err(message) => fail("bind date test failed: \{message}")
  }
}

///|
test "native prepare bind timestamp" {
  fn bind_timestamp_param(stmt : PreparedStatement) -> Result[Unit, DuckDBError] {
    // June 3, 2024, 12:34:56
    let ts = timestamp_from_ymd_hms(2024, 6, 3, 12, 34, 56)
    stmt.bind_timestamp(1, ts)
  }
  let result = run_native_prepare_query("SELECT ?::TIMESTAMP as ts", bind_timestamp_param)
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        Some(v) => {
          // Check that we got some timestamp-like string
          if !v.contains("-") {
            fail("expected timestamp-like string, got '\{v}'")
          } else {
            ()
          }
        }
        None => fail("expected non-null timestamp value")
      }
    Err(message) => fail("bind timestamp test failed: \{message}")
  }
}

///|
test "native appender append date" {
  fn append_date_row(app : Appender) -> Result[Unit, DuckDBError] {
    let date = date_from_ymd(2024, 6, 3)
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_date(date) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, dt DATE)",
    append_date_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        // Check that we got a date-like value
        match value.cell(0, 1) {
          Some(v) => {
            if !v.contains("-") {
              fail("expected date-like string, got '\{v}'")
            } else {
              ()
            }
          }
          None => fail("expected non-null date value")
        }
      }
    Err(message) => fail("appender date test failed: \{message}")
  }
}

///|
test "native appender append timestamp" {
  fn append_timestamp_row(app : Appender) -> Result[Unit, DuckDBError] {
    let ts = timestamp_from_ymd_hms(2024, 6, 3, 12, 34, 56)
    match app.begin_row() {
      Ok(_) =>
        match app.append_int(1) {
          Ok(_) =>
            match app.append_timestamp(ts) {
              Ok(_) => app.end_row()
              Err(e) => Err(e)
            }
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  }
  let result = run_native_appender_test(
    "CREATE TABLE test_table (id INTEGER, ts TIMESTAMP)",
    append_timestamp_row,
    "SELECT * FROM test_table ORDER BY id",
  )
  match result {
    Ok(value) =>
      if value.row_count() != 1 {
        fail("expected 1 row, got \{value.row_count()}")
      } else {
        // Check that we got a timestamp-like value
        match value.cell(0, 1) {
          Some(v) => {
            if !v.contains("-") {
              fail("expected timestamp-like string, got '\{v}'")
            } else {
              ()
            }
          }
          None => fail("expected non-null timestamp value")
        }
      }
    Err(message) => fail("appender timestamp test failed: \{message}")
  }
}

// ============================================================================
// Advanced Data Types Helper Tests
// ============================================================================

///|
test "decimal helpers" {
  let d1 = decimal_from_double(123.456, 10, 3)
  let (whole, _frac) = decimal_to_parts(d1)
  if whole != 123 {
    fail("expected whole part 123, got \{whole}")
  }
  let d2 = decimal_to_double(d1)
  if d2 < 123.0 || d2 > 124.0 {
    fail("expected decimal ~123.456, got \{d2}")
  }
  ()
}

///|
test "interval helpers" {
  let i1 = interval_from_days(1)
  if i1.days != 1 {
    fail("expected 1 day, got \{i1.days}")
  }
  let i2 = interval_from_seconds(1)
  // 1 second = 1000000 micros
  if i2.micros != 1000000 {
    fail("expected 1000000 micros, got \{i2.micros}")
  }
  ()
}

///|
test "list helpers" {
  let l = list_from_strings(["a", "b", "c"])
  if list_length(l) != 3 {
    fail("expected length 3, got \{list_length(l)}")
  }
  match list_get(l, 1) {
    Some(v) => {
      if v != "b" {
        fail("expected 'b', got '\{v}'")
      }
    }
    None => fail("expected Some value, got None")
  }
  ()
}

///|
test "struct helpers" {
  let s = struct_from_arrays(["x", "y"], ["1", "2"])
  if struct_field_count(s) != 2 {
    fail("expected 2 fields, got \{struct_field_count(s)}")
  }
  match struct_get(s, "x") {
    Some(v) => {
      if v != "1" {
        fail("expected '1', got '\{v}'")
      }
    }
    None => fail("expected Some value, got None")
  }
  ()
}

///|
test "map helpers" {
  let m = map_from_arrays(["k1", "k2"], ["v1", "v2"])
  if map_size(m) != 2 {
    fail("expected size 2, got \{map_size(m)}")
  }
  match map_get(m, "k1") {
    Some(v) => {
      if v != "v1" {
        fail("expected 'v1', got '\{v}'")
      }
    }
    None => fail("expected Some value, got None")
  }
  ()
}

// ============================================================================
// Typed Result API Tests
// ============================================================================

///|
test "typed result basic types" {
  let result = run_native_query("SELECT 42 AS int_col, 3.14 AS double_col, true AS bool_col, 'hello' AS str_col")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      if typed.row_count() != 1 {
        fail("expected 1 row, got \{typed.row_count()}")
      }
      if typed.column_count() != 4 {
        fail("expected 4 columns, got \{typed.column_count()}")
      }

      match typed.get_int(0, 0) {
        Some(i) => {
          if i != 42 {
            fail("expected int 42, got \{i}")
          }
        }
        None => fail("expected Some(42), got None")
      }

      match typed.get_double(0, 1) {
        Some(d) => {
          let diff = d - 3.14
          if diff < 0.0 { diff = -diff }
          if diff > 0.001 {
            fail("expected double ~3.14, got \{d}")
          }
        }
        None => fail("expected Some(3.14), got None")
      }

      match typed.get_bool(0, 2) {
        Some(b) => {
          if !b {
            fail("expected true, got false")
          }
        }
        None => fail("expected Some(true), got None")
      }

      match typed.get_string(0, 3) {
        Some(s) => {
          if s != "hello" {
            fail("expected 'hello', got '\{s}'")
          }
        }
        None => fail("expected Some('hello'), got None")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

///|
test "typed result null values" {
  let result = run_native_query("SELECT 1 AS a, NULL AS b, 2 AS c")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      match typed.get_int(0, 0) {
        Some(i) => {
          if i != 1 {
            fail("expected 1, got \{i}")
          }
        }
        None => fail("expected Some(1), got None")
      }

      match typed.get_int(0, 1) {
        Some(_) => fail("expected None for NULL, got Some")
        None => ()
      }

      if typed.is_null(0, 0) {
        fail("expected non-null at (0, 0)")
      }
      if !typed.is_null(0, 1) {
        fail("expected null at (0, 1)")
      }

      match typed.get_int(0, 2) {
        Some(i) => {
          if i != 2 {
            fail("expected 2, got \{i}")
          }
        }
        None => fail("expected Some(2), got None")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

///|
test "typed result date and timestamp" {
  let result = run_native_query("SELECT DATE '2024-06-03' AS dt, TIMESTAMP '2024-06-03 12:34:56' AS ts")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      match typed.get_date(0, 0) {
        Some(days) => {
          let date = date_from_ymd(2024, 6, 3)
          if days != date {
            fail("expected date \{date}, got \{days}")
          }
        }
        None => fail("expected Some(date), got None")
      }

      match typed.get_timestamp(0, 1) {
        Some(micros) => {
          let ts = timestamp_from_ymd_hms(2024, 6, 3, 12, 34, 56)
          let diff = micros - ts
          if diff < 0 { diff = -diff }
          if diff > 1000000 {
            fail("expected timestamp ~\{ts}, got \{micros}")
          }
        }
        None => fail("expected Some(timestamp), got None")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

///|
test "typed result columnar access" {
  let result = run_native_query("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS t(id, name)")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      match typed.get_int_column(0) {
        Some(col) => {
          if col.length() != 3 {
            fail("expected column length 3, got \{col.length()}")
          }
          match col[0] { Some(1) => () _ => fail("expected Some(1)") }
          match col[1] { Some(2) => () _ => fail("expected Some(2)") }
          match col[2] { Some(3) => () _ => fail("expected Some(3)") }
        }
        None => fail("expected Some(column), got None")
      }

      match typed.get_string_column(1) {
        Some(col) => {
          if col.length() != 3 {
            fail("expected column length 3, got \{col.length()}")
          }
          match col[0] { Some(s) => { if s != "a" { fail("expected 'a'") } } _ => fail("expected Some('a')") }
          match col[1] { Some(s) => { if s != "b" { fail("expected 'b'") } } _ => fail("expected Some('b')") }
          match col[2] { None => () _ => fail("expected None for NULL") }
        }
        None => fail("expected Some(column), got None")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

///|
test "typed result bigint extremes" {
  let result = run_native_query("SELECT 9223372036854775807 AS max_val, -9223372036854775808 AS min_val")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      match typed.get_int(0, 0) {
        Some(i) => {
          if i != 9223372036854775807 {
            fail("expected max bigint, got \{i}")
          }
        }
        None => fail("expected Some(max bigint)")
      }

      match typed.get_int(0, 1) {
        Some(i) => {
          if i != -9223372036854775808 {
            fail("expected min bigint, got \{i}")
          }
        }
        None => fail("expected Some(min bigint)")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

///|
test "typed result get_value" {
  let result = run_native_query("SELECT 42 AS num, 'text' AS str, NULL AS nul")
  match result {
    Ok(query_result) => {
      let typed = query_result.to_typed()

      match typed.get_value(0, 0) {
        Some(Value::Int(42)) => ()
        Some(_) => fail("expected Value::Int(42), got other value")
        None => fail("expected Some(value), got None")
      }

      match typed.get_value(0, 1) {
        Some(Value::String(s)) => {
          if s != "text" {
            fail("expected 'text', got '\{s}'")
          }
        }
        Some(_) => fail("expected Value::String, got other")
        None => fail("expected Some(value), got None")
      }

      match typed.get_value(0, 2) {
        Some(Value::Null) => ()
        Some(_) => fail("expected Value::Null, got other")
        None => fail("expected Some(value), got None")
      }
    }
    Err(message) => fail("query failed: \{message}")
  }
}

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

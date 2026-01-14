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
  let result = run_native_prepare_query("SELECT ? + ? + ? AS x", fn(stmt) {
    match stmt.bind_int(1, 10) {
      Ok(_) =>
        match stmt.bind_int(2, 20) {
          Ok(_) => stmt.bind_int(3, 30)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  })
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
  let result = run_native_prepare_query("SELECT ? AS x UNION ALL SELECT ? UNION ALL SELECT ?", fn(stmt) {
    match stmt.bind_int(1, 1) {
      Ok(_) =>
        match stmt.bind_int(2, 2) {
          Ok(_) => stmt.bind_int(3, 3)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  })
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
  let result = run_native_prepare_query("SELECT ? AS x", fn(stmt) {
    match stmt.bind_int(1, 42) {
      Ok(_) =>
        match stmt.clear_bindings() {
          Ok(_) => stmt.bind_null(1)
          Err(e) => Err(e)
        }
      Err(e) => Err(e)
    }
  })
  match result {
    Ok(value) =>
      match value.cell(0, 0) {
        None => () // Expected to be null after clear_bindings + bind_null
        Some(v) => fail("expected null after clear_bindings, got '\{v}'")
      }
    Err(message) => fail("clear bindings failed: \{message}")
  }
}

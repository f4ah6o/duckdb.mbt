// ============================================================================
// Arrow Integration Tests
// ============================================================================

// Helper to run Arrow query synchronously
fn run_native_arrow_query(sql : String) -> Result[ArrowResult, String] {
  let result_ref : Ref[ArrowResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(conn_result) {
    match conn_result {
      Ok(conn) => {
        conn.query_arrow(sql, on_done=fn(arrow_result) {
          match arrow_result {
            Ok(result) => result_ref.val = Some(result)
            Err(DuckDBError::Message(message)) =>
              error_ref.val = Some("arrow query failed: \{message}")
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
        None => Err("arrow query returned no result")
      }
  }
}

///|
test "native arrow query basic" {
  let result = run_native_arrow_query("SELECT 1 AS x, 2 AS y")
  match result {
    Ok(result) => {
      let cc = result.column_count()
      let rc = result.row_count()
      result.close(on_done=fn(_) { () })
      if cc != 2 {
        fail("expected 2 columns, got \{cc}")
      } else if rc != 1 {
        fail("expected 1 row, got \{rc}")
      } else {
        ()
      }
    }
    Err(message) => fail("arrow query failed: \{message}")
  }
}

///|
test "native arrow query multiple rows" {
  let result = run_native_arrow_query("SELECT * FROM RANGE(10)")
  match result {
    Ok(result) => {
      let cc = result.column_count()
      let rc = result.row_count()
      result.close(on_done=fn(_) { () })
      if cc != 1 {
        fail("expected 1 column, got \{cc}")
      } else if rc != 10 {
        fail("expected 10 rows, got \{rc}")
      } else {
        ()
      }
    }
    Err(message) => fail("arrow query failed: \{message}")
  }
}

///|
test "native arrow query with types" {
  let result = run_native_arrow_query("SELECT 42::INTEGER AS i, 3.14::DOUBLE AS d, true::BOOLEAN AS b, 'hello'::VARCHAR AS s")
  match result {
    Ok(result) => {
      let cc = result.column_count()
      result.close(on_done=fn(_) { () })
      if cc != 4 {
        fail("expected 4 columns, got \{cc}")
      } else {
        ()
      }
    }
    Err(message) => fail("arrow query failed: \{message}")
  }
}

///|
test "native arrow query empty result" {
  let result = run_native_arrow_query("SELECT * FROM RANGE(0)")
  match result {
    Ok(result) => {
      let cc = result.column_count()
      let rc = result.row_count()
      result.close(on_done=fn(_) { () })
      if cc != 1 {
        fail("expected 1 column, got \{cc}")
      } else if rc != 0 {
        fail("expected 0 rows, got \{rc}")
      } else {
        ()
      }
    }
    Err(message) => fail("arrow query failed: \{message}")
  }
}

///|
test "native arrow query error" {
  let result = run_native_arrow_query("SELECT * FROM nonexistent_table")
  match result {
    Ok(_) => fail("query should have failed")
    Err(_) => { () }  // Expected error
  }
}

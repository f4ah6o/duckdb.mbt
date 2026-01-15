// ============================================================================
// Arrow Integration Tests
// ============================================================================

// Helper to run Arrow query synchronously

///|
fn run_native_arrow_query(sql : String) -> Result[ArrowResult, String] {
  let result_ref : Ref[ArrowResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(conn_result) {
    match conn_result {
      Ok(conn) =>
        conn.query_arrow(sql, on_done=fn(arrow_result) {
          match arrow_result {
            Ok(result) => result_ref.val = Some(result)
            Err(DuckDBError::Message(message)) =>
              error_ref.val = Some("arrow query failed: \{message}")
          }
        })
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
  let result = run_native_arrow_query(
    "SELECT 42::INTEGER AS i, 3.14::DOUBLE AS d, true::BOOLEAN AS b, 'hello'::VARCHAR AS s",
  )
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
    Err(_) => ()
  } // Expected error
}

// ============================================================================
// Schema Extraction Tests
// ============================================================================

///|
test "native arrow schema extraction" {
  let result = run_native_arrow_query(
    "SELECT 42::INTEGER AS a, 3.14::DOUBLE AS b",
  )
  match result {
    Ok(result) => {
      match result.get_schema() {
        Ok(schema) => {
          let fields = schema.fields
          if fields.length() != 2 {
            fail("expected 2 fields, got \{fields.length()}")
          } else {
            let f0 = fields[0]
            if f0.name != "a" {
              fail("expected field name 'a', got '\{f0.name}'")
            } else if f0.type_id != "int32" {
              fail("expected type_id 'int32', got '\{f0.type_id}'")
            } else {
              let f1 = fields[1]
              if f1.name != "b" {
                fail("expected field name 'b', got '\{f1.name}'")
              } else if f1.type_id != "double" {
                fail("expected type_id 'double', got '\{f1.type_id}'")
              } else {
                ()
              }
            }
          }
        }
        Err(err) =>
          match err {
            Message(msg) => fail("schema failed: \{msg}")
          }
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow schema multiple types" {
  let result = run_native_arrow_query(
    "SELECT 1::INTEGER AS i, 2::BIGINT AS bi, 3.0::DOUBLE AS d, true::BOOLEAN AS b, 'text'::VARCHAR AS s",
  )
  match result {
    Ok(result) => {
      match result.get_schema() {
        Ok(schema) => {
          let fields = schema.fields
          if fields.length() != 5 {
            fail("expected 5 fields, got \{fields.length()}")
          } else if fields[0].type_id != "int32" {
            fail("expected int32, got \{fields[0].type_id}")
          } else if fields[1].type_id != "int64" {
            fail("expected int64, got \{fields[1].type_id}")
          } else if fields[2].type_id != "double" {
            fail("expected double, got \{fields[2].type_id}")
          } else if fields[3].type_id != "bool" {
            fail("expected bool, got \{fields[3].type_id}")
          } else if fields[4].type_id != "string" {
            fail("expected string, got \{fields[4].type_id}")
          } else {
            ()
          }
        }
        Err(err) =>
          match err {
            Message(msg) => fail("schema failed: \{msg}")
          }
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Column Data Extraction Tests
// ============================================================================

///|
test "native arrow int32 column data" {
  let result = run_native_arrow_query("SELECT * FROM RANGE(5)")
  match result {
    Ok(result) => {
      let values = result.get_column_int32(0)
      if values.length() != 5 {
        fail("expected 5 values, got \{values.length()}")
      } else if values[0] != 0 {
        fail("expected values[0] = 0, got \{values[0]}")
      } else if values[4] != 4 {
        fail("expected values[4] = 4, got \{values[4]}")
      } else {
        ()
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow int64 column data" {
  let result = run_native_arrow_query("SELECT 100::BIGINT AS x")
  match result {
    Ok(result) => {
      let values = result.get_column_int64(0)
      if values.length() != 1 {
        fail("expected 1 value, got \{values.length()}")
      } else if values[0] != 100 {
        fail("expected 100, got \{values[0]}")
      } else {
        ()
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow double column data" {
  let result = run_native_arrow_query("SELECT 3.14::DOUBLE AS x")
  match result {
    Ok(result) => {
      let values = result.get_column_double(0)
      if values.length() != 1 {
        fail("expected 1 value, got \{values.length()}")
      } else {
        let v = values[0]
        if v < 3.13 || v > 3.15 {
          fail("expected ~3.14, got \{v}")
        } else {
          ()
        }
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow bool column data" {
  let result = run_native_arrow_query(
    "SELECT true::BOOLEAN AS t, false::BOOLEAN AS f",
  )
  match result {
    Ok(result) => {
      // First column should be true
      let t_values = result.get_column_bool(0)
      if t_values.length() != 1 {
        fail("expected 1 value in column 0, got \{t_values.length()}")
      } else if t_values[0] != true {
        fail("expected true, got \{t_values[0]}")
      }
      // Second column should be false
      let f_values = result.get_column_bool(1)
      if f_values.length() != 1 {
        fail("expected 1 value in column 1, got \{f_values.length()}")
      } else if f_values[0] != false {
        fail("expected false, got \{f_values[0]}")
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow string column data" {
  let result = run_native_arrow_query("SELECT 'hello'::VARCHAR AS s")
  match result {
    Ok(result) => {
      let values = result.get_column_string(0)
      if values.length() != 1 {
        fail("expected 1 value, got \{values.length()}")
      } else if values[0] != "hello" {
        fail("expected 'hello', got '\{values[0]}'")
      } else {
        ()
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow multiple rows int32" {
  let result = run_native_arrow_query("SELECT * FROM RANGE(100)")
  match result {
    Ok(result) => {
      let values = result.get_column_int32(0)
      if values.length() != 100 {
        fail("expected 100 values, got \{values.length()}")
      } else if values[0] != 0 {
        fail("expected 0, got \{values[0]}")
      } else if values[99] != 99 {
        fail("expected 99, got \{values[99]}")
      } else {
        ()
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Null Handling Tests
// ============================================================================

///|
test "native arrow int32 nullable with nulls" {
  let result = run_native_arrow_query("SELECT 1::INTEGER UNION ALL SELECT NULL::INTEGER UNION ALL SELECT 3::INTEGER UNION ALL SELECT NULL::INTEGER UNION ALL SELECT 5::INTEGER")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_int32_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 5 {
        fail("expected 5 values, got \{values.length()}")
      } else if validity.length() != 5 {
        fail("expected 5 validity, got \{validity.length()}")
      } else if validity[0] != true {
        fail("expected validity[0] = true, got false")
      } else if validity[1] != false {
        fail("expected validity[1] = false, got true")
      } else if validity[2] != true {
        fail("expected validity[2] = true, got false")
      } else if validity[3] != false {
        fail("expected validity[3] = false, got true")
      } else if validity[4] != true {
        fail("expected validity[4] = true, got false")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow int32 nullable all nulls" {
  let result = run_native_arrow_query("SELECT NULL::INTEGER UNION ALL SELECT NULL::INTEGER UNION ALL SELECT NULL::INTEGER")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_int32_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 3 {
        fail("expected 3 values, got \{values.length()}")
      } else if validity.length() != 3 {
        fail("expected 3 validity, got \{validity.length()}")
      } else if validity[0] != false {
        fail("expected validity[0] = false, got true")
      } else if validity[1] != false {
        fail("expected validity[1] = false, got true")
      } else if validity[2] != false {
        fail("expected validity[2] = false, got true")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow int32 nullable no nulls" {
  let result = run_native_arrow_query("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_int32_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 3 {
        fail("expected 3 values, got \{values.length()}")
      } else if validity.length() != 3 {
        fail("expected 3 validity, got \{validity.length()}")
      } else if validity[0] != true {
        fail("expected validity[0] = true, got false")
      } else if validity[1] != true {
        fail("expected validity[1] = true, got false")
      } else if validity[2] != true {
        fail("expected validity[2] = true, got false")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow string nullable with nulls" {
  let result = run_native_arrow_query("SELECT 'a' UNION ALL SELECT NULL UNION ALL SELECT 'c' UNION ALL SELECT NULL UNION ALL SELECT 'e'")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_string_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 5 {
        fail("expected 5 values, got \{values.length()}")
      } else if validity.length() != 5 {
        fail("expected 5 validity, got \{validity.length()}")
      } else if validity[0] != true {
        fail("expected validity[0] = true")
      } else if validity[1] != false {
        fail("expected validity[1] = false")
      } else if validity[2] != true {
        fail("expected validity[2] = true")
      } else if validity[3] != false {
        fail("expected validity[3] = false")
      } else if validity[4] != true {
        fail("expected validity[4] = true")
      } else if values[0] != "a" {
        fail("expected 'a', got '\{values[0]}'")
      } else if values[2] != "c" {
        fail("expected 'c', got '\{values[2]}'")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow double nullable with nulls" {
  let result = run_native_arrow_query("SELECT 1.5::DOUBLE UNION ALL SELECT NULL::DOUBLE UNION ALL SELECT 3.14::DOUBLE")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_double_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 3 {
        fail("expected 3 values, got \{values.length()}")
      } else if validity.length() != 3 {
        fail("expected 3 validity, got \{validity.length()}")
      } else if validity[0] != true {
        fail("expected validity[0] = true")
      } else if validity[1] != false {
        fail("expected validity[1] = false")
      } else if validity[2] != true {
        fail("expected validity[2] = true")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow bool nullable with nulls" {
  let result = run_native_arrow_query("SELECT true::BOOLEAN UNION ALL SELECT NULL::BOOLEAN UNION ALL SELECT false::BOOLEAN")
  match result {
    Ok(result) => {
      let (values, validity) = result.get_column_bool_nullable(0)
      result.close(on_done=fn(_) { () })
      if values.length() != 3 {
        fail("expected 3 values, got \{values.length()}")
      } else if validity.length() != 3 {
        fail("expected 3 validity, got \{validity.length()}")
      } else if validity[0] != true {
        fail("expected validity[0] = true")
      } else if validity[1] != false {
        fail("expected validity[1] = false")
      } else if validity[2] != true {
        fail("expected validity[2] = true")
      } else if values[0] != true {
        fail("expected true, got \{values[0]}")
      } else if values[2] != false {
        fail("expected false, got \{values[2]}")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

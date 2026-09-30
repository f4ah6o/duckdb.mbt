// ============================================================================
// Arrow Integration Tests (issue #62: canonical typed vectors)
// ============================================================================

// Helper to run an Arrow query synchronously

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
            Err(err) =>
              error_ref.val = Some("arrow query failed: \{err.message()}")
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
        None => Err("arrow query returned no result")
      }
  }
}

///|
/// Materialized chunks of `result` or a test failure.
fn arrow_chunks_or_fail(result : ArrowResult) -> Array[VectorChunk] raise {
  match result.to_chunks() {
    Ok(chunks) => chunks
    Err(err) => fail("to_chunks failed: \{err.message()}")
  }
}

///|
/// Concatenate `ints()` payloads of column `col` across all chunks.
fn arrow_column_ints(
  chunks : Array[VectorChunk],
  col : Int,
) -> Array[Int] raise {
  let out : Array[Int] = []
  for chunk in chunks {
    match chunk.column(col) {
      Some(vector) =>
        match vector.ints() {
          Some(values) =>
            for v in values {
              out.push(v)
            } nobreak {
              ()
            }
          None => fail("column \{col} is not an int vector")
        }
      None => fail("missing column \{col}")
    }
  } nobreak {
    ()
  }
  out
}

///|
/// Concatenate `int64s()` payloads of column `col` across all chunks.
fn arrow_column_int64s(
  chunks : Array[VectorChunk],
  col : Int,
) -> Array[Int64] raise {
  let out : Array[Int64] = []
  for chunk in chunks {
    match chunk.column(col) {
      Some(vector) =>
        match vector.int64s() {
          Some(values) =>
            for v in values {
              out.push(v)
            } nobreak {
              ()
            }
          None => fail("column \{col} is not an int64 vector")
        }
      None => fail("missing column \{col}")
    }
  } nobreak {
    ()
  }
  out
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
    Err(err) => fail("arrow query failed: \{err}")
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
    Err(err) => fail("arrow query failed: \{err}")
  }
}

///|
test "native arrow query empty result" {
  let result = run_native_arrow_query("SELECT * FROM RANGE(0)")
  match result {
    Ok(result) => {
      let cc = result.column_count()
      let rc = result.row_count()
      let schema_ok = match result.get_schema() {
        Ok(schema) => schema.fields.length() == 1
        Err(_) => false
      }
      result.close(on_done=fn(_) { () })
      if cc != 1 {
        fail("expected 1 column, got \{cc}")
      } else if rc != 0 {
        fail("expected 0 rows, got \{rc}")
      } else if !schema_ok {
        fail("schema should still describe the empty result")
      } else {
        ()
      }
    }
    Err(err) => fail("arrow query failed: \{err}")
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
        Err(err) => fail("schema failed: \{err.message()}")
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
        Err(err) => fail("schema failed: \{err.message()}")
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow schema decimal and list" {
  let result = run_native_arrow_query(
    "SELECT 12.34::DECIMAL(10, 2) AS amount, [1, 2, 3]::INTEGER[] AS xs",
  )
  match result {
    Ok(result) => {
      match result.get_schema() {
        Ok(schema) => {
          let fields = schema.fields
          if fields.length() != 2 {
            fail("expected 2 fields, got \{fields.length()}")
          } else if fields[0].name != "amount" {
            fail("expected 'amount', got '\{fields[0].name}'")
          } else if fields[0].type_id != "decimal" {
            fail("expected type_id 'decimal', got '\{fields[0].type_id}'")
          } else if fields[1].type_id != "list" {
            fail("expected type_id 'list', got '\{fields[1].type_id}'")
          } else {
            ()
          }
        }
        Err(err) => fail("schema failed: \{err.message()}")
      }
      result.close(on_done=fn(_) { () })
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Typed Column Data Tests (canonical Vector accessors, no JSON/byte packs)
// ============================================================================

///|
test "native arrow int32 column data" {
  let result = run_native_arrow_query("SELECT unnest(range(5))::INTEGER AS v")
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let values = arrow_column_ints(chunks, 0)
      result.close(on_done=fn(_) { () })
      if values.length() != 5 {
        fail("expected 5 values, got \{values.length()}")
      } else if values[0] != 0 {
        fail("expected values[0] = 0, got \{values[0]}")
      } else if values[4] != 4 {
        fail("expected values[4] = 4, got \{values[4]}")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow int64 column data" {
  let result = run_native_arrow_query("SELECT 100::BIGINT AS x")
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let values = arrow_column_int64s(chunks, 0)
      result.close(on_done=fn(_) { () })
      if values.length() != 1 {
        fail("expected 1 value, got \{values.length()}")
      } else if values[0] != 100L {
        fail("expected 100, got \{values[0]}")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow int64 keeps full precision" {
  let result = run_native_arrow_query(
    "SELECT 4611686018427387904::BIGINT AS big, -9223372036854775807::BIGINT AS neg",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let bigs = arrow_column_int64s(chunks, 0)
      let negs = arrow_column_int64s(chunks, 1)
      result.close(on_done=fn(_) { () })
      if bigs.length() != 1 || negs.length() != 1 {
        fail("expected 1 value per column")
      } else if bigs[0] != 4611686018427387904L {
        fail("int64 precision lost: \{bigs[0]}")
      } else if negs[0] != -9223372036854775807L {
        fail("negative int64 precision lost: \{negs[0]}")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow uint64 keeps full precision" {
  let result = run_native_arrow_query(
    "SELECT 18446744073709551615::UBIGINT AS max_u64",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let vector = chunks[0].column(0)
      let value = match vector {
        Some(v) =>
          match v.uint64s() {
            Some(values) => values[0]
            None => fail("column 0 is not a uint64 vector")
          }
        None => fail("missing column 0")
      }
      result.close(on_done=fn(_) { () })
      if value != 18446744073709551615UL {
        fail("uint64 precision lost: \{value}")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow double column data" {
  let result = run_native_arrow_query("SELECT 3.14::DOUBLE AS x")
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let vector = chunks[0].column(0)
      let value = match vector {
        Some(v) =>
          match v.doubles() {
            Some(values) => values[0]
            None => fail("column 0 is not a double vector")
          }
        None => fail("missing column 0")
      }
      result.close(on_done=fn(_) { () })
      if value < 3.13 || value > 3.15 {
        fail("expected ~3.14, got \{value}")
      }
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
      let chunks = arrow_chunks_or_fail(result)
      let bools : Array[Bool] = []
      for col = 0; col < 2; col = col + 1 {
        match chunks[0].column(col) {
          Some(vector) =>
            match vector.bools() {
              Some(values) => bools.push(values[0])
              None => fail("column \{col} is not a bool vector")
            }
          None => fail("missing column \{col}")
        }
      } nobreak {
        ()
      }
      result.close(on_done=fn(_) { () })
      if bools.length() != 2 || bools[0] != true || bools[1] != false {
        fail("expected [true, false], got \{bools}")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow string column data" {
  let result = run_native_arrow_query("SELECT 'hello'::VARCHAR AS s")
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let value = match chunks[0].column(0) {
        Some(vector) =>
          match vector.strings() {
            Some(values) => values[0]
            None => fail("column 0 is not a string vector")
          }
        None => fail("missing column 0")
      }
      result.close(on_done=fn(_) { () })
      if value != "hello" {
        fail("expected 'hello', got '\{value}'")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow multi-chunk result" {
  // RANGE() produces BIGINT, so the column materializes as an Int64 vector.
  let result = run_native_arrow_query("SELECT * FROM RANGE(3000)")
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let rc = result.row_count()
      let values = arrow_column_int64s(chunks, 0)
      result.close(on_done=fn(_) { () })
      if rc != 3000 {
        fail("expected 3000 rows, got \{rc}")
      } else if values.length() != 3000 {
        fail("expected 3000 values, got \{values.length()}")
      } else if values[2047] != 2047L || values[2048] != 2048L {
        // Crosses DuckDB's STANDARD_VECTOR_SIZE chunk boundary.
        fail("multi-chunk boundary values wrong")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Decimal + nested-type coverage (issue #62 acceptance criteria)
// ============================================================================

///|
test "native arrow decimal column" {
  let result = run_native_arrow_query(
    "SELECT 12.34::DECIMAL(10, 2) AS amount, -0.01::DECIMAL(4, 2) AS small",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let ok = match chunks[0].column(0) {
        Some(vector) =>
          match vector.decimals() {
            Some(decimals) => {
              let d = decimals[0]
              d.width == 10 && d.scale == 2 && vector.string_at(0) == "12.34"
            }
            None => false
          }
        None => false
      }
      let small_ok = match chunks[0].column(1) {
        Some(vector) => vector.string_at(0) == "-0.01"
        None => false
      }
      result.close(on_done=fn(_) { () })
      if !ok {
        fail("decimal vector or value wrong")
      } else if !small_ok {
        fail("negative decimal wrong")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow list column (nested)" {
  let result = run_native_arrow_query(
    "SELECT [1, 2, 3]::INTEGER[] AS xs, []::INTEGER[] AS empty",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let ok = match chunks[0].column(0) {
        Some(vector) =>
          match vector.list_parts() {
            Some((child, offsets, lengths)) => {
              let child_values = match child.ints() {
                Some(values) =>
                  values.length() == 3 &&
                  values[0] == 1 &&
                  values[1] == 2 &&
                  values[2] == 3
                None => false
              }
              child_values && offsets[0] == 0UL && lengths[0] == 3UL
            }
            None => false
          }
        None => false
      }
      // Second column decodes as a nested `Value::List` via value_at.
      let nested_ok = match chunks[0].column(0) {
        Some(vector) =>
          match vector.value_at(0) {
            Value::List(items) => items.length() == 3
            _ => false
          }
        None => false
      }
      result.close(on_done=fn(_) { () })
      if !ok {
        fail("list vector parts wrong")
      } else if !nested_ok {
        fail("list value_at decode wrong")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow struct column (nested)" {
  let result = run_native_arrow_query(
    "SELECT {'a': 7, 'b': 'x'}::STRUCT(a INTEGER, b VARCHAR) AS s",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let ok = match chunks[0].column(0) {
        Some(vector) =>
          match vector.struct_parts() {
            Some((fields, children)) => {
              let names_ok = fields == ["a", "b"]
              let a_ok = match children[0].ints() {
                Some(values) => values[0] == 7
                None => false
              }
              let b_ok = match children[1].strings() {
                Some(values) => values[0] == "x"
                None => false
              }
              names_ok && a_ok && b_ok
            }
            None => false
          }
        None => false
      }
      result.close(on_done=fn(_) { () })
      if !ok {
        fail("struct vector parts wrong")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Null Mask Tests (canonical validity, no byte packs)
// ============================================================================

///|
/// Validity mask of column `col` across all chunks (true = non-NULL).
fn arrow_column_validity(
  chunks : Array[VectorChunk],
  col : Int,
) -> Array[Bool] raise {
  let out : Array[Bool] = []
  for chunk in chunks {
    match chunk.column(col) {
      Some(vector) =>
        for row = 0; row < vector.len(); row = row + 1 {
          out.push(vector.validity[row])
        } nobreak {
          ()
        }
      None => fail("missing column \{col}")
    }
  } nobreak {
    ()
  }
  out
}

///|
test "native arrow int32 nulls via validity mask" {
  let result = run_native_arrow_query(
    "SELECT v FROM (VALUES (1), (NULL), (3), (NULL), (5)) t(v)",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let validity = arrow_column_validity(chunks, 0)
      let vector = chunks[0].column(0)
      let null_ok = match vector {
        Some(v) => v.is_null(1) && !v.is_null(0) && v.is_null(3)
        None => false
      }
      let values = arrow_column_ints(chunks, 0)
      result.close(on_done=fn(_) { () })
      if validity != [true, false, true, false, true] {
        fail("expected [true, false, true, false, true], got \{validity}")
      } else if !null_ok {
        fail("Vector::is_null disagrees with the validity mask")
      } else if values[0] != 1 || values[2] != 3 || values[4] != 5 {
        fail("non-null values wrong")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow all-nulls column" {
  let result = run_native_arrow_query(
    "SELECT v FROM (VALUES (NULL::INTEGER), (NULL::INTEGER), (NULL::INTEGER)) t(v)",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let validity = arrow_column_validity(chunks, 0)
      result.close(on_done=fn(_) { () })
      if validity != [false, false, false] {
        fail("expected all-invalid, got \{validity}")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

///|
test "native arrow string nulls via validity mask" {
  let result = run_native_arrow_query(
    "SELECT v FROM (VALUES ('a'), (NULL), ('c'), (NULL), ('e')) t(v)",
  )
  match result {
    Ok(result) => {
      let chunks = arrow_chunks_or_fail(result)
      let validity = arrow_column_validity(chunks, 0)
      let values = match chunks[0].column(0) {
        Some(vector) =>
          match vector.strings() {
            Some(v) => v
            None => fail("column 0 is not a string vector")
          }
        None => fail("missing column 0")
      }
      result.close(on_done=fn(_) { () })
      if validity != [true, false, true, false, true] {
        fail("expected [true, false, true, false, true], got \{validity}")
      } else if values[0] != "a" || values[2] != "c" || values[4] != "e" {
        fail("non-null strings wrong")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Ownership / Closed semantics
// ============================================================================

///|
test "native arrow closed result reports Closed" {
  let result = run_native_arrow_query("SELECT 1 AS x")
  match result {
    Ok(result) => {
      let first_close : Ref[Bool] = Ref::new(false)
      result.close(on_done=fn(res) { first_close.val = res is Ok(_) })
      let chunks_err = result.to_chunks() is Err(_)
      let schema_err = result.get_schema() is Err(_)
      let second_close : Ref[Bool] = Ref::new(false)
      result.close(on_done=fn(res) {
        match res {
          Err(DuckDBError::Closed(_)) => second_close.val = true
          _ => second_close.val = false
        }
      })
      if !first_close.val {
        fail("first close should succeed")
      } else if !chunks_err || !schema_err {
        fail("accessors should report errors after close")
      } else if !second_close.val {
        fail("second close should report Closed")
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

// ============================================================================
// Deprecated getter compat (migration path coverage)
// ============================================================================

///|
test "native arrow deprecated getters still work" {
  let result = run_native_arrow_query(
    "SELECT v FROM (VALUES (1::INTEGER), (NULL::INTEGER), (3::INTEGER)) t(v)",
  )
  match result {
    Ok(result) => {
      let flat = result.get_column_int32(0)
      let (values, validity) = result.get_column_int32_nullable(0)
      result.close(on_done=fn(_) { () })
      if flat != [1, 0, 3] {
        fail("expected [1, 0, 3], got \{flat}")
      } else if values != [1, 0, 3] {
        fail("expected nullable values [1, 0, 3], got \{values}")
      } else if validity != [true, false, true] {
        fail("expected validity [true, false, true], got \{validity}")
      } else {
        ()
      }
    }
    Err(err) => fail("query failed: \{err}")
  }
}

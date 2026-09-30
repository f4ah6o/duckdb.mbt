///|
// Tests for the canonical typed chunk/vector result representation (#61).

///|
fn run_query_chunks(sql : String) -> Result[Array[VectorChunk], String] {
  let result_ref : Ref[Array[VectorChunk]?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query_chunks(sql, on_done=fn(chunks_result) {
          match chunks_result {
            Ok(chunks) => result_ref.val = Some(chunks)
            Err(err) => error_ref.val = Some("query failed: \{err.message()}")
          }
        })
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(chunks) => Ok(chunks)
        None => Err("query_chunks returned no result")
      }
  }
}

///|
fn run_query_result(sql : String) -> Result[QueryResult, String] {
  let result_ref : Ref[QueryResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query(sql, on_done=fn(query_result) {
          match query_result {
            Ok(res) => result_ref.val = Some(res)
            Err(err) => error_ref.val = Some("query failed: \{err.message()}")
          }
        })
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(res) => Ok(res)
        None => Err("query returned no result")
      }
  }
}

///|
/// Prepare `sql`, execute it via `PreparedStatement::execute_chunks`, and
/// close the statement.
fn run_execute_chunks(sql : String) -> Result[Array[VectorChunk], String] {
  let result_ref : Ref[Array[VectorChunk]?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.prepare(sql, on_done=fn(stmt_result) {
          match stmt_result {
            Ok(stmt) => {
              stmt.execute_chunks(on_done=fn(chunks_result) {
                match chunks_result {
                  Ok(chunks) => result_ref.val = Some(chunks)
                  Err(err) =>
                    error_ref.val = Some("execute failed: \{err.message()}")
                }
              })
              stmt.close(on_done=fn(_) { () })
            }
            Err(err) => error_ref.val = Some("prepare failed: \{err.message()}")
          }
        })
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None =>
      match result_ref.val {
        Some(chunks) => Ok(chunks)
        None => Err("execute_chunks returned no result")
      }
  }
}

///|
/// Drain `ResultStream::next_chunk`, labeling each call "some", "none", or
/// "err", until the stream ends or fails.
fn run_stream_outcomes(sql : String) -> Result[Array[String], String] {
  let outcomes_ref : Ref[Array[String]] = Ref::new([])
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query_stream(sql, on_done=fn(stream_result) {
          match stream_result {
            Ok(stream) => {
              let done_ref : Ref[Bool] = Ref::new(false)
              while !done_ref.val {
                stream.next_chunk(on_done=fn(chunk_result) {
                  match chunk_result {
                    Ok(Some(_)) => outcomes_ref.val.push("some")
                    Ok(None) => {
                      outcomes_ref.val.push("none")
                      done_ref.val = true
                    }
                    Err(_) => {
                      outcomes_ref.val.push("err")
                      done_ref.val = true
                    }
                  }
                })
              }
              stream.close(on_done=fn(_) { () })
            }
            Err(err) => error_ref.val = Some("stream failed: \{err.message()}")
          }
        })
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => Err(message)
    None => Ok(outcomes_ref.val)
  }
}

///|
/// First chunk convenience for tests asserting on single-result queries.
fn first_chunk(chunks : Array[VectorChunk]) -> VectorChunk {
  if chunks.length() == 0 {
    { columns: [], vectors: [], }
  } else {
    chunks[0]
  }
}

///|
/// Collect every (row, col) cell of all chunks via `get_value`.
fn collect_values(chunks : Array[VectorChunk], col : Int) -> Array[Value] {
  let out : Array[Value] = []
  for chunk in chunks {
    for row in 0..<chunk.row_count() {
      match chunk.get_value(row, col) {
        Some(value) => out.push(value)
        None => out.push(Value::Null)
      }
    }
  }
  out
}

///|
test "query_chunks returns typed vectors for primitives" {
  match
    run_query_chunks(
      "SELECT 42::INTEGER AS i, 7::BIGINT AS b, 1.5::DOUBLE AS d, true AS flag, 'hello' AS s",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.column_count(), content="5")
      inspect(chunk.row_count(), content="1")
      inspect(chunk.columns, content="[i, b, d, flag, s]")
      match chunk.column_type(0) {
        ColumnType::Integer => ()
        other => fail("expected Integer column type")
      }
      match chunk.column_type(1) {
        ColumnType::BigInt => ()
        other => fail("expected BigInt column type")
      }
      match chunk.column_type(2) {
        ColumnType::Double => ()
        other => fail("expected Double column type")
      }
      match chunk.get_value(0, 0) {
        Some(Value::Int(42)) => ()
        other => fail("expected Int(42)")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Int64(7L)) => ()
        other => fail("expected Int64(7)")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Double(d)) => if d != 1.5 { fail("expected 1.5, got \{d}") }
        other => fail("expected Double(1.5)")
      }
      match chunk.get_value(0, 3) {
        Some(Value::Bool(true)) => ()
        other => fail("expected Bool(true)")
      }
      match chunk.get_value(0, 4) {
        Some(Value::String("hello")) => ()
        other => fail("expected String(hello)")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks integer column exposes raw int array" {
  match run_query_chunks("SELECT i::INTEGER FROM range(5) tbl(i)") {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.row_count(), content="5")
      match chunk.column(0) {
        Some(vector) =>
          match vector.ints() {
            Some(values) => {
              let collected : Array[Int] = []
              for i in 0..<values.length() {
                collected.push(values[i])
              }
              inspect(collected, content="[0, 1, 2, 3, 4]")
            }
            None => fail("expected Int32 vector")
          }
        None => fail("missing column 0")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks tracks nulls via validity" {
  match
    run_query_chunks(
      "SELECT * FROM (VALUES (1), (NULL), (3)) t(v) ORDER BY v IS NULL, v",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.row_count(), content="3")
      // Values are sorted as 1, 3, NULL.
      inspect(chunk.is_null(0, 0), content="false")
      inspect(chunk.is_null(1, 0), content="false")
      inspect(chunk.is_null(2, 0), content="true")
      match chunk.get_value(2, 0) {
        None => ()
        other => fail("expected None for NULL cell")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks handles multiple chunks" {
  // Force several chunks: standard vector size is 2048.
  match run_query_chunks("SELECT i::INTEGER FROM range(5000) tbl(i)") {
    Ok(chunks) => {
      inspect(
        chunks.fold(init=0, fn(acc, c) { acc + c.row_count() }),
        content="5000",
      )
      let values = collect_values(chunks, 0)
      match values[4999] {
        Value::Int(4999) => ()
        _ => fail("expected Int(4999)")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks decodes decimal and interval typed" {
  match
    run_query_chunks(
      "SELECT CAST(123.456 AS DECIMAL(10,3)) AS d, INTERVAL '1 year 2 months 3 days 04:05:06.789' AS iv",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      match chunk.get_value(0, 0) {
        Some(Value::Decimal(dec)) => {
          inspect(dec.width, content="10")
          inspect(dec.scale, content="3")
          inspect(dec.lower.to_string(), content="123456")
          inspect(dec.upper, content="0")
        }
        other => fail("expected Decimal")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Interval(iv)) => {
          inspect(iv.months, content="14")
          inspect(iv.days, content="3")
          inspect(iv.micros, content="14706789000")
        }
        other => fail("expected Interval")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks decodes date/time/timestamp" {
  match
    run_query_chunks(
      "SELECT DATE '2024-06-03' AS d, TIME '12:34:56.789' AS t, TIMESTAMP '2024-06-03 12:34:56.789' AS ts",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      match chunk.get_value(0, 0) {
        Some(Value::Date(days)) =>
          inspect(days, content="\{date_from_ymd(2024, 6, 3)}")
        other => fail("expected Date")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Int64(micros)) => inspect(micros, content="45296789000")
        other => fail("expected Int64 time micros")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Timestamp(micros)) =>
          inspect(
            micros,
            content="\{timestamp_from_ymd_hms(2024, 6, 3, 12, 34, 56) + 789000L}",
          )
        other => fail("expected Timestamp")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks timestamp_ns keeps nanoseconds typed" {
  match
    run_query_chunks(
      "SELECT TIMESTAMP_NS '1969-12-31 23:59:59.999999999' AS pre, TIMESTAMP_NS '2024-01-01 00:00:00.123456789' AS post",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      match chunk.get_value(0, 0) {
        Some(Value::TimestampNs(nanos)) => inspect(nanos, content="-1")
        _ => fail("expected TimestampNs")
      }
      match chunk.get_value(0, 1) {
        Some(Value::TimestampNs(nanos)) =>
          inspect(nanos, content="1704067200123456789")
        _ => fail("expected TimestampNs")
      }
      // Compat rendering must match duckdb_value_varchar output, including
      // pre-epoch nanosecond fractions.
      match
        run_query_result(
          "SELECT TIMESTAMP_NS '1969-12-31 23:59:59.999999999' AS pre, TIMESTAMP_NS '2024-01-01 00:00:00.123456789' AS post",
        ) {
        Ok(qr) => {
          let vector = chunk.column(0).unwrap()
          inspect(vector.string_at(0), content="\{qr.rows[0][0]}")
          let vector2 = chunk.column(1).unwrap()
          inspect(vector2.string_at(0), content="\{qr.rows[0][1]}")
        }
        Err(err) => fail(err)
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks decodes nested list/struct/map without strings" {
  match
    run_query_chunks(
      "SELECT [1, 2, 3]::INTEGER[] AS l, {'a': 1, 'b': 2} AS st, MAP {'x': 10, 'y': 20} AS m",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      match chunk.get_value(0, 0) {
        Some(Value::List(items)) =>
          inspect(items.map(fn(v) { v.to_string() }), content="[1, 2, 3]")
        other => fail("expected List")
      }
      match chunk.get_value(0, 1) {
        Some(Value::Struct(fields~, values~)) => {
          inspect(fields, content="[a, b]")
          inspect(values.map(fn(v) { v.to_string() }), content="[1, 2]")
        }
        other => fail("expected Struct")
      }
      match chunk.get_value(0, 2) {
        Some(Value::Map(keys~, values~)) => {
          inspect(keys.map(fn(v) { v.to_string() }), content="[x, y]")
          inspect(values.map(fn(v) { v.to_string() }), content="[10, 20]")
        }
        other => fail("expected Map")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks hugeint and uuid typed" {
  match
    run_query_chunks(
      "SELECT 170141183460469231731687303715884105727::HUGEINT AS h, '550e8400-e29b-41d4-a716-446655440000'::UUID AS u",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      match chunk.get_value(0, 0) {
        Some(Value::HugeInt(lower~, upper~)) => {
          inspect(upper, content="9223372036854775807")
          inspect(lower, content="18446744073709551615")
        }
        other => fail("expected HugeInt")
      }
      match chunk.column(1) {
        Some(vector) =>
          match vector.logical_type {
            ColumnType::Uuid =>
              inspect(
                vector.string_at(0),
                content="550e8400-e29b-41d4-a716-446655440000",
              )
            other => fail("expected Uuid column type")
          }
        None => fail("missing column 1")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks compat round-trip matches query" {
  let sql = "SELECT i::INTEGER AS i, (i * 1.5)::DOUBLE AS d, 'row' || i AS s FROM range(10) tbl(i)"
  match (run_query_chunks(sql), run_query_result(sql)) {
    (Ok(chunks), Ok(compat)) => {
      let via_facade = chunks_to_query_result(chunks)
      assert_eq(via_facade.columns, compat.columns)
      assert_eq(via_facade.rows, compat.rows)
      assert_eq(via_facade.nulls, compat.nulls)
      assert_true(via_facade.column_types == compat.column_types)
    }
    (Err(err), _) | (_, Err(err)) => fail(err)
    _ => fail("expected Ok results")
  }
}

///|
test "ResultStream::next_chunk yields typed chunks" {
  let chunks_ref : Ref[Array[VectorChunk]] = Ref::new([])
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query_stream("SELECT i::INTEGER AS i FROM range(7) tbl(i)", on_done=fn(
          stream_result,
        ) {
          match stream_result {
            Ok(stream) => {
              let done_ref : Ref[Bool] = Ref::new(false)
              while !done_ref.val {
                stream.next_chunk(on_done=fn(chunk_result) {
                  match chunk_result {
                    Ok(Some(chunk)) => chunks_ref.val.push(chunk)
                    Ok(None) => done_ref.val = true
                    Err(err) => {
                      error_ref.val = Some("stream failed: \{err.message()}")
                      done_ref.val = true
                    }
                  }
                })
              }
              stream.close(on_done=fn(_) { () })
            }
            Err(err) => error_ref.val = Some("stream failed: \{err.message()}")
          }
        })
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => fail(message)
    None => {
      let chunk = first_chunk(chunks_ref.val)
      inspect(chunk.row_count(), content="7")
      inspect(chunk.columns, content="[i]")
      match chunk.column(0) {
        Some(vector) =>
          match vector.ints() {
            Some(values) => {
              let collected : Array[Int] = []
              for i in 0..<values.length() {
                collected.push(values[i])
              }
              inspect(collected, content="[0, 1, 2, 3, 4, 5, 6]")
            }
            None => fail("expected Int32 vector")
          }
        None => fail("missing column 0")
      }
    }
  }
}

///|
test "ResultStream::next compat still works" {
  let counts_ref : Ref[Array[Int]] = Ref::new([])
  let rows_ref : Ref[Array[String]] = Ref::new([])
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query_stream(
          "SELECT 'abc' AS s, i::INTEGER FROM range(3) tbl(i)",
          on_done=fn(stream_result) {
            match stream_result {
              Ok(stream) => {
                let done_ref : Ref[Bool] = Ref::new(false)
                while !done_ref.val {
                  stream.next(on_done=fn(chunk_result) {
                    match chunk_result {
                      Ok(Some(chunk)) => {
                        counts_ref.val.push(chunk.row_count())
                        for row in chunk.rows {
                          rows_ref.val.push(row.join(","))
                        }
                      }
                      Ok(None) => done_ref.val = true
                      Err(err) => {
                        error_ref.val = Some("stream failed: \{err.message()}")
                        done_ref.val = true
                      }
                    }
                  })
                }
                stream.close(on_done=fn(_) { () })
              }
              Err(err) =>
                error_ref.val = Some("stream failed: \{err.message()}")
            }
          },
        )
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => fail(message)
    None => inspect(rows_ref.val, content="[abc,0, abc,1, abc,2]")
  }
}

///|
test "VectorChunk::to_typed skips string round-trip" {
  match run_query_chunks("SELECT 1::INTEGER AS a, 'x' AS b") {
    Ok(chunks) => {
      let typed = first_chunk(chunks).to_typed()
      match typed.get_value(0, 0) {
        Some(Value::Int(1)) => ()
        other => fail("expected Int(1)")
      }
      match typed.get_value(0, 1) {
        Some(Value::String("x")) => ()
        other => fail("expected String(x)")
      }
    }
    Err(err) => fail(err)
  }
}

///|
test "query_chunks error propagation" {
  match run_query_chunks("SELECT * FROM nonexistent_table") {
    Ok(_) => fail("expected error for missing table")
    Err(_) => ()
  }
}

///|
/// INTERVAL payloads arrive as flat `[months, days, micros]` triples; more
/// than one row is required to pin the `i * 3` / `+1` / `+2` indexing.
test "query_chunks decodes multi-row interval triples" {
  match
    run_query_chunks(
      "SELECT * FROM (VALUES (INTERVAL '1 day'), (INTERVAL '2 months 3 days'), (INTERVAL '10 hours')) t(iv)",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.row_count(), content="3")
      match chunk.column(0) {
        Some(vector) => {
          match vector.logical_type {
            ColumnType::Interval => ()
            _ => fail("expected Interval column type")
          }
          match vector.intervals() {
            Some(values) => {
              inspect(values.length(), content="3")
              let got : Array[String] = []
              for iv in values {
                got.push("\{iv.months}/\{iv.days}/\{iv.micros}")
              }
              assert_eq(got, ["0/1/0", "2/3/0", "0/0/36000000000"])
            }
            None => fail("expected Interval vector")
          }
        }
        None => fail("missing column 0")
      }
      // Per-row typed access agrees with the raw triples.
      match chunk.get_value(1, 0) {
        Some(Value::Interval(iv)) => {
          inspect(iv.months, content="2")
          inspect(iv.days, content="3")
          inspect(iv.micros, content="0")
        }
        other => fail("expected Interval at row 1")
      }
      match chunk.get_value(2, 0) {
        Some(Value::Interval(iv)) => {
          inspect(iv.months, content="0")
          inspect(iv.days, content="0")
          inspect(iv.micros, content="36000000000")
        }
        other => fail("expected Interval at row 2")
      }
    }
    Err(err) => fail(err)
  }
}

///|
/// Fixed-size ARRAY columns (DuckDB type 33) decode through the `List`
/// layout with computed offsets `i * array_size` and a dense child of
/// `rows * array_size` elements; NULL parents keep their slots but flag
/// invalidity.
test "query_chunks decodes fixed-size arrays" {
  match
    run_query_chunks(
      "SELECT * FROM (VALUES ([1,2,3]::INTEGER[3]), ([4,5,6]::INTEGER[3]), (NULL::INTEGER[3])) t(a)",
    ) {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.row_count(), content="3")
      match chunk.column(0) {
        Some(vector) => {
          match vector.logical_type {
            ColumnType::Array => ()
            _ => fail("expected Array column type")
          }
          match vector.list_parts() {
            Some((child, offsets, lengths)) => {
              let got_offsets : Array[Int] = []
              let got_lengths : Array[Int] = []
              for i in 0..<offsets.length() {
                got_offsets.push(offsets[i].to_int())
                got_lengths.push(lengths[i].to_int())
              }
              assert_eq(got_offsets, [0, 3, 6])
              assert_eq(got_lengths, [3, 3, 3])
              // Dense child carries rows * array_size elements.
              inspect(child.len(), content="9")
              match child.ints() {
                Some(xs) => {
                  inspect(xs.length(), content="9")
                  let flat : Array[Int] = []
                  for i in 0..<6 {
                    flat.push(xs[i])
                  }
                  assert_eq(flat, [1, 2, 3, 4, 5, 6])
                }
                None => fail("expected Int32 child")
              }
            }
            None => fail("expected List payload for ARRAY")
          }
        }
        None => fail("missing column 0")
      }
      match chunk.get_value(1, 0) {
        Some(Value::List(items)) =>
          inspect(items.map(fn(v) { v.to_string() }), content="[4, 5, 6]")
        other => fail("expected List at row 1")
      }
      inspect(chunk.is_null(2, 0), content="true")
      match chunk.get_value(2, 0) {
        None => ()
        other => fail("expected None for NULL array row")
      }
    }
    Err(err) => fail(err)
  }
}

///|
/// `PreparedStatement::execute_chunks` materializes canonical chunks on
/// success and surfaces execute-time failures as `Err`.
test "PreparedStatement::execute_chunks success and execute-time error" {
  match run_execute_chunks("SELECT 42::INTEGER AS i") {
    Ok(chunks) => {
      let chunk = first_chunk(chunks)
      inspect(chunk.row_count(), content="1")
      match chunk.get_value(0, 0) {
        Some(Value::Int(42)) => ()
        other => fail("expected Int(42)")
      }
    }
    Err(err) => fail(err)
  }
  // Prepare succeeds; execution fails (conversion error at execute time).
  match
    run_execute_chunks("SELECT CAST(x AS INTEGER) FROM (VALUES ('abc')) t(x)") {
    Ok(_) => fail("expected execute error for bad cast")
    Err(_) => ()
  }
}

///|
/// An empty stream is already at end-of-stream: first fetch must yield
/// `Ok(None)`, not an error or a phantom chunk.
test "ResultStream::next_chunk yields Ok(None) for empty results" {
  match run_stream_outcomes("SELECT i FROM range(10) t(i) WHERE false") {
    Ok(outcomes) => assert_eq(outcomes, ["none"])
    Err(err) => fail(err)
  }
  match run_stream_outcomes("SELECT i FROM range(3) t(i)") {
    Ok(outcomes) => assert_eq(outcomes, ["some", "none"])
    Err(err) => fail(err)
  }
}

///|
/// Fetching from a closed stream surfaces the native "stream is null"
/// message as `Err`, exercising the non-empty-error branch of `next_chunk`.
test "ResultStream::next_chunk errors after close" {
  let outcome_ref : Ref[String] = Ref::new("")
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) =>
        conn.query_stream("SELECT 1::INTEGER AS i", on_done=fn(stream_result) {
          match stream_result {
            Ok(stream) => {
              stream.close(on_done=fn(_) { () })
              stream.next_chunk(on_done=fn(chunk_result) {
                match chunk_result {
                  Ok(_) => outcome_ref.val = "ok"
                  Err(_) => outcome_ref.val = "err"
                }
              })
            }
            Err(err) => error_ref.val = Some("stream failed: \{err.message()}")
          }
        })
      Err(err) => error_ref.val = Some("connect failed: \{err.message()}")
    }
  })
  match error_ref.val {
    Some(message) => fail(message)
    None => inspect(outcome_ref.val, content="err")
  }
}

///|
/// The compat `query()` path renders cells via `native_chunk_string_rows`;
/// NULL cells must produce `""` plus a `true` null flag, non-NULL cells the
/// rendered value, for every column type.
test "query compat path renders NULL cells across types" {
  match
    run_query_result(
      "SELECT NULL::INTEGER AS ni, 1.5::DOUBLE AS d, NULL::VARCHAR AS s, true AS b, NULL::BOOLEAN AS nb, 7::BIGINT AS bi",
    ) {
    Ok(qr) => {
      inspect(qr.rows, content="[[, 1.5, , true, , 7]]")
      inspect(qr.nulls, content="[[true, false, true, false, true, false]]")
    }
    Err(err) => fail(err)
  }
}

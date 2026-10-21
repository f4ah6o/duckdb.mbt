///|
// DuckDB 2.0 keeps the classic C API source-compatible but adds behavior the
// native binding has to pin down:
//
// - TIMESTAMP_TZ_NS (type id 42) must render engine text instead of an empty
//   string, in materialized and streaming results;
// - appender flushes fail with `resource_in_use` while an unfinished streaming
//   result is open;
// - `storage_compatibility_version` at open time produces reusable files.
//
// The 2.0-only expectations are gated on `SELECT version()` so the same tests
// run unchanged on the 1.5 lane.

///|
fn ddb2_query(conn : Connection, sql : String) -> Result[QueryResult, String] {
  let response : Ref[Result[QueryResult, String]?] = Ref::new(None)
  conn.query(sql, on_done=fn(result) {
    match result {
      Ok(rows) => response.val = Some(Ok(rows))
      Err(err) => response.val = Some(Err(err.message()))
    }
  })
  match response.val {
    Some(result) => result
    None => Err("query callback did not run")
  }
}

///|
fn ddb2_query_one(conn : Connection, sql : String) -> String {
  match ddb2_query(conn, sql) {
    Ok(rows) =>
      match rows.cell(0, 0) {
        Some(value) => value
        None => ""
      }
    Err(_) => ""
  }
}

///|
fn ddb2_is_20(conn : Connection) -> Bool {
  ddb2_query_one(conn, "SELECT version()").has_prefix("v2.")
}

///|
test "native build and engine major version agree" {
  let failure : Ref[String?] = Ref::new(None)
  let version_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        version_ref.val = Some(ddb2_query_one(conn, "SELECT version()"))
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => failure.val = Some("connect failed: " + err.message())
    }
  })
  match failure.val {
    Some(message) => fail(message)
    None => ()
  }
  match version_ref.val {
    Some(version) =>
      if version == "" {
        fail("SELECT version() returned no value")
      } else if !version.has_prefix("v") {
        fail("unexpected engine version string: \{version}")
      } else if version.has_prefix("v2.") && !native_has_duckdb2() {
        fail(
          "the 2.0 engine is linked against 1.x headers; build against the " +
          "libduckdb headers that match the library",
        )
      }
    None => fail("version was not observed")
  }
}

///|
test "duckdb 2.0: TIMESTAMPTZ_NS renders engine text" {
  let sql = "SELECT CAST('2024-06-03 12:34:56.123456789+00' AS TIMESTAMPTZ_NS) AS v"
  let failure : Ref[String?] = Ref::new(None)
  let is_20_ref : Ref[Bool?] = Ref::new(None)
  let text_ref : Ref[String?] = Ref::new(None)
  let column_type_ref : Ref[ColumnType?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        is_20_ref.val = Some(ddb2_is_20(conn))
        match ddb2_query(conn, sql) {
          Ok(rows) => {
            text_ref.val = rows.cell(0, 0)
            column_type_ref.val = Some(rows.column_types[0])
          }
          Err(message) => error_ref.val = Some(message)
        }
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => failure.val = Some("connect failed: " + err.message())
    }
  })
  match failure.val {
    Some(message) => fail(message)
    None => ()
  }
  match is_20_ref.val {
    Some(true) => {
      match error_ref.val {
        Some(message) => fail("DuckDB 2.0 rejected TIMESTAMPTZ_NS: \{message}")
        None =>
          match text_ref.val {
            Some(text) => assert_eq(text, "2024-06-03 12:34:56.123456789+00")
            None => fail("expected a non-null TIMESTAMPTZ_NS cell")
          }
      }
      match column_type_ref.val {
        Some(Unknown(id)) => assert_eq(id, 42)
        Some(_) => fail("expected ColumnType::Unknown(42)")
        None => fail("column type was not observed")
      }
    }
    Some(false) =>
      // 1.x has no TIMESTAMPTZ_NS type, so the cast must be rejected.
      match text_ref.val {
        Some(text) => fail("DuckDB 1.x accepted TIMESTAMPTZ_NS: \{text}")
        None => ()
      }
    None => fail("engine version was not observed")
  }
}

///|
test "duckdb 2.0: TIMESTAMPTZ_NS streams" {
  let sql = "SELECT CAST('2024-06-03 12:34:56.123456789+00' AS TIMESTAMPTZ_NS) AS v"
  let failure : Ref[String?] = Ref::new(None)
  let is_20_ref : Ref[Bool?] = Ref::new(None)
  let text_ref : Ref[String?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        is_20_ref.val = Some(ddb2_is_20(conn))
        conn.query_stream(sql, on_done=fn(stream_result) {
          match stream_result {
            Ok(stream) => {
              stream.next(on_done=fn(chunk_result) {
                match chunk_result {
                  Ok(Some(chunk)) => text_ref.val = chunk.cell(0, 0)
                  Ok(None) => error_ref.val = Some("stream returned no chunk")
                  Err(err) => error_ref.val = Some(err.message())
                }
              })
              stream.close(on_done=fn(_) { () })
            }
            Err(err) => error_ref.val = Some(err.message())
          }
        })
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => failure.val = Some("connect failed: " + err.message())
    }
  })
  match failure.val {
    Some(message) => fail(message)
    None => ()
  }
  match is_20_ref.val {
    Some(true) =>
      match error_ref.val {
        Some(message) => fail("2.0 stream failed: \{message}")
        None =>
          match text_ref.val {
            Some(text) => assert_eq(text, "2024-06-03 12:34:56.123456789+00")
            None => fail("stream produced no TIMESTAMPTZ_NS value")
          }
      }
    Some(false) => ()
    None => fail("engine version was not observed")
  }
}

///|
test "duckdb 2.0: appender flush needs a drained stream" {
  let failure : Ref[String?] = Ref::new(None)
  let is_20_ref : Ref[Bool?] = Ref::new(None)
  let flush_ref : Ref[Result[Unit, DuckDBError]?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        is_20_ref.val = Some(ddb2_is_20(conn))
        match ddb2_query(conn, "CREATE OR REPLACE TABLE t(i INTEGER)") {
          Ok(_) => ()
          Err(message) => failure.val = Some("create table failed: " + message)
        }
        conn.query_stream("SELECT i FROM RANGE(100000) tbl(i)", on_done=fn(
          stream_result,
        ) {
          match stream_result {
            Ok(stream) => {
              // Fetch one chunk and leave the stream unfinished.
              stream.next(on_done=fn(_) { () })
              conn.create_appender("main", "t", on_done=fn(appender_result) {
                match appender_result {
                  Ok(appender) => {
                    ignore(appender.begin_row())
                    ignore(appender.append_int(1))
                    ignore(appender.end_row())
                    flush_ref.val = Some(appender.flush())
                    appender.close(on_done=fn(_) { () })
                  }
                  Err(err) =>
                    failure.val = Some(
                      "create_appender failed: " + err.message(),
                    )
                }
              })
              stream.close(on_done=fn(_) { () })
            }
            Err(err) =>
              failure.val = Some("query_stream failed: " + err.message())
          }
        })
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => failure.val = Some("connect failed: " + err.message())
    }
  })
  match failure.val {
    Some(message) => fail(message)
    None => ()
  }
  match is_20_ref.val {
    Some(true) =>
      match flush_ref.val {
        Some(Err(err)) =>
          match err {
            DuckDB(error_type~, ..) => assert_eq(error_type, "resource_in_use")
            _ => fail("expected the engine to classify the flush failure")
          }
        Some(Ok(_)) => fail("2.0 accepted a flush while a stream was open")
        None => fail("flush did not run")
      }
    Some(false) =>
      match flush_ref.val {
        Some(Ok(_)) => ()
        Some(Err(err)) => fail("1.5 flush failed: \{err.message()}")
        None => fail("flush did not run")
      }
    None => fail("engine version was not observed")
  }
}

///|
test "storage_compatibility_version round-trips a database file" {
  let failure : Ref[String?] = Ref::new(None)
  let version_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        let version = ddb2_query_one(conn, "SELECT version()")
        if version != "" {
          version_ref.val = Some(version)
        }
        conn.close(on_done=fn(_) { () })
      }
      Err(err) => failure.val = Some("connect failed: " + err.message())
    }
  })
  match failure.val {
    Some(message) => fail(message)
    None => ()
  }
  match version_ref.val {
    None => fail("engine version was not observed")
    Some(version) => {
      // Version-tagged so a 2.0 file is never reopened by a 1.x engine.
      let path = "/tmp/duckdb_mbt_storage_" + version + ".db"
      let config = Config::create()
      match config.set("storage_compatibility_version", "v1.5.6") {
        // Engines before 1.5.6 do not know the option; the supported 1.5.6 and
        // 2.0 lanes both accept it.
        Err(_) => ()
        Ok(_) => {
          let created : Ref[String?] = Ref::new(None)
          connect_with_config(
            on_ready=fn(created_result) {
              match created_result {
                Ok(writer) => {
                  match
                    ddb2_query(
                      writer, "DROP TABLE IF EXISTS t; CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (7)",
                    ) {
                    Ok(_) => ()
                    Err(message) => created.val = Some(message)
                  }
                  writer.close(on_done=fn(_) { () })
                }
                Err(err) => created.val = Some(err.message())
              }
            },
            Some(config),
            path~,
          )
          match created.val {
            Some(message) => fail("write failed: " + message)
            None => {
              let read_back : Ref[String?] = Ref::new(None)
              connect(
                on_ready=fn(read_result) {
                  match read_result {
                    Ok(reader) => {
                      match ddb2_query(reader, "SELECT x FROM t") {
                        Ok(rows) =>
                          match rows.cell(0, 0) {
                            Some(value) => read_back.val = Some(value)
                            None => read_back.val = Some("")
                          }
                        Err(message) =>
                          read_back.val = Some("query failed: " + message)
                      }
                      reader.close(on_done=fn(_) { () })
                    }
                    Err(err) =>
                      read_back.val = Some("connect failed: " + err.message())
                  }
                },
                path~,
              )
              match read_back.val {
                Some("7") => ()
                Some(message) =>
                  fail(
                    "expected 7 from the reopened database, got '\{message}'",
                  )
                None => fail("the reopened database returned no value")
              }
            }
          }
        }
      }
    }
  }
}

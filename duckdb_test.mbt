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

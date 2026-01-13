///|
test "query rows" {
  let result_ref : Ref[QueryResult?] = Ref::new(None)
  let error_ref : Ref[String?] = Ref::new(None)
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query("select 1 as a, NULL as b, 'duck' as c", on_done=fn(
          query_result,
        ) {
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
    Some(message) => fail(message)
    None => ()
  }
  match result_ref.val {
    Some(result) => {
      inspect(result.columns, content="[\"a\", \"b\", \"c\"]")
      inspect(result.rows, content="[[\"1\", \"\", \"duck\"]]")
      inspect(result.nulls, content="[[false, true, false]]")
      inspect(result.cell(0, 1), content="None")
    }
    None => fail("query returned no result")
  }
}

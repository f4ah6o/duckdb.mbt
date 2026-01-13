///|
test "query rows" {
  connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query("select 1 as a, NULL as b, 'duck' as c", on_done=fn(
          query_result,
        ) {
          match query_result {
            Ok(result) => {
              inspect(result.columns, content="[\"a\", \"b\", \"c\"]")
              inspect(result.rows, content="[[\"1\", \"\", \"duck\"]]")
              inspect(result.nulls, content="[[false, true, false]]")
              inspect(result.cell(0, 1), content="None")
            }
            Err(DuckDBError::Message(message)) =>
              fail("query failed: \{message}")
          }
        })
        conn.close(on_done=fn(closed) {
          match closed {
            Ok(_) => ()
            Err(DuckDBError::Message(message)) =>
              fail("close failed: \{message}")
          }
        })
      }
      Err(DuckDBError::Message(message)) => fail("connect failed: \{message}")
    }
  })
}

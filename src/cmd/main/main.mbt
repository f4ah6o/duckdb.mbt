///|
fn main {
  @lib.connect(on_ready=fn(result) {
    match result {
      Ok(conn) => {
        conn.query("select 42 as answer", on_done=fn(query_result) {
          match query_result {
            Ok(result) => {
              println("columns: \{result.columns}")
              println("rows: \{result.rows}")
            }
            Err(@lib.DuckDBError::Message(message)) =>
              println("query failed: \{message}")
          }
        })
        conn.close(on_done=fn(closed) {
          match closed {
            Ok(_) => ()
            Err(@lib.DuckDBError::Message(message)) =>
              println("close failed: \{message}")
          }
        })
      }
      Err(@lib.DuckDBError::Message(message)) =>
        println("connect failed: \{message}")
    }
  })
}

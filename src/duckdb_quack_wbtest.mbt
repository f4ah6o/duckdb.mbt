///|
test "quack SQL string escaping" {
  inspect(quack_sql_string("quack:local'host"), content="'quack:local''host'")
  inspect(quack_sql_string("SELECT 'duck'"), content="'SELECT ''duck'''")
}

///|
test "quack stateless query SQL" {
  inspect(
    quack_query_sql("quack:remote.com", "SELECT 'duck'", "tok'en", true),
    content="FROM quack_query('quack:remote.com', 'SELECT ''duck''', token => 'tok''en', disable_ssl => true)",
  )
}

///|
test "quack attach SQL" {
  match quack_attach_sql("quack:localhost", "remote_db", "secret", false) {
    Ok(sql) =>
      inspect(
        sql,
        content="ATTACH 'quack:localhost' AS \"remote_db\" (TYPE quack, TOKEN 'secret')",
      )
    Err(DuckDBError::Message(message)) =>
      fail("unexpected attach SQL error: \{message}")
  }
  match quack_attach_sql("quack:remote.com", "remote_2", "", true) {
    Ok(sql) =>
      inspect(
        sql,
        content="ATTACH 'quack:remote.com' AS \"remote_2\" (TYPE quack, DISABLE_SSL true)",
      )
    Err(DuckDBError::Message(message)) =>
      fail("unexpected attach SQL error: \{message}")
  }
}

///|
test "quack attach rejects unsafe aliases" {
  match quack_attach_sql("quack:localhost", "", "", false) {
    Ok(sql) => fail("expected empty alias error, got \{sql}")
    Err(DuckDBError::Message(message)) =>
      inspect(message, content="Quack alias must not be empty")
  }
  match quack_attach_sql("quack:localhost", "remote-db", "", false) {
    Ok(sql) => fail("expected invalid alias error, got \{sql}")
    Err(DuckDBError::Message(message)) =>
      inspect(
        message,
        content="Quack alias may only contain letters, digits, and underscores",
      )
  }
}

///|
test "quack server and secret SQL" {
  inspect(
    quack_serve_sql("quack:0.0.0.0:9494", "tok'en", true),
    content="CALL quack_serve('quack:0.0.0.0:9494', token => 'tok''en', allow_other_hostname => true)",
  )
  inspect(
    quack_stop_sql("quack:0.0.0.0:9494"),
    content="CALL quack_stop('quack:0.0.0.0:9494')",
  )
  inspect(
    quack_secret_sql("tok'en", "quack:localhost"),
    content="CREATE SECRET (TYPE quack, TOKEN 'tok''en', SCOPE 'quack:localhost')",
  )
}

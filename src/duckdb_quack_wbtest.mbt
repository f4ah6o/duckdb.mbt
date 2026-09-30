///|
test "decimal Value::to_string renders full 128-bit" {
  // Positive max-width decimal: 10^38 - 1
  let d1 = decimal_from_hugeint(
    687399551400673279UL, 5421010862427522170L, 38, 0,
  )
  inspect(
    Value::Decimal(d1).to_string(),
    content="99999999999999999999999999999999999999",
  )
  // Negative value between -1 and 0 keeps its sign
  let d2 = decimal_from_hugeint((-50L).reinterpret_as_uint64(), -1L, 10, 2)
  inspect(Value::Decimal(d2).to_string(), content="-0.50")
}

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
    Err(err) => fail("unexpected attach SQL error: \{err.message()}")
  }
  match quack_attach_sql("quack:remote.com", "remote_2", "", true) {
    Ok(sql) =>
      inspect(
        sql,
        content="ATTACH 'quack:remote.com' AS \"remote_2\" (TYPE quack, DISABLE_SSL true)",
      )
    Err(err) => fail("unexpected attach SQL error: \{err.message()}")
  }
}

///|
test "quack attach rejects unsafe aliases" {
  match quack_attach_sql("quack:localhost", "", "", false) {
    Ok(sql) => fail("expected empty alias error, got \{sql}")
    Err(err) => inspect(err.message(), content=(
      #|invalid argument alias: Quack alias must not be empty
    ))
  }
  match quack_attach_sql("quack:localhost", "remote-db", "", false) {
    Ok(sql) => fail("expected invalid alias error, got \{sql}")
    Err(err) =>
      inspect(
        err.message(),
        content=(
          #|invalid argument alias: Quack alias may only contain letters, digits, and underscores
        ),
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

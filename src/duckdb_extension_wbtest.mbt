///|
test "install extension SQL" {
  match install_extension_sql("quack", "core_nightly", true) {
    Ok(sql) => inspect(sql, content="FORCE INSTALL quack FROM core_nightly")
    Err(err) => fail("unexpected install SQL error: \{err.message()}")
  }
  match install_extension_sql("httpfs", "", false) {
    Ok(sql) => inspect(sql, content="INSTALL httpfs")
    Err(err) => fail("unexpected install SQL error: \{err.message()}")
  }
  match install_extension_sql("httpfs", "community", false) {
    Ok(sql) => inspect(sql, content="INSTALL httpfs FROM community")
    Err(err) => fail("unexpected install SQL error: \{err.message()}")
  }
}

///|
test "install extension rejects unsafe names" {
  match install_extension_sql("httpfs; DROP TABLE t", "", false) {
    Ok(sql) => fail("expected unsafe extension name error, got \{sql}")
    Err(err) =>
      inspect(
        err.message(),
        content=(
          #|invalid argument name: may only contain letters, digits, and underscores
        ),
      )
  }
  match install_extension_sql("httpfs", "bad repo", false) {
    Ok(sql) => fail("expected unsafe repository error, got \{sql}")
    Err(err) =>
      inspect(
        err.message(),
        content=(
          #|invalid argument repository: may only contain letters, digits, and underscores
        ),
      )
  }
}

///|
test "attach SQL" {
  match attach_sql("data.duckdb", "", "") {
    Ok(sql) => inspect(sql, content="ATTACH 'data.duckdb'")
    Err(err) => fail("unexpected attach SQL error: \{err.message()}")
  }
  match
    attach_sql("quack:localhost", "remote_db", "TYPE quack, TOKEN 'secret'") {
    Ok(sql) =>
      inspect(
        sql,
        content="ATTACH 'quack:localhost' AS \"remote_db\" (TYPE quack, TOKEN 'secret')",
      )
    Err(err) => fail("unexpected attach SQL error: \{err.message()}")
  }
}

///|
test "attach rejects unsafe aliases" {
  match attach_sql("data.duckdb", "remote-db", "") {
    Ok(sql) => fail("expected unsafe alias error, got \{sql}")
    Err(err) =>
      inspect(
        err.message(),
        content=(
          #|invalid argument alias: may only contain letters, digits, and underscores
        ),
      )
  }
}

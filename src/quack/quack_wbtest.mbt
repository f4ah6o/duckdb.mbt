///|
test "quack SQL string escaping" {
  inspect(sql_string("quack:local'host"), content="'quack:local''host'")
  inspect(sql_string("SELECT 'duck'"), content="'SELECT ''duck'''")
}

///|
test "quack stateless query SQL" {
  inspect(
    query_sql("quack:remote.com", "SELECT 'duck'", "tok'en", true),
    content="FROM quack_query('quack:remote.com', 'SELECT ''duck''', token => 'tok''en', disable_ssl => true)",
  )
}

///|
test "quack attach options" {
  inspect(attach_options("secret", false), content="TYPE quack, TOKEN 'secret'")
  inspect(attach_options("", true), content="TYPE quack, DISABLE_SSL true")
  inspect(attach_options("", false), content="TYPE quack")
}

///|
test "quack server and secret SQL" {
  inspect(
    serve_sql("quack:0.0.0.0:9494", "tok'en", true),
    content="CALL quack_serve('quack:0.0.0.0:9494', token => 'tok''en', allow_other_hostname => true)",
  )
  inspect(
    stop_sql("quack:0.0.0.0:9494"),
    content="CALL quack_stop('quack:0.0.0.0:9494')",
  )
  inspect(
    secret_sql("tok'en", "quack:localhost"),
    content="CREATE SECRET (TYPE quack, TOKEN 'tok''en', SCOPE 'quack:localhost')",
  )
}

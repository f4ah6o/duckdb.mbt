///|
test "arrow schema decode failure is a Backend error on js" {
  match parse_arrow_schema_json("this is not json") {
    Err(Backend(backend~, message~)) => {
      assert_eq(backend, "js")
      if !message.contains("schema parse failed") {
        fail("expected preserved diagnostic, got \{message}")
      }
    }
    Err(err) => fail("expected Backend error, got \{err.message()}")
    Ok(_) => fail("expected Backend error, got Ok")
  }
}

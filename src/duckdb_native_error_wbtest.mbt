///|
test "duckdb_error_type_name maps every engine code 0-42" {
  let expected = [
    "invalid", "out_of_range", "conversion", "unknown_type", "decimal", "mismatch_type",
    "divide_by_zero", "object_size", "invalid_type", "serialization", "transaction",
    "not_implemented", "expression", "catalog", "parser", "planner", "scheduler",
    "executor", "constraint", "index", "stat", "connection", "syntax", "settings",
    "binder", "network", "optimizer", "null_pointer", "io", "interrupt", "fatal",
    "internal", "invalid_input", "out_of_memory", "permission", "parameter_not_resolved",
    "parameter_not_allowed", "dependency", "http", "missing_extension", "autoload",
    "sequence", "invalid_configuration",
  ]
  for code = 0; code < expected.length(); code = code + 1 {
    let name = duckdb_error_type_name(code)
    if name != expected[code] {
      fail("code \{code}: expected \{expected[code]}, got \{name}")
    }
  }
}

///|
test "duckdb_error_type_name maps code 42 to invalid_configuration" {
  assert_eq(duckdb_error_type_name(42), "invalid_configuration")
}

///|
test "duckdb_error_type_name falls back to unknown" {
  assert_eq(duckdb_error_type_name(-1), "unknown")
  assert_eq(duckdb_error_type_name(43), "unknown")
  assert_eq(duckdb_error_type_name(999), "unknown")
}

///|
pub struct FixtureCase {
  name : String
  sql : String
  columns : Array[String]
  rows : Array[Array[String]]
  nulls : Array[Array[Bool]]
}

///|
fn fail_case(case_name : String, message : String) -> Unit raise {
  fail("fixture '\{case_name}': \{message}")
}

///|
fn expect_string(
  case_name : String,
  label : String,
  actual : String,
  expected : String,
) -> Unit raise {
  if actual != expected {
    fail_case(case_name, "\{label} expected '\{expected}' got '\{actual}'")
  }
}

///|
fn expect_bool(
  case_name : String,
  label : String,
  actual : Bool,
  expected : Bool,
) -> Unit raise {
  if actual != expected {
    fail_case(case_name, "\{label} expected \{expected} got \{actual}")
  }
}

///|
fn expect_string_array(
  case_name : String,
  label : String,
  actual : Array[String],
  expected : Array[String],
) -> Unit raise {
  if actual.length() != expected.length() {
    fail_case(
      case_name,
      "\{label} length expected \{expected.length()} got \{actual.length()}",
    )
  }
  for i = 0; i < expected.length(); i = i + 1 {
    expect_string(case_name, "\{label}[\{i}]", actual[i], expected[i])
  } else {
    ()
  }
}

///|
fn expect_bool_array(
  case_name : String,
  label : String,
  actual : Array[Bool],
  expected : Array[Bool],
) -> Unit raise {
  if actual.length() != expected.length() {
    fail_case(
      case_name,
      "\{label} length expected \{expected.length()} got \{actual.length()}",
    )
  }
  for i = 0; i < expected.length(); i = i + 1 {
    expect_bool(case_name, "\{label}[\{i}]", actual[i], expected[i])
  } else {
    ()
  }
}

///|
fn expect_string_matrix(
  case_name : String,
  label : String,
  actual : Array[Array[String]],
  expected : Array[Array[String]],
) -> Unit raise {
  if actual.length() != expected.length() {
    fail_case(
      case_name,
      "\{label} row count expected \{expected.length()} got \{actual.length()}",
    )
  }
  for row = 0; row < expected.length(); row = row + 1 {
    expect_string_array(
      case_name,
      "\{label}[\{row}]",
      actual[row],
      expected[row],
    )
  } else {
    ()
  }
}

///|
fn expect_bool_matrix(
  case_name : String,
  label : String,
  actual : Array[Array[Bool]],
  expected : Array[Array[Bool]],
) -> Unit raise {
  if actual.length() != expected.length() {
    fail_case(
      case_name,
      "\{label} row count expected \{expected.length()} got \{actual.length()}",
    )
  }
  for row = 0; row < expected.length(); row = row + 1 {
    expect_bool_array(case_name, "\{label}[\{row}]", actual[row], expected[row])
  } else {
    ()
  }
}

///|
pub fn expect_fixture_case(
  case : FixtureCase,
  columns : Array[String],
  rows : Array[Array[String]],
  nulls : Array[Array[Bool]],
) -> Unit raise {
  expect_string_array(case.name, "columns", columns, case.columns)
  expect_string_matrix(case.name, "rows", rows, case.rows)
  expect_bool_matrix(case.name, "nulls", nulls, case.nulls)
}

///|
pub fn expect_query_result(
  case : FixtureCase,
  result : QueryResult,
) -> Unit raise {
  expect_fixture_case(case, result.columns, result.rows, result.nulls)
}

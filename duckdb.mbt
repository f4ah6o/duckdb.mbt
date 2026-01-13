///|
#external
pub type Connection

///|
/// JS backend selection for `connect`.
pub enum JsBackend {
  Auto
  Node
  Wasm
}

///|
/// DuckDB error wrapper.
pub suberror DuckDBError {
  Message(String)
}

///|
/// Query result data is represented as strings plus a null mask.
pub struct QueryResult {
  columns : Array[String]
  rows : Array[Array[String]]
  nulls : Array[Array[Bool]]
}

///|
pub fn QueryResult::row_count(self : QueryResult) -> Int {
  self.rows.length()
}

///|
pub fn QueryResult::column_count(self : QueryResult) -> Int {
  self.columns.length()
}

///|
pub fn QueryResult::cell(self : QueryResult, row : Int, col : Int) -> String? {
  if self.nulls[row][col] {
    None
  } else {
    Some(self.rows[row][col])
  }
}

///|
/// Deprecated APIs kept for the 0.6.x -> 0.7.0 migration.

///|
/// Extract the diagnostic text of an error, as matching on the removed
/// `DuckDBError::Message(String)` variant used to.
///
/// Prefer matching on the structured `DuckDBError` variants for categories
/// and calling `DuckDBError::message` for the text.
#deprecated("DuckDBError::Message was replaced by structured variants in 0.7.0; use err.message() for the diagnostic text or match on the Query/Prepare/Bind/Append/Unsupported/Closed/InvalidArgument/Backend/DuckDB variants")
pub fn DuckDBError::to_message(self : DuckDBError) -> String {
  self.message()
}

///|
/// Construct an error from a plain diagnostic message, as constructing
/// `DuckDBError::Message(message)` used to.
///
/// Prefer constructing the structured variant that fits the failure (for
/// example `DuckDBError::Query`, `DuckDBError::Unsupported`, or
/// `DuckDBError::Backend`).
#deprecated("DuckDBError::Message was replaced by structured variants in 0.7.0; construct the matching variant (Query/Prepare/Bind/Append/Unsupported/Closed/InvalidArgument/Backend/DuckDB) instead")
pub fn duckdb_message_error(message : String) -> DuckDBError {
  DuckDBError::Backend(backend="generic", message~)
}

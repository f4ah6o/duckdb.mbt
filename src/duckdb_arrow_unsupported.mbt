///|
/// Stub backend handle; Arrow is unsupported on this target so it is never
/// constructed.
#external
type ArrowHandle

///|
/// An Unsupported error for `feature` on this stub-only target.
fn unsupported_target_error_feature(feature : String) -> DuckDBError {
  Backend::Unsupported.capabilities().unsupported(feature)
}

///|
fn arrow_result_close_handle(handle : ArrowHandle) -> Unit {
  let _ = handle
}

// ============================================================================
// Arrow Stubs (for unsupported targets)
// ============================================================================

///|
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  let _ = self
  let _ = sql
  on_done(Err(unsupported_target_error_feature("arrow integration")))
}

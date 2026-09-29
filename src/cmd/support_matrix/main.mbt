///|
/// Print the README "Feature Support Matrix" section, rendered from
/// `BackendCapabilities` — the same table the capability conformance tests
/// snapshot. `scripts/support_matrix.sh` splices this output into
/// `README.mbt.md` between the support-matrix markers.
fn main {
  println(@lib.support_matrix_markdown())
}

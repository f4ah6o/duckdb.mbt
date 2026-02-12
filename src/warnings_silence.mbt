///|
/// Keep quickcheck import marked as used across targets.
fn _keep_quickcheck_reference() -> Unit {
  let _ = @pbt.pure(())
}

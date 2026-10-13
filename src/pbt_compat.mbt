///|
/// QuickCheck compatibility helpers for legacy PBT tests.

///|
struct CheckConfig {
  cases : Int32
  max_size : Int32
  seed : Int32
  max_shrinks : Int32
  discard_ratio : Int32
}

///|
pub fn CheckConfig::seed(self : CheckConfig) -> Int32 {
  self.seed
}

///|
pub fn CheckConfig::new(
  cases : Int32,
  max_size : Int32,
  seed : Int32,
  max_shrinks : Int32,
  discard_ratio~ : Int32 = 10,
) -> CheckConfig {
  { cases, max_size, seed, max_shrinks, discard_ratio, }
}

///|
pub fn CheckConfig::default() -> CheckConfig {
  CheckConfig::new(100, 30, 0, 100)
}

///|
struct CheckResult {
  passed : Bool
  stats : String?
}

///|
pub fn CheckResult::passed(self : CheckResult) -> Bool {
  self.passed
}

///|
pub fn CheckResult::stats(self : CheckResult) -> String? {
  self.stats
}

///|
fn[A] property_from_result(
  name : String,
  f : (A) -> Result[Unit, String],
) -> (A) -> @pbt.Property {
  fn(x : A) {
    match f(x) {
      Ok(_) => @pbt.counterexample(true, name)
      Err(msg) => @pbt.counterexample(false, name + ": " + msg)
    }
  }
}

///|
pub fn[A : @debug.Debug] assert_check(
  name : String,
  gen : @gen.Generator[A],
  check : (A) -> Result[Unit, String],
  config~ : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  let cfg = config
  let _ = cfg
  let prop_fn = property_from_result(name, check)
  let prop = match shrink {
    Some(shr) => @pbt.forall_shrink(gen, shr, prop_fn)
    None => @pbt.forall(gen, prop_fn)
  }
  try! @pbt.quick_check(prop)
}

///|
pub fn[A : @debug.Debug] check_with_stats(
  gen : @gen.Generator[A],
  check : (A) -> (Result[Unit, String], String?),
  config~ : CheckConfig = CheckConfig::default(),
) -> CheckResult {
  let cfg = config
  let _ = cfg
  let prop = @pbt.forall(gen, fn(x) {
    let (res, label) = check(x)
    let base = match res {
      Ok(_) => @pbt.counterexample(true, "")
      Err(msg) => @pbt.counterexample(false, msg)
    }
    match label {
      Some(lbl) => @pbt.classify(base, true, lbl)
      None => base
    }
  })
  try {
    @pbt.quick_check(prop)
    CheckResult::{ passed: true, stats: None, }
  } catch {
    err => CheckResult::{ passed: false, stats: Some("\{err}"), }
  }
}

///|
/// Size-driven array generator compatible with previous Gen::array_of.
pub fn[T] array_of(gen : @gen.Generator[T]) -> @gen.Generator[Array[T]] {
  @gen.sized(fn(size) { @gen.Generator::array_with_size(gen, size) })
}

///|
/// Simple integer shrinker for legacy shrink_int usage.
pub fn shrink_int(value : Int32) -> Iter[Int32] {
  let candidates : Array[Int32] = if value == 0 { [] } else { [0, value / 2] }
  candidates.iter()
}

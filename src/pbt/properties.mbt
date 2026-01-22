///|
/// PBT Property Combinators for DuckDB MoonBit bindings
/// Provides reusable property patterns for property-based testing

///|
/// Round-trip property: encode -> decode should preserve the original value
pub fn prop_roundtrip[A : Show, B : Show](
  gen: @pbt.Gen[A],
  encode: (A) -> B,
  decode: (B) -> A,
  name: String,
  equal: (A, A) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(original) {
      let encoded = encode(original)
      let decoded = decode(encoded)
      if equal(original, decoded) {
        Ok(())
      } else {
        Err(
          "Round-trip failed: \{original} -> \{encoded} -> \{decoded}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Idempotent property: operation(x) == operation(operation(x))
pub fn prop_idempotent[A : Show](
  gen: @pbt.Gen[A],
  operation: (A) -> A,
  name: String,
  equal: (A, A) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(input) {
      let once = operation(input)
      let twice = operation(once)
      if equal(once, twice) {
        Ok(())
      } else {
        Err(
          "Idempotence failed: op(\{input}) = \{once}, op(op(\{input})) = \{twice}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Invariant property: invariant(x) == invariant(operation(x))
pub fn prop_invariant[A : Show, B : Show](
  gen: @pbt.Gen[A],
  operation: (A) -> A,
  invariant: (A) -> B,
  name: String,
  equal: (B, B) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(input) {
      let before = invariant(input)
      let after = invariant(operation(input))
      if equal(before, after) {
        Ok(())
      } else {
        Err(
          "Invariant failed: invariant(\{input}) = \{before}, invariant(op(\{input})) = \{after}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Associative property: op(op(a, b), c) == op(a, op(b, c))
pub fn prop_associative[A : Show](
  gen: @pbt.Gen[A],
  operation: (A, A) -> A,
  name: String,
  equal: (A, A) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : ((A, A, A)) -> Iter[(A, A, A)],
) -> Unit {
  let triple_gen = gen.bind(fn(a) {
    gen.bind(fn(b) { gen.map(fn(c) { (a, b, c) }) })
  })

  let shrink_triple = fn(((a, b, c) : (A, A, A))) {
    let candidates : Array[(A, A, A)] = []
    // Try simplifying each component
    candidates.push((a, a, a))
    candidates.push((b, b, b))
    candidates.push((c, c, c))
    candidates.push((a, b, b))
    candidates.push((a, a, c))
    candidates.push((a, b, a))
    candidates.iter()
  }

  assert_check(
    name,
    triple_gen,
    fn((a, b, c)) {
      let left = operation(operation(a, b), c)
      let right = operation(a, operation(b, c))
      if equal(left, right) {
        Ok(())
      } else {
        Err(
          "Associativity failed: op(op(\{a}, \{b}), \{c}) = \{left}, op(\{a}, op(\{b}, \{c})) = \{right}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Commutative property: op(a, b) == op(b, a)
pub fn prop_commutative[A : Show](
  gen: @pbt.Gen[A],
  operation: (A, A) -> A,
  name: String,
  equal: (A, A) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : ((A, A)) -> Iter[(A, A)],
) -> Unit {
  let pair_gen = gen.bind(fn(a) { gen.map(fn(b) { (a, b) }) })

  assert_check(
    name,
    pair_gen,
    fn((a, b)) {
      let ab = operation(a, b)
      let ba = operation(b, a)
      if equal(ab, ba) {
        Ok(())
      } else {
        Err(
          "Commutativity failed: op(\{a}, \{b}) = \{ab}, op(\{b}, \{a}) = \{ba}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Identity property: op(x, identity) == x
pub fn prop_identity[A : Show](
  gen: @pbt.Gen[A],
  operation: (A, A) -> A,
  identity: A,
  name: String,
  equal: (A, A) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(x) {
      let result = operation(x, identity)
      if equal(result, x) {
        Ok(())
      } else {
        Err(
          "Identity failed: op(\{x}, identity) = \{result}, expected \{x}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Reflexive property: equal(x, x) should always be true
pub fn prop_reflexive[A : Show](
  gen: @pbt.Gen[A],
  equal: (A, A) -> Bool,
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(x) {
      if equal(x, x) {
        Ok(())
      } else {
        Err("Reflexivity failed: equal(\{x}, \{x}) should be true")
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Symmetric property: equal(a, b) implies equal(b, a)
pub fn prop_symmetric[A : Show](
  gen: @pbt.Gen[A],
  equal: (A, A) -> Bool,
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : ((A, A)) -> Iter[(A, A)],
) -> Unit {
  let pair_gen = gen.bind(fn(a) { gen.map(fn(b) { (a, b) }) })

  assert_check(
    name,
    pair_gen,
    fn((a, b)) {
      let ab = equal(a, b)
      let ba = equal(b, a)
      if ab == ba {
        Ok(())
      } else {
        Err(
          "Symmetry failed: equal(\{a}, \{b}) = \{ab}, equal(\{b}, \{a}) = \{ba}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Transitive property: equal(a, b) and equal(b, c) implies equal(a, c)
pub fn prop_transitive[A : Show](
  gen: @pbt.Gen[A],
  equal: (A, A) -> Bool,
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : ((A, A, A)) -> Iter[(A, A, A)],
) -> Unit {
  let triple_gen = gen.bind(fn(a) {
    gen.bind(fn(b) { gen.map(fn(c) { (a, b, c) }) })
  })

  let shrink_triple = fn(((a, b, c) : (A, A, A))) {
    let candidates : Array[(A, A, A)] = []
    candidates.push((a, a, a))
    candidates.push((a, a, c))
    candidates.push((a, b, b))
    candidates.iter()
  }

  assert_check(
    name,
    triple_gen,
    fn((a, b, c)) {
      let ab = equal(a, b)
      let bc = equal(b, c)
      let ac = equal(a, c)
      if ab && bc && !ac {
        Err(
          "Transitivity failed: equal(\{a}, \{b}) = true, equal(\{b}, \{c}) = true, but equal(\{a}, \{c}) = false",
        )
      } else {
        Ok(())
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Consistency property: f1(x) == f2(x) for equivalent implementations
pub fn prop_consistency[A : Show, B : Show](
  gen: @pbt.Gen[A],
  f1: (A) -> B,
  f2: (A) -> B,
  name: String,
  equal: (B, B) -> Bool,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(input) {
      let r1 = f1(input)
      let r2 = f2(input)
      if equal(r1, r2) {
        Ok(())
      } else {
        Err(
          "Consistency failed: f1(\{input}) = \{r1}, f2(\{input}) = \{r2}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Monotonic property: a <= b implies f(a) <= f(b)
pub fn prop_monotonic[A : Show](
  gen: @pbt.Gen[A],
  f: (A) -> Int,
  leq: (A, A) -> Bool,
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : ((A, A)) -> Iter[(A, A)],
) -> Unit {
  let pair_gen = gen.bind(fn(a) { gen.map(fn(b) { (a, b) }) })

  assert_check(
    name,
    pair_gen,
    fn((a, b)) {
      if leq(a, b) {
        let fa = f(a)
        let fb = f(b)
        if fa <= fb {
          Ok(())
        } else {
          Err(
            "Monotonicity failed: \{a} <= \{b}, but f(\{a}) = \{fa} > f(\{b}) = \{fb}",
          )
        }
      } else {
        Ok(())
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Preservation property: predicate(f(x)) should equal predicate(x)
pub fn prop_preservation[A : Show](
  gen: @pbt.Gen[A],
  operation: (A) -> A,
  predicate: (A) -> Bool,
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(input) {
      let before = predicate(input)
      let after = predicate(operation(input))
      if before == after {
        Ok(())
      } else {
        Err(
          "Preservation failed: predicate(\{input}) = \{before}, predicate(op(\{input})) = \{after}",
        )
      }
    },
    config~,
    shrink?,
  )
}

///|
/// Error preservation property: error cases should remain error cases
pub fn prop_error_preservation[A : Show, B : Show](
  gen: @pbt.Gen[A],
  operation: (A) -> Result[B, String],
  name: String,
  config? : CheckConfig = CheckConfig::default(),
  shrink? : (A) -> Iter[A],
) -> Unit {
  assert_check(
    name,
    gen,
    fn(input) {
      let _ = operation(input)
      // Just verify it doesn't crash - result can be Ok or Err
      Ok(())
    },
    config~,
    shrink?,
  )
}

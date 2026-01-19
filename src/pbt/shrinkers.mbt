///|
/// PBT Shrinkers for DuckDB MoonBit bindings
/// Provides shrinking functions for finding minimal counterexamples

///|
/// Shrink a date tuple toward simpler values
pub fn shrink_date_tuple(input: (Int, Int, Int)) -> Iter[(Int, Int, Int)] {
  let (year, month, day) = input
  let candidates : Array[(Int, Int, Int)] = []

  // Always try epoch first
  candidates.push((1970, 1, 1))

  // Try simpler year
  if year > 1970 {
    candidates.push((1970, month, day))
    candidates.push(((year + 1970) / 2, month, day))
  }

  // Try simpler month
  if month > 1 {
    candidates.push((year, 1, day))
    candidates.push((year, month / 2, day))
  }

  // Try simpler day
  if day > 1 {
    candidates.push((year, month, 1))
    candidates.push((year, month, day / 2))
  }

  // Try leap year boundaries
  if month == 2 && day == 29 {
    candidates.push((year, 2, 28))
  }

  candidates.iter()
}

///|
/// Shrink a timestamp tuple toward simpler values
pub fn shrink_timestamp_tuple(
  input: (Int, Int, Int, Int, Int, Int),
) -> Iter[(Int, Int, Int, Int, Int, Int)] {
  let (year, month, day, hour, minute, second) = input
  let candidates : Array[(Int, Int, Int, Int, Int, Int)] = []

  // Try epoch
  candidates.push((1970, 1, 1, 0, 0, 0))

  // Zero out time components
  if hour != 0 || minute != 0 || second != 0 {
    candidates.push((year, month, day, 0, 0, 0))
  }

  // Simplify date
  if year > 1970 {
    candidates.push((1970, month, day, hour, minute, second))
  }
  if month > 1 {
    candidates.push((year, 1, day, hour, minute, second))
  }
  if day > 1 {
    candidates.push((year, month, 1, hour, minute, second))
  }

  // Simplify time
  if hour > 0 {
    candidates.push((year, month, day, 0, minute, second))
  }
  if minute > 0 {
    candidates.push((year, month, day, hour, 0, second))
  }
  if second > 0 {
    candidates.push((year, month, day, hour, minute, 0))
  }

  candidates.iter()
}

///|
/// Shrink a Decimal toward simpler values
pub fn shrink_decimal(d: Decimal) -> Iter[Decimal] {
  let candidates : Array[Decimal] = []

  // Try zero
  if d.lower != 0 || d.upper != 0 {
    candidates.push(Decimal::{
      width: d.width,
      scale: d.scale,
      lower: 0,
      upper: 0,
    })
  }

  // Try simpler scale
  if d.scale > 0 {
    candidates.push(Decimal::{
      width: d.width,
      scale: d.scale / 2,
      lower: d.lower,
      upper: d.upper,
    })
    candidates.push(Decimal::{
      width: d.width,
      scale: 0,
      lower: d.lower,
      upper: d.upper,
    })
  }

  // Try simpler width
  if d.width > 1 {
    candidates.push(Decimal::{
      width: d.width / 2 + 1,
      scale: d.scale,
      lower: d.lower,
      upper: d.upper,
    })
  }

  // Try halving the value
  if d.lower != 0 {
    candidates.push(Decimal::{
      width: d.width,
      scale: d.scale,
      lower: d.lower / 2,
      upper: d.upper,
    })
  }

  candidates.iter()
}

///|
/// Shrink an Interval toward simpler values
pub fn shrink_interval(i: Interval) -> Iter[Interval] {
  let candidates : Array[Interval] = []

  // Try zero
  if i.months != 0 || i.days != 0 || i.micros != 0 {
    candidates.push(Interval::{
      months: 0,
      days: 0,
      micros: 0,
    })
  }

  // Try zeroing individual components
  if i.months != 0 {
    candidates.push(Interval::{
      months: 0,
      days: i.days,
      micros: i.micros,
    })
    candidates.push(Interval::{
      months: i.months / 2,
      days: i.days,
      micros: i.micros,
    })
  }

  if i.days != 0 {
    candidates.push(Interval::{
      months: i.months,
      days: 0,
      micros: i.micros,
    })
    candidates.push(Interval::{
      months: i.months,
      days: i.days / 2,
      micros: i.micros,
    })
  }

  if i.micros != 0 {
    candidates.push(Interval::{
      months: i.months,
      days: i.days,
      micros: 0,
    })
    candidates.push(Interval::{
      months: i.months,
      days: i.days,
      micros: i.micros / 2,
    })
  }

  candidates.iter()
}

///|
/// Shrink a Value toward simpler values
pub fn shrink_value(v: Value) -> Iter[Value] {
  let candidates : Array[Value] = []

  match v {
    Value::Int(n) =>
      if n != 0 {
        candidates.push(Value::Int(0))
        candidates.push(Value::Int(n / 2))
        candidates.push(Value::Int(1))
        if n < 0 {
          candidates.push(Value::Int(-1))
        }
      }
    Value::Double(d) =>
      if d != 0.0 {
        candidates.push(Value::Double(0.0))
        candidates.push(Value::Double(d / 2.0))
        candidates.push(Value::Double(1.0))
        candidates.push(Value::Double(-1.0))
      }
    Value::String(s) => {
      candidates.push(Value::String(""))
      if s.length() > 1 {
        candidates.push(Value::String("x"))
        candidates.push(Value::String(
          s.view(start_offset=0, end_offset=s.length() / 2).to_string(),
        ))
      }
    }
    Value::Date(days) => {
      if days != 0 {
        candidates.push(Value::Date(0))
        candidates.push(Value::Date(days / 2))
      }
    }
    Value::Timestamp(micros) => {
      if micros != 0L {
        candidates.push(Value::Timestamp(0L))
        candidates.push(Value::Timestamp(micros / 2L))
      }
    }
    Value::Decimal(d) => {
      for shrunk in shrink_decimal(d) {
        candidates.push(Value::Decimal(shrunk))
      }
    }
    Value::Blob(bytes) => {
      candidates.push(Value::Blob(Bytes::empty()))
      if bytes.length() > 0 {
        // Try smaller blob - would need Bytes slicing
        candidates.push(Value::Blob(Bytes::from_array([0.to_uint8()])))
      }
    }
    Value::Bool(_) => {
      candidates.push(Value::Bool(false))
      candidates.push(Value::Bool(true))
    }
    Value::Null => ()
  }

  candidates.iter()
}

///|
/// Shrink Bytes toward simpler values
pub fn shrink_bytes(b: Bytes) -> Iter[Bytes] {
  let candidates : Array[Bytes] = []

  let len = b.length()
  if len > 0 {
    // Always try empty
    candidates.push(Bytes::empty())

    // Try single byte
    candidates.push(Bytes::from_array([0.to_uint8()]))

    // Try half length
    if len > 1 {
      let half = len / 2
      let mut arr = Array::make(half, 0.to_uint8())
      let mut i = 0
      while i < half {
        arr[i] = b[i]
        i = i + 1
      }
      candidates.push(Bytes::from_array(arr))
    }
  }

  candidates.iter()
}

///|
/// Shrink a List toward simpler values
pub fn shrink_list(l: List) -> Iter[List] {
  let candidates : Array[List] = []

  let len = l.elements.length()
  if len > 0 {
    // Try empty
    candidates.push(List::{ elements: [] })

    // Try single element
    if len > 1 {
      candidates.push(List::{
        elements: [l.elements[0]],
      })
    }

    // Try half
    if len > 2 {
      let half = len / 2
      candidates.push(List::{
        elements: l.elements.view(start_offset=0, end_offset=half).to_array(),
      })
    }
  }

  candidates.iter()
}

///|
/// Shrink a Struct toward simpler values
pub fn shrink_struct(s: Struct) -> Iter[Struct] {
  let candidates : Array[Struct] = []

  let field_count = s.fields.length()
  if field_count > 0 {
    // Try empty
    candidates.push(Struct::{
      fields: [],
      values: [],
    })

    // Try single field
    if field_count > 1 {
      candidates.push(Struct::{
        fields: [s.fields[0]],
        values: [s.values[0]],
      })
    }

    // Try half
    if field_count > 2 {
      let half = field_count / 2
      candidates.push(Struct::{
        fields: s.fields.view(start_offset=0, end_offset=half).to_array(),
        values: s.values.view(start_offset=0, end_offset=half).to_array(),
      })
    }
  }

  candidates.iter()
}

///|
/// Shrink a Map toward simpler values
pub fn shrink_map(m: Map) -> Iter[Map] {
  let candidates : Array[Map] = []

  let size = m.keys.length()
  if size > 0 {
    // Try empty
    candidates.push(Map::{
      keys: [],
      values: [],
    })

    // Try single entry
    if size > 1 {
      candidates.push(Map::{
        keys: [m.keys[0]],
        values: [m.values[0]],
      })
    }

    // Try half
    if size > 2 {
      let half = size / 2
      candidates.push(Map::{
        keys: m.keys.view(start_offset=0, end_offset=half).to_array(),
        values: m.values.view(start_offset=0, end_offset=half).to_array(),
      })
    }
  }

  candidates.iter()
}

///|
/// Shrink an array of strings
pub fn shrink_string_array(arr: Array[String]) -> Iter[Array[String]] {
  let len = arr.length()
  if len == 0 {
    return [].iter()
  }

  let candidates : Array[Array[String]] = []

  // Try empty
  candidates.push([])

  // Try single element
  if len > 1 {
    candidates.push([arr[0]])
  }

  // Try half
  if len > 2 {
    let half = len / 2
    candidates.push(arr.view(start_offset=0, end_offset=half).to_array())
  }

  candidates.iter()
}

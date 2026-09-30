///|
// Whitebox tests for the shared `append_chunk` validation/spec layer
// (issue #68): identical behavior on every backend since all public
// implementations funnel through `append_chunk_specs`.

///|
fn spec_vector(logical_type : ColumnType, data : VectorData) -> Vector {
  Vector::all_valid(logical_type, data)
}

///|
fn spec_i32(n : Int) -> VectorData {
  VectorData::Int32(FixedArray::make(n, 7))
}

///|
test "append_chunk_specs rejects malformed chunks" {
  let backend = Backend::Native
  // Empty chunk.
  match append_chunk_specs({ columns: [], vectors: [], }, backend) {
    Err(DuckDBError::InvalidArgument(argument~, ..)) =>
      inspect(argument, content="chunk")
    _ => fail("expected InvalidArgument for empty chunk")
  }
  // Name/vector count mismatch.
  let mismatched : VectorChunk = {
    columns: ["a", "b"],
    vectors: [spec_vector(ColumnType::Integer, spec_i32(1))],
  }
  match append_chunk_specs(mismatched, backend) {
    Err(DuckDBError::InvalidArgument(..)) => ()
    _ => fail("expected InvalidArgument for count mismatch")
  }
  // Ragged vectors.
  let ragged : VectorChunk = {
    columns: ["a", "b"],
    vectors: [
      spec_vector(ColumnType::Integer, spec_i32(2)),
      spec_vector(ColumnType::Varchar, VectorData::Varchar(["x"])),
    ],
  }
  match append_chunk_specs(ragged, backend) {
    Err(DuckDBError::InvalidArgument(..)) => ()
    _ => fail("expected InvalidArgument for ragged vectors")
  }
  // Payload shorter than validity mask.
  let bad_validity : VectorChunk = {
    columns: ["a"],
    vectors: [
      Vector::{
        logical_type: ColumnType::Integer,
        data: spec_i32(2),
        validity: FixedArray::make(3, true),
      },
    ],
  }
  match append_chunk_specs(bad_validity, backend) {
    Err(DuckDBError::InvalidArgument(..)) => ()
    _ => fail("expected InvalidArgument for bad validity")
  }
}

///|
test "append_chunk_specs maps logical/storage pairs to type ids" {
  let backend = Backend::Native
  let chunk : VectorChunk = {
    columns: ["i", "s", "d"],
    vectors: [
      spec_vector(ColumnType::Integer, spec_i32(2)),
      spec_vector(ColumnType::Varchar, VectorData::Varchar(["a", "b"])),
      spec_vector(
        ColumnType::Double,
        VectorData::Double(FixedArray::make(2, 0.5)),
      ),
    ],
  }
  match append_chunk_specs(chunk, backend) {
    Ok((rows, type_ids, aux)) => {
      inspect(rows, content="2")
      inspect(Array::from_fixed_array(type_ids), content="[4, 17, 11]")
      inspect(Array::from_fixed_array(aux), content="[0, 0, 0]")
    }
    Err(err) => fail("expected specs, got \{err.message()}")
  }
  // Logical/payload mismatch is InvalidArgument, not Unsupported.
  let wrong : VectorChunk = {
    columns: ["a"],
    vectors: [spec_vector(ColumnType::Varchar, spec_i32(1))],
  }
  match append_chunk_specs(wrong, backend) {
    Err(DuckDBError::InvalidArgument(..)) => ()
    _ => fail("expected InvalidArgument for logical/payload mismatch")
  }
}

///|
test "append_chunk_specs reports Unsupported for nested and exotic types" {
  let backend = Backend::Native
  let list_vector = spec_vector(
    ColumnType::List,
    VectorData::List(
      child=spec_vector(ColumnType::Integer, spec_i32(1)),
      offsets=FixedArray::from_array([0UL]),
      lengths=FixedArray::from_array([1UL]),
    ),
  )
  let chunk : VectorChunk = { columns: ["l"], vectors: [list_vector], }
  match append_chunk_specs(chunk, backend) {
    Err(DuckDBError::Unsupported(feature~, backend~)) => {
      inspect(feature.contains("LIST"), content="true")
      inspect(backend, content="native")
    }
    _ => fail("expected Unsupported for list column")
  }
  // Enum payloads are unsupported even though they share the string storage.
  let enum_vector = spec_vector(ColumnType::Enum, VectorData::Varchar(["x"]))
  match
    append_chunk_specs({ columns: ["e"], vectors: [enum_vector], }, backend) {
    Err(DuckDBError::Unsupported(feature~, ..)) =>
      inspect(feature.contains("ENUM"), content="true")
    _ => fail("expected Unsupported for enum column")
  }
}

///|
test "append_chunk_specs decimal meta" {
  let backend = Backend::Native
  let cells = FixedArray::from_array([
    decimal_from_hugeint(12345UL, 0L, 10, 2),
    decimal_from_hugeint(100UL, 0L, 10, 2),
  ])
  let chunk : VectorChunk = {
    columns: ["d"],
    vectors: [spec_vector(ColumnType::Decimal, VectorData::Decimal(cells))],
  }
  match append_chunk_specs(chunk, backend) {
    Ok((rows, type_ids, aux)) => {
      inspect(rows, content="2")
      inspect(Array::from_fixed_array(type_ids), content="[19]")
      inspect(Array::from_fixed_array(aux), content="[655362]") // (10<<16)|2
    }
    Err(err) => fail("expected decimal specs, got \{err.message()}")
  }
  // Mixed width/scale is rejected.
  let mixed = FixedArray::from_array([
    decimal_from_hugeint(12345UL, 0L, 10, 2),
    decimal_from_hugeint(100UL, 0L, 18, 4),
  ])
  let bad : VectorChunk = {
    columns: ["d"],
    vectors: [spec_vector(ColumnType::Decimal, VectorData::Decimal(mixed))],
  }
  match append_chunk_specs(bad, backend) {
    Err(DuckDBError::InvalidArgument(..)) => ()
    _ => fail("expected InvalidArgument for mixed decimal width/scale")
  }
}

///|
test "column_type_to_id round-trips through column_type_from_id" {
  for id in 0..<40 {
    let column_type = column_type_from_id(id)
    if column_type_to_id(column_type) != id {
      fail("type id \{id} round-tripped to \{column_type_to_id(column_type)}")
    }
  }
}

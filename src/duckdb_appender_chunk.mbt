///|
// ============================================================================
// Vectorized appender ingestion (issue #68): shared builders + validation.
// ============================================================================
//
// `Appender::append_chunk` bulk-inserts a canonical `VectorChunk` with one
// backend call per chunk instead of per-cell FFI calls. This file holds the
// backend-neutral pieces: `Vector`/`VectorChunk` builders, structural
// validation, and the logical-type -> DuckDB type-id mapping each backend's
// writer builds on. Both the native and Node implementations reuse
// `append_chunk_specs` so they reject invalid chunks with identical errors.

///|
/// Build a `Vector` from a payload with every row marked valid.
pub fn Vector::all_valid(
  logical_type : ColumnType,
  data : VectorData,
) -> Vector {
  { logical_type, data, validity: FixedArray::make(data.len(), true), }
}

///|
/// Row count of a `VectorData` payload (0 for an empty nested payload).
pub fn VectorData::len(self : VectorData) -> Int {
  match self {
    Bool(values) => values.length()
    Int32(values) => values.length()
    Int64(values) => values.length()
    UInt64(values) => values.length()
    Float(values) => values.length()
    Double(values) => values.length()
    Decimal(values) => values.length()
    Interval(values) => values.length()
    Varchar(values) => values.length()
    Blob(values) => values.length()
    Any(values) => values.length()
    HugeInt(lower~, ..) => lower.length()
    List(offsets~, ..) => offsets.length()
    Map(offsets~, ..) => offsets.length()
    Struct(children~, ..) =>
      if children.length() == 0 {
        0
      } else {
        children[0].len()
      }
  }
}

///|
/// Build a validated `VectorChunk` from column names and vectors — every
/// vector must carry the same row count.
pub fn VectorChunk::new(
  columns : Array[String],
  vectors : Array[Vector],
) -> Result[VectorChunk, DuckDBError] {
  match validate_vector_chunk({ columns, vectors, }) {
    Ok(_) => Ok({ columns, vectors, })
    Err(err) => Err(err)
  }
}

///|
/// DuckDB type id for a `ColumnType` (inverse of `column_type_from_id`);
/// `Unknown(id)` round-trips its payload.
pub fn column_type_to_id(column_type : ColumnType) -> Int {
  match column_type {
    Invalid => 0
    Boolean => 1
    TinyInt => 2
    SmallInt => 3
    Integer => 4
    BigInt => 5
    UTinyInt => 6
    USmallInt => 7
    UInteger => 8
    UBigInt => 9
    Float => 10
    Double => 11
    Timestamp => 12
    Date => 13
    Time => 14
    Interval => 15
    HugeInt => 16
    Varchar => 17
    Blob => 18
    Decimal => 19
    TimestampS => 20
    TimestampMs => 21
    TimestampNs => 22
    Enum => 23
    List => 24
    Struct => 25
    Map => 26
    Uuid => 27
    Union => 28
    Bit => 29
    TimeTz => 30
    TimestampTz => 31
    UHugeInt => 32
    Array => 33
    Any => 34
    Bignum => 35
    SqlNull => 36
    StringLiteral => 37
    IntegerLiteral => 38
    TimeNs => 39
    Unknown(id) => id
  }
}

///|
/// DuckDB-style type name for diagnostics.
fn column_type_name(column_type : ColumnType) -> String {
  match column_type {
    Invalid => "INVALID"
    Boolean => "BOOLEAN"
    TinyInt => "TINYINT"
    SmallInt => "SMALLINT"
    Integer => "INTEGER"
    BigInt => "BIGINT"
    UTinyInt => "UTINYINT"
    USmallInt => "USMALLINT"
    UInteger => "UINTEGER"
    UBigInt => "UBIGINT"
    Float => "FLOAT"
    Double => "DOUBLE"
    Timestamp => "TIMESTAMP"
    Date => "DATE"
    Time => "TIME"
    Interval => "INTERVAL"
    HugeInt => "HUGEINT"
    UHugeInt => "UHUGEINT"
    Varchar => "VARCHAR"
    Blob => "BLOB"
    Decimal => "DECIMAL"
    TimestampS => "TIMESTAMP_S"
    TimestampMs => "TIMESTAMP_MS"
    TimestampNs => "TIMESTAMP_NS"
    Enum => "ENUM"
    List => "LIST"
    Struct => "STRUCT"
    Map => "MAP"
    Array => "ARRAY"
    Uuid => "UUID"
    Union => "UNION"
    Bit => "BIT"
    TimeTz => "TIME_TZ"
    TimestampTz => "TIMESTAMP_TZ"
    Any => "ANY"
    Bignum => "BIGNUM"
    SqlNull => "SQLNULL"
    StringLiteral => "STRING_LITERAL"
    IntegerLiteral => "INTEGER_LITERAL"
    TimeNs => "TIME_NS"
    Unknown(id) => "UNKNOWN(\{id})"
  }
}

///|
/// Structural validation shared by every `append_chunk` implementation:
/// non-empty column list, matching names/vectors, per-vector payload/
/// validity length agreement, and uniform row counts. Returns the chunk's
/// row count.
fn validate_vector_chunk(chunk : VectorChunk) -> Result[Int, DuckDBError] {
  let ncols = chunk.vectors.length()
  if ncols == 0 {
    return Err(
      DuckDBError::InvalidArgument(
        argument="chunk",
        reason="must contain at least one column",
      ),
    )
  }
  if chunk.columns.length() != ncols {
    return Err(
      InvalidArgument(
        argument="chunk",
        reason="column names (\{chunk.columns.length()}) and vectors (\{ncols}) differ",
      ),
    )
  }
  let rows = chunk.vectors[0].len()
  for i, vector in chunk.vectors {
    if vector.data.len() != vector.validity.length() {
      return Err(
        InvalidArgument(
          argument="chunk",
          reason="column \{i} payload length \{vector.data.len()} does not match validity length \{vector.validity.length()}",
        ),
      )
    }
    if vector.validity.length() != rows {
      return Err(
        InvalidArgument(
          argument="chunk",
          reason="column \{i} has \{vector.validity.length()} rows, expected \{rows}",
        ),
      )
    }
  }
  Ok(rows)
}

///|
/// Resolve a vector to its `(duckdb type id, aux meta)` write spec, or the
/// `DuckDBError` `append_chunk` must return for it. `aux` is only meaningful
/// for DECIMAL columns where it packs `(width << 16) | scale`; every other
/// supported column reports 0. Logical-type/storage mismatches produce
/// `InvalidArgument`; column types no backend can ingest produce
/// `Unsupported` so native and Node report identical errors.
fn column_write_spec(
  vector : Vector,
  backend : Backend,
) -> Result[(Int, Int), DuckDBError] {
  let unsupported = fn() {
    DuckDBError::Unsupported(
      feature="append_chunk for column type \{column_type_name(vector.logical_type)}",
      backend=backend.name(),
    )
  }
  let mismatch = fn() {
    DuckDBError::InvalidArgument(
      argument="chunk",
      reason="logical type \{column_type_name(vector.logical_type)} does not match its column payload",
    )
  }
  match vector.logical_type {
    ColumnType::Invalid
    | ColumnType::Enum
    | ColumnType::List
    | ColumnType::Struct
    | ColumnType::Map
    | ColumnType::Array
    | ColumnType::Union
    | ColumnType::Bit
    | ColumnType::TimeTz
    | ColumnType::Any
    | ColumnType::Bignum
    | ColumnType::SqlNull
    | ColumnType::StringLiteral
    | ColumnType::IntegerLiteral
    | ColumnType::Unknown(_) => return Err(unsupported())
    _ => ()
  }
  match vector.data {
    Bool(_) =>
      if vector.logical_type == ColumnType::Boolean {
        Ok((1, 0))
      } else {
        Err(mismatch())
      }
    Int32(_) =>
      match vector.logical_type {
        ColumnType::TinyInt => Ok((2, 0))
        ColumnType::SmallInt => Ok((3, 0))
        ColumnType::Integer => Ok((4, 0))
        ColumnType::UTinyInt => Ok((6, 0))
        ColumnType::USmallInt => Ok((7, 0))
        ColumnType::Date => Ok((13, 0))
        _ => Err(mismatch())
      }
    Int64(_) =>
      match vector.logical_type {
        ColumnType::BigInt => Ok((5, 0))
        ColumnType::UInteger => Ok((8, 0))
        ColumnType::Timestamp => Ok((12, 0))
        ColumnType::Time => Ok((14, 0))
        ColumnType::TimestampS => Ok((20, 0))
        ColumnType::TimestampMs => Ok((21, 0))
        ColumnType::TimestampNs => Ok((22, 0))
        ColumnType::TimestampTz => Ok((31, 0))
        ColumnType::TimeNs => Ok((39, 0))
        _ => Err(mismatch())
      }
    UInt64(_) =>
      if vector.logical_type == ColumnType::UBigInt {
        Ok((9, 0))
      } else {
        Err(mismatch())
      }
    Float(_) =>
      if vector.logical_type == ColumnType::Float {
        Ok((10, 0))
      } else {
        Err(mismatch())
      }
    Double(_) =>
      if vector.logical_type == ColumnType::Double {
        Ok((11, 0))
      } else {
        Err(mismatch())
      }
    Varchar(_) =>
      if vector.logical_type == ColumnType::Varchar {
        Ok((17, 0))
      } else {
        Err(mismatch())
      }
    Blob(_) =>
      if vector.logical_type == ColumnType::Blob {
        Ok((18, 0))
      } else {
        Err(mismatch())
      }
    Decimal(cells) =>
      match vector.logical_type {
        ColumnType::Decimal => {
          let mut meta = (1 << 16) | 0
          for i, cell in cells {
            if cell.width < 1 ||
              cell.width > 38 ||
              cell.scale < 0 ||
              cell.scale > cell.width {
              return Err(
                InvalidArgument(
                  argument="chunk",
                  reason="decimal width/scale (\{cell.width},\{cell.scale}) out of range",
                ),
              )
            }
            let cell_meta = (cell.width << 16) | cell.scale
            if i == 0 {
              meta = cell_meta
            } else if cell_meta != meta {
              return Err(
                InvalidArgument(
                  argument="chunk",
                  reason="decimal column mixes (\{meta >> 16},\{meta & 0xffff}) and (\{cell.width},\{cell.scale}) width/scale",
                ),
              )
            }
          }
          Ok((19, meta))
        }
        _ => Err(mismatch())
      }
    Interval(_) =>
      if vector.logical_type == ColumnType::Interval {
        Ok((15, 0))
      } else {
        Err(mismatch())
      }
    HugeInt(..) =>
      match vector.logical_type {
        ColumnType::HugeInt => Ok((16, 0))
        ColumnType::Uuid => Ok((27, 0))
        ColumnType::UHugeInt => Ok((32, 0))
        _ => Err(mismatch())
      }
    List(..) | Struct(..) | Map(..) | Any(_) => Err(mismatch())
  }
}

///|
/// Validate `chunk` for `append_chunk` on `backend` and precompute the
/// per-column `(type id, aux)` write spec in one pass. On success returns
/// `(rows, type_ids, aux)` with `type_ids.length() == vectors.length()`.
fn append_chunk_specs(
  chunk : VectorChunk,
  backend : Backend,
) -> Result[(Int, FixedArray[Int], FixedArray[Int]), DuckDBError] {
  let rows = match validate_vector_chunk(chunk) {
    Ok(rows) => rows
    Err(err) => return Err(err)
  }
  let ncols = chunk.vectors.length()
  let type_ids = FixedArray::make(ncols, 0)
  let aux = FixedArray::make(ncols, 0)
  for i, vector in chunk.vectors {
    match column_write_spec(vector, backend) {
      Ok((type_id, meta)) => {
        type_ids[i] = type_id
        aux[i] = meta
      }
      Err(err) => return Err(err)
    }
  }
  Ok((rows, type_ids, aux))
}

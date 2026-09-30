///|
// ============================================================================
// Arrow results on canonical typed vectors (issue #62)
// ============================================================================
//
// `Connection::query_arrow` previously re-encoded every column: as JSON on
// the JS backends and as a custom packed-byte format on native. The result
// is now eagerly materialized into the canonical `VectorChunk` typed-vector
// model shared with `Connection::query_chunks`, so no JSON round-trip or
// byte re-encoding is involved on any backend. Int64/UInt64 columns keep
// full precision, and per-row NULL masks come from the same validity layout
// used by every other result path.
//
// `ArrowResult` stays an opaque handle: `close` releases the backend-native
// result, and `to_chunks`/`get_schema`/`close` report `Closed` afterwards.
// The legacy per-column `get_column_*` accessors are kept as deprecated
// forwarders in `deprecated.mbt`; new code should use `to_chunks` and the
// `Vector` accessors (`ints`, `int64s`, `uint64s`, `doubles`, `strings`,
// `decimals`, `list_parts`, `value_at`, `is_null`, ...).

///|
/// One Arrow field of a result schema.
pub struct ArrowField {
  name : String
  nullable : Bool
  type_id : String
}

///|
/// Result schema: one `ArrowField` per column, in column order.
pub struct ArrowSchemaInfo {
  fields : Array[ArrowField]
}

///|
/// Materialized `query_arrow` result. Opaque to callers; the canonical
/// `VectorChunk`s are exposed via `to_chunks`, and the backend-native handle
/// is released by `close`.
pub struct ArrowResult {
  priv mut handle : ArrowHandle?
  priv columns : Array[String]
  priv column_types : Array[ColumnType]
  priv nullables : FixedArray[Bool]
  priv chunks : Array[VectorChunk]
  priv mut closed : Bool
}

///|
/// Number of result columns.
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  self.columns.length()
}

///|
/// Total rows across all materialized chunks.
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  let mut rows = 0
  for chunk in self.chunks {
    rows = rows + chunk.row_count()
  } nobreak {
    ()
  }
  rows
}

///|
/// The canonical typed-vector representation of the whole result.
///
/// `Err(Closed)` once `close` has released the result.
pub fn ArrowResult::to_chunks(
  self : ArrowResult,
) -> Result[Array[VectorChunk], DuckDBError] {
  if self.closed {
    Err(DuckDBError::Closed(resource="arrow result"))
  } else {
    Ok(self.chunks)
  }
}

///|
/// Result schema: column names, nullability, and a flat type id per field.
/// `type_id` keeps the legacy primitive names ("bool"/"int32"/"int64"/
/// "double"/"string") and covers the wider `ColumnType` set ("uint32",
/// "uint64", "decimal", "date", "timestamp", "interval", "hugeint", "uuid",
/// "list", "struct", "map", ...). `Err(Closed)` once closed.
pub fn ArrowResult::get_schema(
  self : ArrowResult,
) -> Result[ArrowSchemaInfo, DuckDBError] {
  if self.closed {
    Err(DuckDBError::Closed(resource="arrow result"))
  } else {
    let fields = Array::makei(self.columns.length(), fn(col) {
      {
        name: self.columns[col],
        nullable: if col < self.nullables.length() {
          self.nullables[col]
        } else {
          true
        },
        type_id: arrow_field_type_id(self.column_types[col]),
      }
    })
    Ok({ fields, })
  }
}

///|
/// Flat type-id string for a `ColumnType` (see `get_schema`).
fn arrow_field_type_id(logical_type : ColumnType) -> String {
  match logical_type {
    ColumnType::Boolean => "bool"
    ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Integer => "int32"
    ColumnType::BigInt => "int64"
    ColumnType::UTinyInt | ColumnType::USmallInt | ColumnType::UInteger =>
      "uint32"
    ColumnType::UBigInt => "uint64"
    ColumnType::Float | ColumnType::Double => "double"
    ColumnType::Varchar | ColumnType::Enum | ColumnType::StringLiteral =>
      "string"
    ColumnType::Blob => "blob"
    ColumnType::Date => "date"
    ColumnType::Time | ColumnType::TimeNs | ColumnType::TimeTz => "time"
    ColumnType::Timestamp
    | ColumnType::TimestampS
    | ColumnType::TimestampMs
    | ColumnType::TimestampNs
    | ColumnType::TimestampTz => "timestamp"
    ColumnType::Interval => "interval"
    ColumnType::Decimal => "decimal"
    ColumnType::HugeInt | ColumnType::UHugeInt => "hugeint"
    ColumnType::Uuid => "uuid"
    ColumnType::List | ColumnType::Array => "list"
    ColumnType::Struct => "struct"
    ColumnType::Map => "map"
    ColumnType::Union => "union"
    ColumnType::Bit => "bit"
    _ => "string"
  }
}

///|
/// Release the backend-native result. Idempotent contract: the first call
/// frees the handle; calling `close` again reports `Closed` instead of
/// touching released resources.
pub fn ArrowResult::close(
  self : ArrowResult,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  if self.closed {
    on_done(Err(DuckDBError::Closed(resource="arrow result")))
    return
  }
  match self.handle {
    Some(handle) => arrow_result_close_handle(handle)
    None => ()
  }
  self.handle = None
  self.closed = true
  on_done(Ok(()))
}

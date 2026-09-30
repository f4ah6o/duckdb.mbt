///|
// ============================================================================
// JS backends: `Appender::append_chunk` (issue #68).
// ============================================================================
//
// Node supports real vectorized ingestion: the canonical `VectorChunk` is
// pooled into per-kind typed arrays and handed to `DuckDBDataChunk` +
// `DuckDBAppender.appendDataChunk` in a single FFI call — no per-cell
// MoonBit<->JS boundary crossings. The pool layout reuses the same kind tags
// as the chunked reader (`duckdb_vector_js.mbt`). The WASM backend has no
// appender at all, so `create_appender` already fails there; `append_chunk`
// additionally reports a structured `Unsupported` for completeness.

///|
/// Append a canonical chunk to `appender` in one call. `type_ids`/`metas` are
/// the per-column DuckDB type ids and aux data (`(width << 16) | scale` for
/// DECIMAL) from `append_chunk_specs`; `kinds`/`offsets` index the value
/// pools (same tags as `js_stream_next_vectors_ffi`); `null_flags` is a
/// `row * ncols + col` grid where 1 = NULL. All parameters are `FixedArray`
/// per the JS FFI boundary rules.
///
/// The result is delivered through `on_ok`/`on_err` callbacks rather than a
/// returned object: MoonBit's JS backend expects its tagged `Result`
/// representation, so a plain `{ ok, error }` object would not decode.
/// `appendDataChunk` is synchronous, so exactly one callback fires before
/// this call returns.
extern "js" fn js_appender_append_chunk_ffi(
  appender : Appender,
  type_ids : FixedArray[Int],
  metas : FixedArray[Int],
  kinds : FixedArray[Int],
  offsets : FixedArray[Int],
  rows : Int,
  null_flags : FixedArray[Int],
  pool_i32 : FixedArray[Int],
  pool_i64 : FixedArray[Int64],
  pool_f64 : FixedArray[Double],
  pool_str : FixedArray[String],
  pool_bytes : FixedArray[Bytes],
  on_ok : () -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(appender, type_ids, metas, kinds, offsets, rows, null_flags, pool_i32, pool_i64, pool_f64, pool_str, pool_bytes, on_ok, on_err) => {
  #|    if (appender && appender.closed) { on_err("ERR_CLOSED:appender"); return; }
  #|    if (!(appender && appender.kind === "appender" && appender.appender)) {
  #|      on_err("invalid appender"); return;
  #|    }
  #|    const api = appender.api;
  #|    if (!api) {
  #|      on_err("WASM backend does not support append_chunk"); return;
  #|    }
  #|    try {
  #|      const U64 = (v) => BigInt.asUintN(64, v);
  #|      const S128 = (hi, lo) =>
  #|        BigInt.asIntN(128, (BigInt(hi) << 64n) | U64(lo));
  #|      const U128 = (hi, lo) =>
  #|        (U64(hi) << 64n) | U64(lo);
  #|      const toBytes = (b) => {
  #|        const u = new Uint8Array(b.byteLength);
  #|        for (let i = 0; i < b.byteLength; i++) u[i] = b.readUint8(i);
  #|        return u;
  #|      };
  #|      const makeType = (tid, meta) => {
  #|        switch (tid) {
  #|          case 1: return new api.DuckDBBooleanType();
  #|          case 2: return new api.DuckDBTinyIntType();
  #|          case 3: return new api.DuckDBSmallIntType();
  #|          case 4: return new api.DuckDBIntegerType();
  #|          case 5: return new api.DuckDBBigIntType();
  #|          case 6: return new api.DuckDBUTinyIntType();
  #|          case 7: return new api.DuckDBUSmallIntType();
  #|          case 8: return new api.DuckDBUIntegerType();
  #|          case 9: return new api.DuckDBUBigIntType();
  #|          case 10: return new api.DuckDBFloatType();
  #|          case 11: return new api.DuckDBDoubleType();
  #|          case 12: return new api.DuckDBTimestampType();
  #|          case 13: return new api.DuckDBDateType();
  #|          case 14: return new api.DuckDBTimeType();
  #|          case 15: return new api.DuckDBIntervalType();
  #|          case 16: return new api.DuckDBHugeIntType();
  #|          case 17: return new api.DuckDBVarCharType();
  #|          case 18: return new api.DuckDBBlobType();
  #|          case 19: return new api.DuckDBDecimalType(meta >> 16, meta & 0xffff);
  #|          case 20: return new api.DuckDBTimestampSecondsType();
  #|          case 21: return new api.DuckDBTimestampMillisecondsType();
  #|          case 22: return new api.DuckDBTimestampNanosecondsType();
  #|          case 27: return new api.DuckDBUUIDType();
  #|          case 31: return new api.DuckDBTimestampTZType();
  #|          case 32: return new api.DuckDBUHugeIntType();
  #|          case 39: return new api.DuckDBTimeNSType();
  #|          default: throw new Error("unsupported column type id " + tid);
  #|        }
  #|      };
  #|      const ncols = type_ids.length;
  #|      const types = [];
  #|      for (let c = 0; c < ncols; c++) {
  #|        types.push(makeType(type_ids[c] | 0, metas[c] | 0));
  #|      }
  #|      const columns = [];
  #|      for (let c = 0; c < ncols; c++) {
  #|        const kind = kinds[c] | 0;
  #|        const tid = type_ids[c] | 0;
  #|        const meta = metas[c] | 0;
  #|        const off = offsets[c] | 0;
  #|        const column = new Array(rows);
  #|        for (let r = 0; r < rows; r++) {
  #|          if (null_flags[r * ncols + c]) { column[r] = null; continue; }
  #|          switch (kind) {
  #|            case 0: // BOOL
  #|              column[r] = pool_i32[off + r] !== 0;
  #|              break;
  #|            case 1: { // I32
  #|              const v = pool_i32[off + r];
  #|              column[r] = tid === 13 ? new api.DuckDBDateValue(v) : v;
  #|              break;
  #|            }
  #|            case 2: { // I64
  #|              const v = pool_i64[off + r];
  #|              if (tid === 14) column[r] = new api.DuckDBTimeValue(v);
  #|              else if (tid === 12) column[r] = new api.DuckDBTimestampValue(v);
  #|              else if (tid === 20)
  #|                // Canonical Int64 payloads carry microseconds; the
  #|                // per-variant constructors take their native unit.
  #|                column[r] = new api.DuckDBTimestampSecondsValue(v / 1000000n);
  #|              else if (tid === 21)
  #|                column[r] = new api.DuckDBTimestampMillisecondsValue(
  #|                  v / 1000n,
  #|                );
  #|              else if (tid === 22)
  #|                column[r] = new api.DuckDBTimestampNanosecondsValue(v);
  #|              else if (tid === 31)
  #|                column[r] = new api.DuckDBTimestampTZValue(v);
  #|              else if (tid === 39) column[r] = new api.DuckDBTimeNSValue(v);
  #|              else column[r] = v;
  #|              break;
  #|            }
  #|            case 3: // U64 (UBIGINT)
  #|              column[r] = U64(pool_i64[off + r]);
  #|              break;
  #|            case 4: // F32 widened to double
  #|            case 5: // F64
  #|              column[r] = pool_f64[off + r];
  #|              break;
  #|            case 6: // STR
  #|              column[r] = pool_str[off + r];
  #|              break;
  #|            case 7: // BYTES -> BLOB
  #|              column[r] = new api.DuckDBBlobValue(toBytes(pool_bytes[off + r]));
  #|              break;
  #|            case 8: { // I128
  #|              const hi = pool_i64[off + r * 2];
  #|              const lo = pool_i64[off + r * 2 + 1];
  #|              if (tid === 27) {
  #|                // Canonical HugeInt halves carry the real uuid u128; the
  #|                // stored representation flips the upper sign bit.
  #|                column[r] = api.DuckDBUUIDValue.fromStoredHugeInt(
  #|                  S128(U64(hi) ^ 0x8000000000000000n, lo),
  #|                );
  #|              } else if (tid === 32) {
  #|                column[r] = U128(hi, lo);
  #|              } else {
  #|                column[r] = S128(hi, lo);
  #|              }
  #|              break;
  #|            }
  #|            case 9: { // DECIMAL
  #|              const hi = pool_i64[off + r * 2];
  #|              const lo = pool_i64[off + r * 2 + 1];
  #|              column[r] = new api.DuckDBDecimalValue(
  #|                S128(hi, lo),
  #|                meta >> 16,
  #|                meta & 0xffff,
  #|              );
  #|              break;
  #|            }
  #|            case 10: { // INTERVAL triples
  #|              column[r] = new api.DuckDBIntervalValue(
  #|                Number(pool_i64[off + r * 3]),
  #|                Number(pool_i64[off + r * 3 + 1]),
  #|                pool_i64[off + r * 3 + 2],
  #|              );
  #|              break;
  #|            }
  #|            default:
  #|              column[r] = null;
  #|          }
  #|        }
  #|        columns.push(column);
  #|      }
  #|      const chunk = api.DuckDBDataChunk.create(types);
  #|      chunk.setColumns(columns);
  #|      appender.appender.appendDataChunk(chunk);
  #|      on_ok();
  #|    } catch (e) {
  #|      on_err(e.message || String(e));
  #|    }
  #|  }

///|
/// Pool a `VectorChunk` into the per-kind `FixedArray` pools consumed by
/// `js_appender_append_chunk_ffi`. Returns `(kinds, offsets, null_flags,
/// pool_i32, pool_i64, pool_f64, pool_str, pool_bytes)`; nested/`Any`
/// payloads never reach here — `append_chunk_specs` rejects them first.
fn js_pack_chunk(
  chunk : VectorChunk,
  offset : Int,
  rows : Int,
) -> (
  FixedArray[Int],
  FixedArray[Int],
  FixedArray[Int],
  FixedArray[Int],
  FixedArray[Int64],
  FixedArray[Double],
  FixedArray[String],
  FixedArray[Bytes],
) {
  let ncols = chunk.vectors.length()
  let kinds = FixedArray::make(ncols, 0)
  let offsets = FixedArray::make(ncols, 0)
  let null_flags = FixedArray::make(rows * ncols, 0)
  let pool_i32 : Array[Int] = []
  let pool_i64 : Array[Int64] = []
  let pool_f64 : Array[Double] = []
  let pool_str : Array[String] = []
  let pool_bytes : Array[Bytes] = []
  for col, vector in chunk.vectors {
    let kind = match vector.data {
      Bool(_) => 0
      Int32(_) => 1
      Int64(_) => 2
      UInt64(_) => 3
      Float(_) => 4
      Double(_) => 5
      Varchar(_) => 6
      Blob(_) => 7
      HugeInt(..) => 8
      Decimal(_) => 9
      Interval(_) => 10
      List(..) | Struct(..) | Map(..) | Any(_) => 11
    }
    let pool_offset = match kind {
      0 | 1 => pool_i32.length()
      2 | 3 | 8 | 9 | 10 => pool_i64.length()
      4 | 5 => pool_f64.length()
      6 => pool_str.length()
      7 => pool_bytes.length()
      _ => 0
    }
    kinds[col] = kind
    offsets[col] = pool_offset
    for row = 0; row < rows; row = row + 1 {
      let src = offset + row
      if !vector.validity[src] {
        null_flags[row * ncols + col] = 1
      }
      match vector.data {
        Bool(values) => pool_i32.push(if values[src] { 1 } else { 0 })
        Int32(values) => pool_i32.push(values[src])
        Int64(values) => pool_i64.push(values[src])
        UInt64(values) => pool_i64.push(values[src].reinterpret_as_int64())
        Float(values) => pool_f64.push(values[src].to_double())
        Double(values) => pool_f64.push(values[src])
        Varchar(values) => pool_str.push(values[src])
        Blob(values) => pool_bytes.push(values[src])
        HugeInt(lower~, upper~) => {
          pool_i64.push(upper[src])
          pool_i64.push(lower[src].reinterpret_as_int64())
        }
        Decimal(cells) => {
          pool_i64.push(cells[src].upper)
          pool_i64.push(cells[src].lower.reinterpret_as_int64())
        }
        Interval(cells) => {
          pool_i64.push(cells[src].months.to_int64())
          pool_i64.push(cells[src].days.to_int64())
          pool_i64.push(cells[src].micros)
        }
        List(..) | Struct(..) | Map(..) | Any(_) => ()
      }
    } nobreak {
      ()
    }
  }
  (
    kinds,
    offsets,
    null_flags,
    FixedArray::from_array(pool_i32),
    FixedArray::from_array(pool_i64),
    FixedArray::from_array(pool_f64),
    FixedArray::from_array(pool_str),
    FixedArray::from_array(pool_bytes),
  )
}

///|
/// Bulk-append a canonical `VectorChunk` in one shot on the Node backend:
/// column payloads are pooled and written into a `DuckDBDataChunk` passed to
/// `appendDataChunk` — no per-cell FFI calls. Unsupported column kinds and
/// invalid chunks fail with the same structured errors as the native
/// implementation; a closed appender reports `Closed(resource="appender")`.
pub fn Appender::append_chunk(
  self : Appender,
  chunk : VectorChunk,
) -> Result[Unit, DuckDBError] {
  match self.capabilities().require(BackendFeature::AppenderChunk) {
    Err(err) => return Err(err)
    Ok(_) => ()
  }
  let backend = self.backend()
  let (rows, type_ids, aux) = match append_chunk_specs(chunk, backend) {
    Ok(specs) => specs
    Err(err) => return Err(err)
  }
  // DuckDBDataChunk rejects more than `vector_size()` rows (normally 2048),
  // so larger inputs are appended as consecutive capacity-sized slices.
  let capacity = 2048
  for offset = 0; offset < rows; offset = offset + capacity {
    let slice_rows = if rows - offset < capacity {
      rows - offset
    } else {
      capacity
    }
    let (
      kinds,
      offsets,
      null_flags,
      pool_i32,
      pool_i64,
      pool_f64,
      pool_str,
      pool_bytes,
    ) = js_pack_chunk(chunk, offset, slice_rows)
    // The JS body invokes exactly one callback synchronously.
    let result_ref : Ref[Result[Unit, String]?] = { val: None, }
    js_appender_append_chunk_ffi(
      self,
      type_ids,
      aux,
      kinds,
      offsets,
      slice_rows,
      null_flags,
      pool_i32,
      pool_i64,
      pool_f64,
      pool_str,
      pool_bytes,
      fn() { result_ref.val = Some(Ok(())) },
      fn(e) { result_ref.val = Some(Err(e)) },
    )
    match result_ref.val {
      Some(Ok(_)) => ()
      Some(Err(e)) => return Err(append_error(e))
      None =>
        return Err(append_error("append_chunk: no result callback received"))
    }
  } nobreak {
    ()
  }
  Ok(())
}

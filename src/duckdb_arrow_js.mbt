///|
#external
pub type ArrowResult

// ============================================================================
// Arrow FFI Declarations (JavaScript Backend)
// ============================================================================

///|
extern "js" fn js_query_arrow(
  conn : Connection,
  sql : String,
  on_ok : (ArrowResult) -> Unit,
  on_err : (String) -> Unit,
) -> Unit =
  #|(conn, sql, on_ok, on_err) => {
  #|  const run = async () => {
  #|    if (conn && conn.kind === "wasm") {
  #|      try {
  #|        const arrowResult = await conn.conn.query(sql, { returnType: "arrow" });
  #|        on_ok({ kind: "arrow", result: arrowResult });
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    if (conn && conn.kind === "node") {
  #|      // Node API - Arrow support
  #|      try {
  #|        const result = await conn.connection.run(sql);
  #|        if (result && result.arrow) {
  #|          on_ok({ kind: "arrow", result: result.arrow() });
  #|          return;
  #|        }
  #|        // Fallback to regular result
  #|        on_ok({ kind: "arrow", result: null });
  #|        return;
  #|      } catch (e) {
  #|        on_err(e.message || String(e));
  #|        return;
  #|      }
  #|    }
  #|    on_err("unknown connection backend");
  #|  };
  #|  run().catch((err) => on_err(err.message || String(err)));
  #|}

///|
extern "js" fn js_arrow_column_count(result : ArrowResult) -> Int =
  #|(result) => {
  #|  if (result && result.kind === "arrow" && result.result) {
  #|    const arrow = result.result;
  #|    if (arrow.numCols) {
  #|      return arrow.numCols;
  #|    }
  #|    if (arrow.schema && arrow.schema.fields) {
  #|      return arrow.schema.fields.length;
  #|    }
  #|  }
  #|  return 0;
  #|}

///|
extern "js" fn js_arrow_row_count(result : ArrowResult) -> Int =
  #|(result) => {
  #|  if (result && result.kind === "arrow" && result.result) {
  #|    const arrow = result.result;
  #|    if (arrow.numRows !== undefined) {
  #|      return arrow.numRows;
  #|    }
  #|    if (arrow.length !== undefined) {
  #|      return arrow.length;
  #|    }
  #|  }
  #|  return 0;
  #|}

///|
extern "js" fn js_arrow_destroy(result : ArrowResult) -> Unit =
  #|(result) => {
  #|  // Arrow result will be garbage collected
  #|  if (result && result.result && result.result.close) {
  #|    result.result.close();
  #|  }
  #|}

///|
extern "js" fn js_arrow_get_schema(result : ArrowResult) -> String =
  #|(result) => {
  #|  if (!result?.result?.schema?.fields) return "[]";
  #|  const fields = result.result.schema.fields.map(field => {
  #|    let typeId = "string";
  #|    // Arrow type detection
  #|    if (field.type) {
  #|      const t = field.type;
  #|      // Check Arrow type by various properties
  #|      if (t.typeId === 1 || t.bitWidth === 1 || t.constructor?.name === "Bool") typeId = "bool";
  #|      else if (t.typeId === 2 || t.bitWidth === 32 || t.constructor?.name === "Int") typeId = "int32";
  #|      else if (t.typeId === 3 || t.bitWidth === 64 || t.constructor?.name === "Int64" || t.constructor?.name === "BigInt") typeId = "int64";
  #|      else if (t.typeId === 7 || t.typeId === 8 || t.typeId === 9 || t.constructor?.name === "Float" || t.constructor?.name === "Float64") typeId = "double";
  #|      else if (t.typeId === 5 || t.typeId === 6 || t.constructor?.name === "Utf8") typeId = "string";
  #|    }
  #|    return {
  #|      name: field.name || "",
  #|      nullable: field.nullable ?? true,
  #|      type_id: typeId
  #|    };
  #|  });
  #|  return JSON.stringify(fields);
  #|}

///|
extern "js" fn js_arrow_get_column_int32(
  result : ArrowResult,
  col : Int,
) -> String =
  #|(result, col) => {
  #|  if (!result?.result) return "[]";
  #|  const vector = result.result.getChild(col);
  #|  if (!vector) return "[]";
  #|  const arr = vector.toArray();
  #|  return JSON.stringify(arr || []);
  #|}

///|
extern "js" fn js_arrow_get_column_int64(
  result : ArrowResult,
  col : Int,
) -> String =
  #|(result, col) => {
  #|  if (!result?.result) return "[]";
  #|  const vector = result.result.getChild(col);
  #|  if (!vector) return "[]";
  #|  const arr = vector.toArray();
  #|  // Convert BigInt values to strings for JSON serialization
  #|  const converted = arr.map(v => typeof v === "bigint" ? v.toString() : v);
  #|  return JSON.stringify(converted || []);
  #|}

///|
extern "js" fn js_arrow_get_column_double(
  result : ArrowResult,
  col : Int,
) -> String =
  #|(result, col) => {
  #|  if (!result?.result) return "[]";
  #|  const vector = result.result.getChild(col);
  #|  if (!vector) return "[]";
  #|  const arr = vector.toArray();
  #|  return JSON.stringify(arr || []);
  #|}

///|
extern "js" fn js_arrow_get_column_string(
  result : ArrowResult,
  col : Int,
) -> String =
  #|(result, col) => {
  #|  if (!result?.result) return "[]";
  #|  const vector = result.result.getChild(col);
  #|  if (!vector) return "[]";
  #|  const arr = vector.toArray();
  #|  return JSON.stringify(arr || []);
  #|}

///|
extern "js" fn js_arrow_get_column_bool(
  result : ArrowResult,
  col : Int,
) -> String =
  #|(result, col) => {
  #|  if (!result?.result) return "[]";
  #|  const vector = result.result.getChild(col);
  #|  if (!vector) return "[]";
  #|  const arr = vector.toArray();
  #|  return JSON.stringify(arr || []);
  #|}

// ============================================================================
// Public API (JavaScript Backend)
// ============================================================================

///|
pub struct ArrowField {
  name : String
  nullable : Bool
  type_id : String
}

///|
pub struct ArrowSchemaInfo {
  fields : Array[ArrowField]
}

///|
pub fn Connection::query_arrow(
  self : Connection,
  sql : String,
  on_done~ : (Result[ArrowResult, DuckDBError]) -> Unit,
) -> Unit {
  js_query_arrow(self, sql, fn(result) { on_done(Ok(result)) }, fn(err) {
    on_done(Err(DuckDBError::Message(err)))
  })
}

///|
pub fn ArrowResult::column_count(self : ArrowResult) -> Int {
  js_arrow_column_count(self)
}

///|
pub fn ArrowResult::row_count(self : ArrowResult) -> Int {
  js_arrow_row_count(self)
}

///|
pub fn ArrowResult::get_schema(
  self : ArrowResult,
) -> Result[ArrowSchemaInfo, DuckDBError] {
  let json_str = js_arrow_get_schema(self)
  parse_arrow_schema_json(json_str)
}

// Parse Arrow schema JSON into ArrowSchemaInfo

///|
fn parse_arrow_schema_json(
  json : String,
) -> Result[ArrowSchemaInfo, DuckDBError] {
  let parsed = @json.parse(json) catch {
    err =>
      return Err(
        DuckDBError::Message("schema parse failed: " + err.to_string()),
      )
  }
  match parsed {
    Json::Array(items) => {
      let fields : Array[ArrowField] = []
      for item in items {
        match item {
          Json::Object(obj) => {
            let name = match obj["name"] {
              Json::String(n) => n
              _ => ""
            }
            let nullable = match obj["nullable"] {
              Json::True => true
              Json::False => false
              _ => true
            }
            let type_id = match obj["type_id"] {
              Json::String(t) => t
              _ => "string"
            }
            fields.push(ArrowField::{ name, nullable, type_id })
          }
          _ => return Err(DuckDBError::Message("invalid field format"))
        }
      } else {
        ()
      }
      Ok(ArrowSchemaInfo::{ fields, })
    }
    _ => Ok(ArrowSchemaInfo::{ fields: [] })
  }
}

///|
pub fn ArrowResult::get_column_int32(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let json_str = js_arrow_get_column_int32(self, col)
  parse_json_int_array(json_str)
}

///|
pub fn ArrowResult::get_column_int64(
  self : ArrowResult,
  col : Int,
) -> Array[Int] {
  let json_str = js_arrow_get_column_int64(self, col)
  parse_json_int_array(json_str)
}

///|
pub fn ArrowResult::get_column_double(
  self : ArrowResult,
  col : Int,
) -> Array[Double] {
  let json_str = js_arrow_get_column_double(self, col)
  parse_json_double_array(json_str)
}

///|
pub fn ArrowResult::get_column_string(
  self : ArrowResult,
  col : Int,
) -> Array[String] {
  let json_str = js_arrow_get_column_string(self, col)
  parse_json_string_array(json_str)
}

///|
pub fn ArrowResult::get_column_bool(
  self : ArrowResult,
  col : Int,
) -> Array[Bool] {
  let json_str = js_arrow_get_column_bool(self, col)
  parse_json_bool_array(json_str)
}

// ============================================================================
// JSON Parsing Helpers
// ============================================================================

// Simple string to Int parser

///|
fn parse_str_to_int(s : String) -> Int {
  let mut result = 0
  let mut i = 0
  let negative = if s.length() > 0 && s[0] == '-' {
    i = 1
    true
  } else {
    false
  }
  while i < s.length() {
    let c = s[i]
    if c >= '0' && c <= '9' {
      result = result * 10 + (c.to_int() - '0'.to_int())
    }
    i = i + 1
  }
  if negative {
    -result
  } else {
    result
  }
}

///|
fn parse_json_int_array(json : String) -> Array[Int] {
  let parsed = @json.parse(json) catch { _ => return [] }
  match parsed {
    Json::Array(items) => {
      let result : Array[Int] = []
      for item in items {
        match item {
          Json::Number(n, ..) => result.push(parse_str_to_int(n.to_string()))
          Json::String(s) =>
            // Handle BigInt strings
            result.push(parse_str_to_int(s))
          Json::Null => result.push(0)
          _ => result.push(0)
        }
      } else {
        ()
      }
      result
    }
    _ => []
  }
}

///|
fn parse_json_double_array(json : String) -> Array[Double] {
  let parsed = @json.parse(json) catch { _ => return [] }
  match parsed {
    Json::Array(items) => {
      let result : Array[Double] = []
      for item in items {
        match item {
          Json::Number(n, ..) => result.push(n)
          Json::Null => result.push(0.0)
          _ => result.push(0.0)
        }
      } else {
        ()
      }
      result
    }
    _ => []
  }
}

///|
fn parse_json_string_array(json : String) -> Array[String] {
  let parsed = @json.parse(json) catch { _ => return [] }
  match parsed {
    Json::Array(items) => {
      let result : Array[String] = []
      for item in items {
        match item {
          Json::String(s) => result.push(s)
          Json::Null => result.push("")
          _ => result.push("")
        }
      } else {
        ()
      }
      result
    }
    _ => []
  }
}

///|
fn parse_json_bool_array(json : String) -> Array[Bool] {
  let parsed = @json.parse(json) catch { _ => return [] }
  match parsed {
    Json::Array(items) => {
      let result : Array[Bool] = []
      for item in items {
        match item {
          Json::True => result.push(true)
          Json::False => result.push(false)
          Json::Null => result.push(false)
          _ => result.push(false)
        }
      } else {
        ()
      }
      result
    }
    _ => []
  }
}

///|
pub fn ArrowResult::close(
  self : ArrowResult,
  on_done~ : (Result[Unit, DuckDBError]) -> Unit,
) -> Unit {
  js_arrow_destroy(self)
  on_done(Ok(()))
}

#include "duckdb.h"
#include "moonbit.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Handle wrappers are stored in MoonBit external objects (allocated via
// moonbit_make_external_object), so a handle stays a valid, GC-managed
// pointer even after close. Closing NULLs the inner DuckDB handle; the
// finalizer releases whatever is left when the object is collected.
typedef struct {
  duckdb_database db;
  duckdb_connection conn;
} duckdb_mb_connection;

typedef struct {
  duckdb_prepared_statement stmt;
} duckdb_mb_statement;

static char *duckdb_mb_last_error_message = NULL;

// DuckDB engine error classification of the last error when the failing C API
// reported one via duckdb_result_error_type or duckdb_error_data
// (-1 = not available).
static int duckdb_mb_last_error_code = -1;

static void duckdb_mb_set_error_typed(const char *message, int code) {
  duckdb_mb_last_error_code = code;
  if (duckdb_mb_last_error_message) {
    free(duckdb_mb_last_error_message);
    duckdb_mb_last_error_message = NULL;
  }
  if (!message) {
    return;
  }
  size_t len = strlen(message);
  char *buf = (char *)malloc(len + 1);
  if (!buf) {
    return;
  }
  memcpy(buf, message, len);
  buf[len] = '\0';
  duckdb_mb_last_error_message = buf;
}

static void duckdb_mb_set_error(const char *message) {
  duckdb_mb_set_error_typed(message, -1);
}

static moonbit_bytes_t duckdb_mb_make_bytes(const char *data, size_t len) {
  moonbit_bytes_t bytes = moonbit_make_bytes_raw((int32_t)len);
  if (len == 0 || !data) {
    return bytes;
  }
  memcpy(bytes, data, len);
  return bytes;
}

static char *duckdb_mb_bytes_to_cstr(moonbit_bytes_t bytes) {
  if (!bytes) {
    return NULL;
  }
  int32_t len = Moonbit_array_length(bytes);
  char *buf = (char *)malloc((size_t)len + 1);
  if (!buf) {
    return NULL;
  }
  if (len > 0) {
    memcpy(buf, bytes, (size_t)len);
  }
  buf[len] = '\0';
  return buf;
}

// Normalize a duckdb_error_type code to the same snake_case names
// "<Type> Error:" message prefixes produce, "unknown" when out of range.
moonbit_bytes_t duckdb_mb_error_type_name(int32_t code) {
  const char *name;
  switch ((duckdb_error_type)code) {
  case DUCKDB_ERROR_INVALID: name = "invalid"; break;
  case DUCKDB_ERROR_OUT_OF_RANGE: name = "out_of_range"; break;
  case DUCKDB_ERROR_CONVERSION: name = "conversion"; break;
  case DUCKDB_ERROR_UNKNOWN_TYPE: name = "unknown_type"; break;
  case DUCKDB_ERROR_DECIMAL: name = "decimal"; break;
  case DUCKDB_ERROR_MISMATCH_TYPE: name = "mismatch_type"; break;
  case DUCKDB_ERROR_DIVIDE_BY_ZERO: name = "divide_by_zero"; break;
  case DUCKDB_ERROR_OBJECT_SIZE: name = "object_size"; break;
  case DUCKDB_ERROR_INVALID_TYPE: name = "invalid_type"; break;
  case DUCKDB_ERROR_SERIALIZATION: name = "serialization"; break;
  case DUCKDB_ERROR_TRANSACTION: name = "transaction"; break;
  case DUCKDB_ERROR_NOT_IMPLEMENTED: name = "not_implemented"; break;
  case DUCKDB_ERROR_EXPRESSION: name = "expression"; break;
  case DUCKDB_ERROR_CATALOG: name = "catalog"; break;
  case DUCKDB_ERROR_PARSER: name = "parser"; break;
  case DUCKDB_ERROR_PLANNER: name = "planner"; break;
  case DUCKDB_ERROR_SCHEDULER: name = "scheduler"; break;
  case DUCKDB_ERROR_EXECUTOR: name = "executor"; break;
  case DUCKDB_ERROR_CONSTRAINT: name = "constraint"; break;
  case DUCKDB_ERROR_INDEX: name = "index"; break;
  case DUCKDB_ERROR_STAT: name = "stat"; break;
  case DUCKDB_ERROR_CONNECTION: name = "connection"; break;
  case DUCKDB_ERROR_SYNTAX: name = "syntax"; break;
  case DUCKDB_ERROR_SETTINGS: name = "settings"; break;
  case DUCKDB_ERROR_BINDER: name = "binder"; break;
  case DUCKDB_ERROR_NETWORK: name = "network"; break;
  case DUCKDB_ERROR_OPTIMIZER: name = "optimizer"; break;
  case DUCKDB_ERROR_NULL_POINTER: name = "null_pointer"; break;
  case DUCKDB_ERROR_IO: name = "io"; break;
  case DUCKDB_ERROR_INTERRUPT: name = "interrupt"; break;
  case DUCKDB_ERROR_FATAL: name = "fatal"; break;
  case DUCKDB_ERROR_INTERNAL: name = "internal"; break;
  case DUCKDB_ERROR_INVALID_INPUT: name = "invalid_input"; break;
  case DUCKDB_ERROR_OUT_OF_MEMORY: name = "out_of_memory"; break;
  case DUCKDB_ERROR_PERMISSION: name = "permission"; break;
  case DUCKDB_ERROR_PARAMETER_NOT_RESOLVED: name = "parameter_not_resolved"; break;
  case DUCKDB_ERROR_PARAMETER_NOT_ALLOWED: name = "parameter_not_allowed"; break;
  case DUCKDB_ERROR_DEPENDENCY: name = "dependency"; break;
  case DUCKDB_ERROR_HTTP: name = "http"; break;
  case DUCKDB_ERROR_MISSING_EXTENSION: name = "missing_extension"; break;
  case DUCKDB_ERROR_AUTOLOAD: name = "autoload"; break;
  case DUCKDB_ERROR_SEQUENCE: name = "sequence"; break;
  case DUCKDB_INVALID_CONFIGURATION: name = "invalid_configuration"; break;
  default: name = "unknown"; break;
  }
  return duckdb_mb_make_bytes(name, strlen(name));
}

static void duckdb_mb_connection_finalize(void *self) {
  duckdb_mb_connection *handle = (duckdb_mb_connection *)self;
  if (!handle) {
    return;
  }
  if (handle->conn) {
    duckdb_disconnect(&handle->conn);
  }
  if (handle->db) {
    duckdb_close(&handle->db);
  }
}

static duckdb_mb_connection *duckdb_mb_connection_new(void) {
  duckdb_mb_connection *handle = (duckdb_mb_connection *)
      moonbit_make_external_object(duckdb_mb_connection_finalize,
                                   sizeof(duckdb_mb_connection));
  if (!handle) {
    return NULL;
  }
  handle->db = NULL;
  handle->conn = NULL;
  return handle;
}

duckdb_mb_connection *duckdb_mb_connect(moonbit_bytes_t path) {
  duckdb_mb_connection *handle = duckdb_mb_connection_new();
  if (!handle) {
    duckdb_mb_set_error("failed to allocate connection handle");
    return NULL;
  }
  int32_t path_len = path ? Moonbit_array_length(path) : 0;
  char *path_c = NULL;
  const char *path_value = ":memory:";
  if (path_len > 0) {
    path_c = duckdb_mb_bytes_to_cstr(path);
    if (!path_c) {
      duckdb_mb_set_error("failed to allocate path buffer");
      return handle;
    }
    path_value = path_c;
  }
  char *open_error = NULL;
  duckdb_state state =
      duckdb_open_ext(path_value, &handle->db, NULL, &open_error);
  if (state != DuckDBSuccess) {
    if (open_error && open_error[0] != '\0') {
      duckdb_mb_set_error(open_error);
    } else if (path_c) {
      char message[256];
      snprintf(
        message,
        sizeof(message),
        "duckdb_open failed (path_len=%d path=%s)",
        path_len,
        path_c
      );
      duckdb_mb_set_error(message);
    } else {
      char message[128];
      snprintf(
        message,
        sizeof(message),
        "duckdb_open failed (path_len=%d path=:memory:)",
        path_len
      );
      duckdb_mb_set_error(message);
    }
    if (open_error) {
      duckdb_free(open_error);
    }
    handle->db = NULL;
    free(path_c);
    return handle;
  }
  if (open_error) {
    duckdb_free(open_error);
  }
  free(path_c);
  state = duckdb_connect(handle->db, &handle->conn);
  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_connect failed");
    duckdb_close(&handle->db);
    return handle;
  }
  return handle;
}

// Returns 1 when this call closed the connection, 0 when the handle was
// already closed.
int32_t duckdb_mb_disconnect(duckdb_mb_connection *handle) {
  if (!handle || !handle->conn) {
    return 0;
  }
  duckdb_disconnect(&handle->conn);
  duckdb_close(&handle->db);
  handle->conn = NULL;
  handle->db = NULL;
  return 1;
}

duckdb_result *duckdb_mb_query(duckdb_mb_connection *handle,
                               moonbit_bytes_t sql) {
  if (!handle || !handle->conn) {
    duckdb_mb_set_error("connection is null");
    return NULL;
  }
  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    duckdb_mb_set_error("failed to allocate sql buffer");
    return NULL;
  }
  duckdb_result *result = (duckdb_result *)malloc(sizeof(duckdb_result));
  if (!result) {
    free(sql_c);
    duckdb_mb_set_error("failed to allocate result");
    return NULL;
  }
  duckdb_state state = duckdb_query(handle->conn, sql_c, result);
  free(sql_c);
  if (state != DuckDBSuccess) {
    duckdb_error_type error_type = duckdb_result_error_type(result);
    const char *error = duckdb_result_error(result);
    if (!error) {
      error = "duckdb_query failed";
    }
    duckdb_mb_set_error_typed(error, (int)error_type);
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }
  return result;
}

void duckdb_mb_result_destroy(duckdb_result *result) {
  if (!result) {
    return;
  }
  duckdb_destroy_result(result);
  free(result);
}

int32_t duckdb_mb_result_column_count(duckdb_result *result) {
  if (!result) {
    return 0;
  }
  return (int32_t)duckdb_column_count(result);
}

int32_t duckdb_mb_result_row_count(duckdb_result *result) {
  if (!result) {
    return 0;
  }
  return (int32_t)duckdb_row_count(result);
}

moonbit_bytes_t duckdb_mb_result_column_name(duckdb_result *result,
                                             int32_t col) {
  if (!result) {
    return moonbit_make_bytes_raw(0);
  }
  const char *name = duckdb_column_name(result, (idx_t)col);
  if (!name) {
    return moonbit_make_bytes_raw(0);
  }
  return duckdb_mb_make_bytes(name, strlen(name));
}

int32_t duckdb_mb_result_column_type(duckdb_result *result, int32_t col) {
  if (!result) {
    return (int32_t)DUCKDB_TYPE_INVALID;
  }
  return (int32_t)duckdb_column_type(result, (idx_t)col);
}

int32_t duckdb_mb_result_is_null(duckdb_result *result,
                                 int32_t col,
                                 int32_t row) {
  if (!result) {
    return 1;
  }
  return duckdb_value_is_null(result, (idx_t)col, (idx_t)row) ? 1 : 0;
}

moonbit_bytes_t duckdb_mb_result_value(duckdb_result *result,
                                       int32_t col,
                                       int32_t row) {
  if (!result) {
    return moonbit_make_bytes_raw(0);
  }
  char *value = duckdb_value_varchar(result, (idx_t)col, (idx_t)row);
  if (!value) {
    return moonbit_make_bytes_raw(0);
  }
  size_t len = strlen(value);
  moonbit_bytes_t bytes = duckdb_mb_make_bytes(value, len);
  duckdb_free(value);
  return bytes;
}

moonbit_bytes_t duckdb_mb_last_error(void) {
  if (!duckdb_mb_last_error_message) {
    return moonbit_make_bytes_raw(0);
  }
  return duckdb_mb_make_bytes(duckdb_mb_last_error_message,
                              strlen(duckdb_mb_last_error_message));
}

int32_t duckdb_mb_last_error_type(void) {
  return duckdb_mb_last_error_code;
}

int32_t duckdb_mb_is_null_conn(duckdb_mb_connection *handle) {
  return handle == NULL || handle->conn == NULL ? 1 : 0;
}

int32_t duckdb_mb_is_null_result(duckdb_result *result) {
  return result == NULL ? 1 : 0;
}

// ============================================================================
// Streaming Result Functions
// ============================================================================

typedef struct {
  duckdb_result *result;
  duckdb_type *column_types;
  int32_t column_count;
} duckdb_mb_stream;

// A fetched data chunk plus the column type ids of the result it came from.
// `column_types` is owned by the chunk (a copy made at fetch time), so the
// chunk stays usable standalone regardless of where it was fetched from.
typedef struct {
  duckdb_data_chunk chunk;
  duckdb_type *column_types;
  int32_t column_count;
} duckdb_mb_chunk;

static duckdb_mb_chunk *duckdb_mb_chunk_wrap(duckdb_data_chunk chunk,
                                             const duckdb_type *column_types,
                                             int32_t column_count) {
  duckdb_mb_chunk *mb_chunk = (duckdb_mb_chunk *)malloc(sizeof(duckdb_mb_chunk));
  if (!mb_chunk) {
    duckdb_mb_set_error("failed to allocate chunk handle");
    duckdb_destroy_data_chunk(&chunk);
    return NULL;
  }
  mb_chunk->chunk = chunk;
  mb_chunk->column_types = NULL;
  mb_chunk->column_count = column_count;
  if (column_count > 0) {
    mb_chunk->column_types =
        (duckdb_type *)malloc(sizeof(duckdb_type) * (size_t)column_count);
    if (!mb_chunk->column_types) {
      duckdb_mb_set_error("failed to allocate chunk column types");
      duckdb_destroy_data_chunk(&chunk);
      free(mb_chunk);
      return NULL;
    }
    memcpy(mb_chunk->column_types, column_types,
           sizeof(duckdb_type) * (size_t)column_count);
  }
  return mb_chunk;
}

static bool duckdb_mb_is_stream_supported_type(duckdb_type type) {
  switch (type) {
  case DUCKDB_TYPE_BOOLEAN:
  case DUCKDB_TYPE_TINYINT:
  case DUCKDB_TYPE_SMALLINT:
  case DUCKDB_TYPE_INTEGER:
  case DUCKDB_TYPE_BIGINT:
  case DUCKDB_TYPE_UTINYINT:
  case DUCKDB_TYPE_USMALLINT:
  case DUCKDB_TYPE_UINTEGER:
  case DUCKDB_TYPE_UBIGINT:
  case DUCKDB_TYPE_FLOAT:
  case DUCKDB_TYPE_DOUBLE:
  case DUCKDB_TYPE_VARCHAR:
  case DUCKDB_TYPE_BLOB:
  case DUCKDB_TYPE_DATE:
  case DUCKDB_TYPE_TIME:
  case DUCKDB_TYPE_TIME_NS:
  case DUCKDB_TYPE_TIME_TZ:
  case DUCKDB_TYPE_TIMESTAMP:
  case DUCKDB_TYPE_TIMESTAMP_TZ:
  case DUCKDB_TYPE_TIMESTAMP_S:
  case DUCKDB_TYPE_TIMESTAMP_MS:
  case DUCKDB_TYPE_TIMESTAMP_NS:
  case DUCKDB_TYPE_INTERVAL:
  case DUCKDB_TYPE_HUGEINT:
  case DUCKDB_TYPE_UHUGEINT:
  case DUCKDB_TYPE_UUID:
    return true;
  default:
    return false;
  }
}

static moonbit_bytes_t duckdb_mb_value_to_bytes(duckdb_value value) {
  if (!value) {
    return moonbit_make_bytes_raw(0);
  }
  // duckdb_get_varchar renders the unquoted VARCHAR cast (like
  // duckdb_value_varchar); duckdb_value_to_string wraps strings, decimals
  // and dates in single quotes, which would change compat output.
  char *str = duckdb_get_varchar(value);
  duckdb_destroy_value(&value);
  if (!str) {
    return moonbit_make_bytes_raw(0);
  }
  size_t len = strlen(str);
  moonbit_bytes_t bytes = duckdb_mb_make_bytes(str, len);
  duckdb_free(str);
  return bytes;
}

static void duckdb_mb_stream_finalize(void *self) {
  duckdb_mb_stream *stream = (duckdb_mb_stream *)self;
  if (!stream) {
    return;
  }
  if (stream->result) {
    duckdb_destroy_result(stream->result);
    free(stream->result);
    stream->result = NULL;
  }
  if (stream->column_types) {
    free(stream->column_types);
    stream->column_types = NULL;
  }
}

static duckdb_mb_stream *duckdb_mb_stream_from_result(duckdb_result *result) {
  if (!result) {
    duckdb_mb_set_error("result is null");
    return NULL;
  }
  int32_t column_count = (int32_t)duckdb_column_count(result);
  duckdb_type *column_types = NULL;
  if (column_count > 0) {
    column_types = (duckdb_type *)malloc(sizeof(duckdb_type) * (size_t)column_count);
    if (!column_types) {
      duckdb_mb_set_error("failed to allocate column types");
      return NULL;
    }
    for (int32_t col = 0; col < column_count; col++) {
      duckdb_type type = duckdb_column_type(result, (idx_t)col);
      if (!duckdb_mb_is_stream_supported_type(type)) {
        duckdb_mb_set_error("streaming query has unsupported column type");
        free(column_types);
        return NULL;
      }
      column_types[col] = type;
    }
  }
  duckdb_mb_stream *stream = (duckdb_mb_stream *)
      moonbit_make_external_object(duckdb_mb_stream_finalize,
                                   sizeof(duckdb_mb_stream));
  if (!stream) {
    free(column_types);
    duckdb_mb_set_error("failed to allocate stream handle");
    return NULL;
  }
  stream->result = result;
  stream->column_types = column_types;
  stream->column_count = column_count;
  return stream;
}

duckdb_mb_stream *duckdb_mb_query_stream(duckdb_mb_connection *handle,
                                         moonbit_bytes_t sql) {
  if (!handle || !handle->conn) {
    duckdb_mb_set_error("connection is null");
    return NULL;
  }
  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    duckdb_mb_set_error("failed to allocate sql buffer");
    return NULL;
  }
  duckdb_prepared_statement stmt;
  duckdb_state state = duckdb_prepare(handle->conn, sql_c, &stmt);
  free(sql_c);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(stmt);
    duckdb_mb_set_error(error && error[0] ? error : "duckdb_prepare failed");
    duckdb_destroy_prepare(&stmt);
    return NULL;
  }
  duckdb_result *result = (duckdb_result *)malloc(sizeof(duckdb_result));
  if (!result) {
    duckdb_mb_set_error("failed to allocate result");
    duckdb_destroy_prepare(&stmt);
    return NULL;
  }
  state = duckdb_execute_prepared_streaming(stmt, result);
  duckdb_destroy_prepare(&stmt);
  if (state != DuckDBSuccess) {
    duckdb_error_type error_type = duckdb_result_error_type(result);
    const char *error = duckdb_result_error(result);
    duckdb_mb_set_error_typed(
        error && error[0] ? error : "execute_prepared_streaming failed",
        (int)error_type);
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }
  duckdb_mb_stream *stream = duckdb_mb_stream_from_result(result);
  if (!stream) {
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }
  return stream;
}

duckdb_mb_stream *duckdb_mb_execute_prepared_stream(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return NULL;
  }
  duckdb_result *result = (duckdb_result *)malloc(sizeof(duckdb_result));
  if (!result) {
    duckdb_mb_set_error("failed to allocate result");
    return NULL;
  }
  duckdb_state state = duckdb_execute_prepared_streaming(mb_stmt->stmt, result);
  if (state != DuckDBSuccess) {
    duckdb_error_type error_type = duckdb_result_error_type(result);
    const char *error = duckdb_result_error(result);
    duckdb_mb_set_error_typed(
        error && error[0] ? error : "execute_prepared_streaming failed",
        (int)error_type);
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }
  duckdb_mb_stream *stream = duckdb_mb_stream_from_result(result);
  if (!stream) {
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }
  return stream;
}

// Returns 1 when this call destroyed the stream, 0 when it was already closed.
int32_t duckdb_mb_stream_destroy(duckdb_mb_stream *stream) {
  if (!stream || !stream->result) {
    return 0;
  }
  duckdb_destroy_result(stream->result);
  free(stream->result);
  stream->result = NULL;
  if (stream->column_types) {
    free(stream->column_types);
    stream->column_types = NULL;
  }
  return 1;
}

int32_t duckdb_mb_is_null_stream(duckdb_mb_stream *stream) {
  return stream == NULL || stream->result == NULL ? 1 : 0;
}

int32_t duckdb_mb_stream_column_count(duckdb_mb_stream *stream) {
  if (!stream || !stream->result) {
    return 0;
  }
  return stream->column_count;
}

moonbit_bytes_t duckdb_mb_stream_column_name(duckdb_mb_stream *stream,
                                             int32_t col) {
  if (!stream || !stream->result) {
    return moonbit_make_bytes_raw(0);
  }
  if (col < 0 || col >= stream->column_count) {
    return moonbit_make_bytes_raw(0);
  }
  const char *name = duckdb_column_name(stream->result, (idx_t)col);
  if (!name) {
    return moonbit_make_bytes_raw(0);
  }
  return duckdb_mb_make_bytes(name, strlen(name));
}

duckdb_mb_chunk *duckdb_mb_stream_fetch_chunk(duckdb_mb_stream *stream) {
  if (!stream || !stream->result) {
    duckdb_mb_set_error("stream is null");
    return NULL;
  }
  duckdb_data_chunk chunk = duckdb_stream_fetch_chunk(*stream->result);
  if (!chunk) {
    const char *error = duckdb_result_error(stream->result);
    if (error && error[0]) {
      duckdb_error_type error_type = duckdb_result_error_type(stream->result);
      duckdb_mb_set_error(error);
      duckdb_mb_last_error_code = (int)error_type;
    } else {
      duckdb_mb_set_error(NULL);
    }
    return NULL;
  }
  return duckdb_mb_chunk_wrap(chunk, stream->column_types, stream->column_count);
}

// Fetch the next chunk of a materialized result (duckdb_fetch_chunk). Unlike
// the streaming path there is no supported-type gate: vector readers decide
// per column type and exotic types degrade to per-cell strings.
duckdb_mb_chunk *duckdb_mb_result_fetch_chunk(duckdb_result *result) {
  if (!result) {
    duckdb_mb_set_error("result is null");
    return NULL;
  }
  duckdb_data_chunk chunk = duckdb_fetch_chunk(*result);
  if (!chunk) {
    duckdb_mb_set_error(NULL);
    return NULL;
  }
  int32_t column_count = (int32_t)duckdb_column_count(result);
  duckdb_type *column_types = NULL;
  if (column_count > 0) {
    column_types =
        (duckdb_type *)malloc(sizeof(duckdb_type) * (size_t)column_count);
    if (!column_types) {
      duckdb_mb_set_error("failed to allocate column types");
      duckdb_destroy_data_chunk(&chunk);
      return NULL;
    }
    for (int32_t col = 0; col < column_count; col++) {
      column_types[col] = duckdb_column_type(result, (idx_t)col);
    }
  }
  duckdb_mb_chunk *mb_chunk =
      duckdb_mb_chunk_wrap(chunk, column_types, column_count);
  free(column_types);
  return mb_chunk;
}

void duckdb_mb_chunk_destroy(duckdb_mb_chunk *chunk) {
  if (!chunk) {
    return;
  }
  if (chunk->chunk) {
    duckdb_destroy_data_chunk(&chunk->chunk);
  }
  free(chunk->column_types);
  free(chunk);
}

int32_t duckdb_mb_is_null_chunk(duckdb_mb_chunk *chunk) {
  return chunk == NULL ? 1 : 0;
}

int32_t duckdb_mb_chunk_row_count(duckdb_mb_chunk *chunk) {
  if (!chunk || !chunk->chunk) {
    return 0;
  }
  return (int32_t)duckdb_data_chunk_get_size(chunk->chunk);
}

int32_t duckdb_mb_chunk_column_count(duckdb_mb_chunk *chunk) {
  if (!chunk || !chunk->chunk) {
    return 0;
  }
  return (int32_t)duckdb_data_chunk_get_column_count(chunk->chunk);
}

int32_t duckdb_mb_chunk_is_null(duckdb_mb_chunk *chunk,
                                int32_t col,
                                int32_t row) {
  if (!chunk || !chunk->chunk || !chunk->column_types) {
    return 1;
  }
  if (col < 0 || col >= chunk->column_count || row < 0) {
    return 1;
  }
  duckdb_vector vector = duckdb_data_chunk_get_vector(chunk->chunk, (idx_t)col);
  uint64_t *validity = duckdb_vector_get_validity(vector);
  if (!validity) {
    return 0;
  }
  return duckdb_validity_row_is_valid(validity, (idx_t)row) ? 0 : 1;
}

static moonbit_bytes_t duckdb_mb_cell_to_bytes(duckdb_vector vector,
                                               duckdb_type type,
                                               idx_t row) {
  void *data = duckdb_vector_get_data(vector);
  if (!data) {
    return moonbit_make_bytes_raw(0);
  }
  switch (type) {
  case DUCKDB_TYPE_BOOLEAN: {
    bool val = ((bool *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_bool(val));
  }
  case DUCKDB_TYPE_TINYINT: {
    int8_t val = ((int8_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_int8(val));
  }
  case DUCKDB_TYPE_SMALLINT: {
    int16_t val = ((int16_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_int16(val));
  }
  case DUCKDB_TYPE_INTEGER: {
    int32_t val = ((int32_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_int32(val));
  }
  case DUCKDB_TYPE_BIGINT: {
    int64_t val = ((int64_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_int64(val));
  }
  case DUCKDB_TYPE_UTINYINT: {
    uint8_t val = ((uint8_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uint8(val));
  }
  case DUCKDB_TYPE_USMALLINT: {
    uint16_t val = ((uint16_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uint16(val));
  }
  case DUCKDB_TYPE_UINTEGER: {
    uint32_t val = ((uint32_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uint32(val));
  }
  case DUCKDB_TYPE_UBIGINT: {
    uint64_t val = ((uint64_t *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uint64(val));
  }
  case DUCKDB_TYPE_FLOAT: {
    float val = ((float *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_float(val));
  }
  case DUCKDB_TYPE_DOUBLE: {
    double val = ((double *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_double(val));
  }
  case DUCKDB_TYPE_VARCHAR: {
    duckdb_string_t *strings = (duckdb_string_t *)data;
    duckdb_string_t str = strings[row];
    const char *ptr = duckdb_string_t_data(&str);
    uint32_t len = duckdb_string_t_length(str);
    return duckdb_mb_value_to_bytes(duckdb_create_varchar_length(ptr, (idx_t)len));
  }
  case DUCKDB_TYPE_BLOB: {
    duckdb_string_t *strings = (duckdb_string_t *)data;
    duckdb_string_t str = strings[row];
    const char *ptr = duckdb_string_t_data(&str);
    uint32_t len = duckdb_string_t_length(str);
    return duckdb_mb_value_to_bytes(duckdb_create_blob((const uint8_t *)ptr, (idx_t)len));
  }
  case DUCKDB_TYPE_DATE: {
    duckdb_date val = ((duckdb_date *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_date(val));
  }
  case DUCKDB_TYPE_TIME: {
    duckdb_time val = ((duckdb_time *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_time(val));
  }
  case DUCKDB_TYPE_TIME_NS: {
    duckdb_time_ns val = ((duckdb_time_ns *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_time_ns(val));
  }
  case DUCKDB_TYPE_TIME_TZ: {
    duckdb_time_tz val = ((duckdb_time_tz *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_time_tz_value(val));
  }
  case DUCKDB_TYPE_TIMESTAMP: {
    duckdb_timestamp val = ((duckdb_timestamp *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_timestamp(val));
  }
  case DUCKDB_TYPE_TIMESTAMP_TZ: {
    duckdb_timestamp val = ((duckdb_timestamp *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_timestamp_tz(val));
  }
  case DUCKDB_TYPE_TIMESTAMP_S: {
    duckdb_timestamp_s val = ((duckdb_timestamp_s *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_timestamp_s(val));
  }
  case DUCKDB_TYPE_TIMESTAMP_MS: {
    duckdb_timestamp_ms val = ((duckdb_timestamp_ms *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_timestamp_ms(val));
  }
  case DUCKDB_TYPE_TIMESTAMP_NS: {
    duckdb_timestamp_ns val = ((duckdb_timestamp_ns *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_timestamp_ns(val));
  }
  case DUCKDB_TYPE_INTERVAL: {
    duckdb_interval val = ((duckdb_interval *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_interval(val));
  }
  case DUCKDB_TYPE_HUGEINT: {
    duckdb_hugeint val = ((duckdb_hugeint *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_hugeint(val));
  }
  case DUCKDB_TYPE_UHUGEINT: {
    duckdb_uhugeint val = ((duckdb_uhugeint *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uhugeint(val));
  }
  case DUCKDB_TYPE_UUID: {
    duckdb_uhugeint val = ((duckdb_uhugeint *)data)[row];
    return duckdb_mb_value_to_bytes(duckdb_create_uuid(val));
  }
  case DUCKDB_TYPE_DECIMAL: {
    duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
    if (!ltype) {
      return moonbit_make_bytes_raw(0);
    }
    duckdb_decimal dec;
    dec.width = duckdb_decimal_width(ltype);
    dec.scale = duckdb_decimal_scale(ltype);
    switch (duckdb_decimal_internal_type(ltype)) {
    case DUCKDB_TYPE_SMALLINT: {
      int64_t v = (int64_t)((int16_t *)data)[row];
      dec.value.lower = (uint64_t)v;
      dec.value.upper = v >> 63;
      break;
    }
    case DUCKDB_TYPE_INTEGER: {
      int64_t v = (int64_t)((int32_t *)data)[row];
      dec.value.lower = (uint64_t)v;
      dec.value.upper = v >> 63;
      break;
    }
    case DUCKDB_TYPE_BIGINT: {
      int64_t v = ((int64_t *)data)[row];
      dec.value.lower = (uint64_t)v;
      dec.value.upper = v >> 63;
      break;
    }
    default: {
      duckdb_hugeint v = ((duckdb_hugeint *)data)[row];
      dec.value = v;
      break;
    }
    }
    duckdb_destroy_logical_type(&ltype);
    return duckdb_mb_value_to_bytes(duckdb_create_decimal(dec));
  }
  case DUCKDB_TYPE_ENUM: {
    duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
    if (!ltype) {
      return moonbit_make_bytes_raw(0);
    }
    // DuckDB stores enum cells as dictionary indices sized by the
    // dictionary: <=256 entries -> uint8, <=65536 -> uint16, else uint32.
    uint32_t dict_size = duckdb_enum_dictionary_size(ltype);
    uint32_t index;
    if (dict_size <= 256) {
      index = ((uint8_t *)data)[row];
    } else if (dict_size <= 65536) {
      index = ((uint16_t *)data)[row];
    } else {
      index = ((uint32_t *)data)[row];
    }
    char *member = duckdb_enum_dictionary_value(ltype, (idx_t)index);
    moonbit_bytes_t bytes =
        member ? duckdb_mb_make_bytes(member, strlen(member))
               : moonbit_make_bytes_raw(0);
    if (member) {
      duckdb_free(member);
    }
    duckdb_destroy_logical_type(&ltype);
    return bytes;
  }
  default:
    return moonbit_make_bytes_raw(0);
  }
}

moonbit_bytes_t duckdb_mb_chunk_value(duckdb_mb_chunk *chunk,
                                      int32_t col,
                                      int32_t row) {
  if (!chunk || !chunk->chunk || !chunk->column_types) {
    return moonbit_make_bytes_raw(0);
  }
  if (col < 0 || col >= chunk->column_count || row < 0) {
    return moonbit_make_bytes_raw(0);
  }
  duckdb_vector vector = duckdb_data_chunk_get_vector(chunk->chunk, (idx_t)col);
  return duckdb_mb_cell_to_bytes(vector, chunk->column_types[col], (idx_t)row);
}

// ============================================================================
// Configuration Functions
// ============================================================================

typedef struct {
  duckdb_config config;
} duckdb_mb_config;

static void duckdb_mb_config_finalize(void *self) {
  duckdb_mb_config *mb_cfg = (duckdb_mb_config *)self;
  if (mb_cfg && mb_cfg->config) {
    duckdb_destroy_config(&mb_cfg->config);
  }
}

duckdb_mb_config *duckdb_mb_config_create(void) {
  duckdb_mb_config *mb_cfg = (duckdb_mb_config *)
      moonbit_make_external_object(duckdb_mb_config_finalize,
                                   sizeof(duckdb_mb_config));
  if (!mb_cfg) {
    duckdb_mb_set_error("failed to allocate config handle");
    return NULL;
  }
  mb_cfg->config = NULL;

  duckdb_state state = duckdb_create_config(&mb_cfg->config);
  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_create_config failed");
    mb_cfg->config = NULL;
  }

  return mb_cfg;
}

// Returns 1 when this call destroyed the config, 0 when it was already
// released.
int32_t duckdb_mb_config_destroy(duckdb_mb_config *mb_cfg) {
  if (!mb_cfg || !mb_cfg->config) {
    return 0;
  }
  duckdb_destroy_config(&mb_cfg->config);
  return 1;
}

int32_t duckdb_mb_config_set(duckdb_mb_config *mb_cfg,
                             moonbit_bytes_t key,
                             moonbit_bytes_t value) {
  if (!mb_cfg || !mb_cfg->config) {
    duckdb_mb_set_error("config is null");
    return 0;
  }

  char *key_c = duckdb_mb_bytes_to_cstr(key);
  if (!key_c) {
    duckdb_mb_set_error("failed to allocate key buffer");
    return 0;
  }

  char *value_c = duckdb_mb_bytes_to_cstr(value);
  if (!value_c) {
    free(key_c);
    duckdb_mb_set_error("failed to allocate value buffer");
    return 0;
  }

  duckdb_state state =
      duckdb_set_config(mb_cfg->config, key_c, value_c);
  free(key_c);
  free(value_c);

  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_set_config failed");
    return 0;
  }

  return 1;
}

duckdb_mb_connection *duckdb_mb_connect_with_config(moonbit_bytes_t path,
                                                     duckdb_mb_config *mb_cfg) {
  if (!mb_cfg || !mb_cfg->config) {
    duckdb_mb_set_error("config is null");
    return NULL;
  }

  duckdb_mb_connection *handle = duckdb_mb_connection_new();
  if (!handle) {
    duckdb_mb_set_error("failed to allocate connection handle");
    return NULL;
  }

  int32_t path_len = path ? Moonbit_array_length(path) : 0;
  char *path_c = NULL;
  const char *path_value = ":memory:";
  if (path_len > 0) {
    path_c = duckdb_mb_bytes_to_cstr(path);
    if (!path_c) {
      duckdb_mb_set_error("failed to allocate path buffer");
      return handle;
    }
    path_value = path_c;
  }

  char *open_error = NULL;
  duckdb_state state = duckdb_open_ext(path_value, &handle->db, mb_cfg->config, &open_error);
  if (state != DuckDBSuccess) {
    if (open_error && open_error[0] != '\0') {
      duckdb_mb_set_error(open_error);
    } else {
      duckdb_mb_set_error("duckdb_open_ext failed");
    }
    if (open_error) {
      duckdb_free(open_error);
    }
    handle->db = NULL;
    free(path_c);
    return handle;
  }

  if (open_error) {
    duckdb_free(open_error);
  }
  free(path_c);

  state = duckdb_connect(handle->db, &handle->conn);
  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_connect failed");
    duckdb_close(&handle->db);
    return handle;
  }

  return handle;
}

int32_t duckdb_mb_is_null_config(duckdb_mb_config *mb_cfg) {
  return mb_cfg == NULL || mb_cfg->config == NULL ? 1 : 0;
}

// ============================================================================
// Prepared Statement Functions
// ============================================================================

static void duckdb_mb_statement_finalize(void *self) {
  duckdb_mb_statement *mb_stmt = (duckdb_mb_statement *)self;
  if (mb_stmt && mb_stmt->stmt) {
    duckdb_destroy_prepare(&mb_stmt->stmt);
  }
}

// Copy the statement's diagnostic to the shared error channel (verbatim,
// dynamically sized). Returns 0 for use as `return duckdb_mb_stmt_fail(..)`.
static int32_t duckdb_mb_stmt_fail(duckdb_mb_statement *mb_stmt) {
  const char *error =
      mb_stmt->stmt ? duckdb_prepare_error(mb_stmt->stmt) : NULL;
  duckdb_mb_set_error(error && error[0] ? error : NULL);
  return 0;
}

duckdb_mb_statement *duckdb_mb_prepare(duckdb_mb_connection *handle,
                                      moonbit_bytes_t sql) {
  duckdb_mb_statement *mb_stmt = (duckdb_mb_statement *)
      moonbit_make_external_object(duckdb_mb_statement_finalize,
                                   sizeof(duckdb_mb_statement));
  if (!mb_stmt) {
    duckdb_mb_set_error("failed to allocate statement handle");
    return NULL;
  }
  mb_stmt->stmt = NULL;

  if (!handle || !handle->conn) {
    duckdb_mb_set_error("connection is null");
    return mb_stmt;
  }
  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    duckdb_mb_set_error("failed to allocate sql buffer");
    return mb_stmt;
  }

  duckdb_state state = duckdb_prepare(handle->conn, sql_c, &mb_stmt->stmt);
  free(sql_c);

  if (state != DuckDBSuccess) {
    // Capture the diagnostic while the failed statement is still alive, so
    // the original parser/catalog message survives the destroy below.
    duckdb_mb_stmt_fail(mb_stmt);
    if (mb_stmt->stmt) {
      duckdb_destroy_prepare(&mb_stmt->stmt);
    }
    return mb_stmt;
  }

  return mb_stmt;
}

// Returns 1 when this call destroyed the statement, 0 when it was already
// closed.
int32_t duckdb_mb_statement_destroy(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_destroy_prepare(&mb_stmt->stmt);
  return 1;
}

int32_t duckdb_mb_bind_int(duckdb_mb_statement *mb_stmt, int32_t index,
                          int32_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_state state = duckdb_bind_int32(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_bigint(duckdb_mb_statement *mb_stmt, int32_t index,
                              int64_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_state state = duckdb_bind_int64(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_double(duckdb_mb_statement *mb_stmt, int32_t index,
                              double value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_state state = duckdb_bind_double(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_varchar(duckdb_mb_statement *mb_stmt, int32_t index,
                               moonbit_bytes_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  char *val_c = duckdb_mb_bytes_to_cstr(value);
  if (!val_c) {
    duckdb_mb_set_error("failed to allocate varchar buffer");
    return 0;
  }
  duckdb_state state =
      duckdb_bind_varchar(mb_stmt->stmt, (idx_t)index, val_c);
  free(val_c);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_bool(duckdb_mb_statement *mb_stmt, int32_t index,
                            bool value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_state state = duckdb_bind_boolean(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_null(duckdb_mb_statement *mb_stmt, int32_t index) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_state state = duckdb_bind_null(mb_stmt->stmt, (idx_t)index);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_clear_bindings(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_clear_bindings(mb_stmt->stmt);
  return 1;
}

duckdb_result *duckdb_mb_execute_prepared(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return NULL;
  }

  duckdb_result *result = (duckdb_result *)malloc(sizeof(duckdb_result));
  if (!result) {
    duckdb_mb_set_error("failed to allocate result");
    return NULL;
  }

  duckdb_state state = duckdb_execute_prepared(mb_stmt->stmt, result);
  if (state != DuckDBSuccess) {
    duckdb_error_type error_type = duckdb_result_error_type(result);
    const char *error = duckdb_result_error(result);
    if (!error) {
      error = "duckdb_execute_prepared failed";
    }
    duckdb_mb_set_error(error);
    duckdb_mb_last_error_code = (int)error_type;
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }

  return result;
}

int32_t duckdb_mb_is_null_statement(duckdb_mb_statement *mb_stmt) {
  return mb_stmt == NULL || mb_stmt->stmt == NULL ? 1 : 0;
}

// ============================================================================
// Appender Functions
// ============================================================================

typedef struct {
  duckdb_appender appender;
} duckdb_mb_appender;

static void duckdb_mb_appender_finalize(void *self) {
  duckdb_mb_appender *mb_append = (duckdb_mb_appender *)self;
  if (mb_append && mb_append->appender) {
    duckdb_appender_destroy(&mb_append->appender);
  }
}

// Copy the appender's diagnostic (verbatim, dynamically sized) to the shared
// error channel, with the engine error type when duckdb_error_data reports
// one. Returns 0 for use as `return duckdb_mb_appender_fail(..)`.
static int32_t duckdb_mb_appender_fail(duckdb_mb_appender *mb_append) {
  if (!mb_append->appender) {
    duckdb_mb_set_error(NULL);
    return 0;
  }
  duckdb_error_data error_data =
      duckdb_appender_error_data(mb_append->appender);
  if (error_data) {
    if (duckdb_error_data_has_error(error_data)) {
      duckdb_mb_set_error_typed(
          duckdb_error_data_message(error_data),
          (int)duckdb_error_data_error_type(error_data));
    } else {
      duckdb_mb_set_error(NULL);
    }
    duckdb_destroy_error_data(&error_data);
    return 0;
  }
  const char *error = duckdb_appender_error(mb_append->appender);
  duckdb_mb_set_error(error && error[0] ? error : NULL);
  return 0;
}

duckdb_mb_appender *duckdb_mb_appender_create(duckdb_mb_connection *handle,
                                             moonbit_bytes_t schema,
                                             moonbit_bytes_t table) {
  duckdb_mb_appender *mb_append = (duckdb_mb_appender *)
      moonbit_make_external_object(duckdb_mb_appender_finalize,
                                   sizeof(duckdb_mb_appender));
  if (!mb_append) {
    duckdb_mb_set_error("failed to allocate appender handle");
    return NULL;
  }
  mb_append->appender = NULL;

  if (!handle || !handle->conn) {
    duckdb_mb_set_error("connection is null");
    return mb_append;
  }

  char *schema_c = duckdb_mb_bytes_to_cstr(schema);
  if (!schema_c) {
    duckdb_mb_set_error("failed to allocate schema buffer");
    return mb_append;
  }

  char *table_c = duckdb_mb_bytes_to_cstr(table);
  if (!table_c) {
    free(schema_c);
    duckdb_mb_set_error("failed to allocate table buffer");
    return mb_append;
  }

  duckdb_state state = duckdb_appender_create(handle->conn, schema_c, table_c,
                                             &mb_append->appender);
  free(schema_c);
  free(table_c);

  if (state != DuckDBSuccess) {
    // Capture the diagnostic while the failed appender is still alive, so
    // the original message (and engine classification) survives destroy.
    duckdb_mb_appender_fail(mb_append);
    if (mb_append->appender) {
      duckdb_appender_destroy(&mb_append->appender);
    }
    return mb_append;
  }

  return mb_append;
}

// Returns 1 when this call destroyed the appender, 0 when it was already
// closed.
int32_t duckdb_mb_appender_destroy(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_appender_destroy(&mb_append->appender);
  return 1;
}

int32_t duckdb_mb_begin_row(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_appender_begin_row(mb_append->appender);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_int(duckdb_mb_appender *mb_append, int32_t value) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_append_int32(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_bigint(duckdb_mb_appender *mb_append, int64_t value) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_append_int64(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_double(duckdb_mb_appender *mb_append, double value) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_append_double(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_varchar(duckdb_mb_appender *mb_append, moonbit_bytes_t value) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  char *val_c = duckdb_mb_bytes_to_cstr(value);
  if (!val_c) {
    duckdb_mb_set_error("failed to allocate varchar buffer");
    return 0;
  }

  duckdb_state state = duckdb_append_varchar(mb_append->appender, val_c);
  free(val_c);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_bool(duckdb_mb_appender *mb_append, bool value) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_append_bool(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_append_null(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_append_null(mb_append->appender);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_end_row(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_appender_end_row(mb_append->appender);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_flush(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  duckdb_state state = duckdb_appender_flush(mb_append->appender);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

int32_t duckdb_mb_is_null_appender(duckdb_mb_appender *mb_append) {
  return mb_append == NULL || mb_append->appender == NULL ? 1 : 0;
}

// ============================================================================
// Date/Timestamp Functions
// ============================================================================

int32_t duckdb_mb_bind_date(duckdb_mb_statement *mb_stmt, int32_t index,
                              int32_t days) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_date date = {days};
  duckdb_state state = duckdb_bind_date(mb_stmt->stmt, (idx_t)index, date);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_bind_timestamp(duckdb_mb_statement *mb_stmt, int32_t index,
                                  int64_t micros) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }
  duckdb_timestamp ts = {micros};
  duckdb_state state =
      duckdb_bind_timestamp(mb_stmt->stmt, (idx_t)index, ts);
  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

// Helper to convert days since epoch to date string "YYYY-MM-DD"
static void days_to_date_string(int32_t days, char *buf, size_t buf_size) {
  // Simplified conversion: days since 1970-01-01
  // This is approximate - a proper implementation would handle leap years
  int32_t year = 1970 + (days / 365);
  int32_t remaining = days % 365;
  if (remaining < 0) {
    year--;
    remaining += 365;
  }
  int32_t month = 1 + (remaining / 30);
  int32_t day = 1 + (remaining % 30);
  snprintf(buf, buf_size, "%04d-%02d-%02d", year, month, day);
}

int32_t duckdb_mb_append_date(duckdb_mb_appender *mb_append, int32_t days) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  char date_buf[32];
  days_to_date_string(days, date_buf, sizeof(date_buf));
  duckdb_state state = duckdb_append_varchar(mb_append->appender, date_buf);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// Helper to convert microseconds since epoch to timestamp string
static void micros_to_timestamp_string(int64_t micros, char *buf, size_t buf_size) {
  // Simplified conversion: microseconds since 1970-01-01 00:00:00
  int64_t seconds = micros / 1000000;
  int32_t year = 1970 + (int32_t)(seconds / 31536000);
  int64_t remaining = seconds % 31536000;
  if (remaining < 0) {
    year--;
    remaining += 31536000;
  }
  int32_t day_of_year = (int32_t)(remaining / 86400);
  int32_t month = 1 + (day_of_year / 30);
  int32_t day = 1 + (day_of_year % 30);
  int32_t hour = (int32_t)((remaining % 86400) / 3600);
  int32_t minute = (int32_t)((remaining % 3600) / 60);
  int32_t sec = (int32_t)(remaining % 60);
  snprintf(buf, buf_size, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, sec);
}

int32_t duckdb_mb_append_timestamp(duckdb_mb_appender *mb_append,
                                    int64_t micros) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  char ts_buf[64];
  micros_to_timestamp_string(micros, ts_buf, sizeof(ts_buf));
  duckdb_state state = duckdb_append_varchar(mb_append->appender, ts_buf);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ============================================================================
// Advanced Data Types Functions
// ============================================================================

// ----------------------------------------------------------------------------
// Blob Type
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_blob(duckdb_mb_statement *mb_stmt, int32_t index,
                             moonbit_bytes_t data, int32_t length) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  void *data_ptr = data;
  duckdb_state state = duckdb_bind_blob(mb_stmt->stmt, (idx_t)index, data_ptr, (idx_t)length);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_append_blob(duckdb_mb_appender *mb_append,
                                moonbit_bytes_t data, int32_t length) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  void *data_ptr = data;
  duckdb_state state = duckdb_append_blob(mb_append->appender, data_ptr, (idx_t)length);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Decimal Type (using duckdb_hugeint)
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_decimal(duckdb_mb_statement *mb_stmt, int32_t index,
                                int32_t width, int32_t scale,
                                uint64_t lower, int64_t upper) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  duckdb_decimal decimal;
  decimal.width = (uint8_t)width;
  decimal.scale = (uint8_t)scale;
  decimal.value.lower = lower;
  decimal.value.upper = upper;

  duckdb_state state = duckdb_bind_decimal(mb_stmt->stmt, (idx_t)index, decimal);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_append_decimal(duckdb_mb_appender *mb_append,
                                  int32_t width, int32_t scale,
                                  uint64_t lower, int64_t upper) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  duckdb_decimal decimal;
  decimal.width = (uint8_t)width;
  decimal.scale = (uint8_t)scale;
  decimal.value.lower = lower;
  decimal.value.upper = upper;

  // Create value from decimal
  duckdb_value val = duckdb_create_decimal(decimal);
  if (!val) {
    duckdb_mb_set_error("failed to create decimal value");
    return 0;
  }

  duckdb_state state = duckdb_append_value(mb_append->appender, val);
  duckdb_destroy_value(&val);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Interval Type
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_interval(duckdb_mb_statement *mb_stmt, int32_t index,
                                 int32_t months, int32_t days, int64_t micros) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  duckdb_interval interval;
  interval.months = months;
  interval.days = days;
  interval.micros = micros;

  duckdb_state state = duckdb_bind_interval(mb_stmt->stmt, (idx_t)index, interval);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

int32_t duckdb_mb_append_interval(duckdb_mb_appender *mb_append,
                                  int32_t months, int32_t days, int64_t micros) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  duckdb_interval interval;
  interval.months = months;
  interval.days = days;
  interval.micros = micros;

  duckdb_state state = duckdb_append_interval(mb_append->appender, interval);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// List Type (VARCHAR list for simplicity)
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_list_varchar(duckdb_mb_statement *mb_stmt, int32_t index,
                                     moonbit_bytes_t *values, int32_t count) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  // For now, serialize list as JSON string for VARCHAR compatibility
  // Calculate total length needed
  size_t total_len = 2; // '[' + ']'
  for (int32_t i = 0; i < count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t len = values[i] ? Moonbit_array_length(values[i]) : 0;
    total_len += (size_t)len + 2; // quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate list buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '[';
  for (int32_t i = 0; i < count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t len = values[i] ? Moonbit_array_length(values[i]) : 0;
    for (int32_t j = 0; j < len; j++) {
      buffer[pos++] = ((char*)values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = ']';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_bind_varchar(mb_stmt->stmt, (idx_t)index, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Struct Type
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_struct_varchar(duckdb_mb_statement *mb_stmt, int32_t index,
                                       moonbit_bytes_t *field_names,
                                       moonbit_bytes_t *field_values,
                                       int32_t field_count) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  // For now, serialize struct as JSON string for VARCHAR compatibility
  // Calculate total length needed
  size_t total_len = 2; // '{' + '}'
  for (int32_t i = 0; i < field_count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t name_len = field_names[i] ? Moonbit_array_length(field_names[i]) : 0;
    int32_t val_len = field_values[i] ? Moonbit_array_length(field_values[i]) : 0;
    total_len += (size_t)name_len + (size_t)val_len + 4; // name, ": ", quotes, quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate struct buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '{';
  for (int32_t i = 0; i < field_count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t name_len = field_names[i] ? Moonbit_array_length(field_names[i]) : 0;
    for (int32_t j = 0; j < name_len; j++) {
      buffer[pos++] = ((char*)field_names[i])[j];
    }
    buffer[pos++] = '"';
    buffer[pos++] = ':';
    buffer[pos++] = ' ';
    buffer[pos++] = '"';
    int32_t val_len = field_values[i] ? Moonbit_array_length(field_values[i]) : 0;
    for (int32_t j = 0; j < val_len; j++) {
      buffer[pos++] = ((char*)field_values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = '}';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_bind_varchar(mb_stmt->stmt, (idx_t)index, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Map Type (represented as list of struct entries with key/value fields)
// ----------------------------------------------------------------------------

int32_t duckdb_mb_bind_map_varchar_varchar(duckdb_mb_statement *mb_stmt, int32_t index,
                                            moonbit_bytes_t *keys,
                                            moonbit_bytes_t *values,
                                            int32_t entry_count) {
  if (!mb_stmt || !mb_stmt->stmt) {
    duckdb_mb_set_error("statement is null");
    return 0;
  }

  // For now, serialize map as JSON string for VARCHAR compatibility
  // Calculate total length needed
  size_t total_len = 2; // '{' + '}'
  for (int32_t i = 0; i < entry_count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t key_len = keys[i] ? Moonbit_array_length(keys[i]) : 0;
    int32_t val_len = values[i] ? Moonbit_array_length(values[i]) : 0;
    total_len += (size_t)key_len + (size_t)val_len + 6; // key, ": ", value, quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate map buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '{';
  for (int32_t i = 0; i < entry_count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t key_len = keys[i] ? Moonbit_array_length(keys[i]) : 0;
    for (int32_t j = 0; j < key_len; j++) {
      buffer[pos++] = ((char*)keys[i])[j];
    }
    buffer[pos++] = '"';
    buffer[pos++] = ':';
    buffer[pos++] = ' ';
    buffer[pos++] = '"';
    int32_t val_len = values[i] ? Moonbit_array_length(values[i]) : 0;
    for (int32_t j = 0; j < val_len; j++) {
      buffer[pos++] = ((char*)values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = '}';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_bind_varchar(mb_stmt->stmt, (idx_t)index, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_stmt_fail(mb_stmt);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// List Type Appender
// ----------------------------------------------------------------------------

int32_t duckdb_mb_append_list_varchar(duckdb_mb_appender *mb_append,
                                     moonbit_bytes_t *values, int32_t count) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  // Serialize list as JSON string for VARCHAR compatibility
  size_t total_len = 2; // '[' + ']'
  for (int32_t i = 0; i < count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t len = values[i] ? Moonbit_array_length(values[i]) : 0;
    total_len += (size_t)len + 2; // quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate list buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '[';
  for (int32_t i = 0; i < count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t len = values[i] ? Moonbit_array_length(values[i]) : 0;
    for (int32_t j = 0; j < len; j++) {
      buffer[pos++] = ((char*)values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = ']';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_append_varchar(mb_append->appender, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Struct Type Appender
// ----------------------------------------------------------------------------

int32_t duckdb_mb_append_struct_varchar(duckdb_mb_appender *mb_append,
                                       moonbit_bytes_t *field_names,
                                       moonbit_bytes_t *field_values,
                                       int32_t field_count) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  // Serialize struct as JSON string for VARCHAR compatibility
  size_t total_len = 2; // '{' + '}'
  for (int32_t i = 0; i < field_count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t name_len = field_names[i] ? Moonbit_array_length(field_names[i]) : 0;
    int32_t val_len = field_values[i] ? Moonbit_array_length(field_values[i]) : 0;
    total_len += (size_t)name_len + (size_t)val_len + 4; // name, ": ", quotes, quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate struct buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '{';
  for (int32_t i = 0; i < field_count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t name_len = field_names[i] ? Moonbit_array_length(field_names[i]) : 0;
    for (int32_t j = 0; j < name_len; j++) {
      buffer[pos++] = ((char*)field_names[i])[j];
    }
    buffer[pos++] = '"';
    buffer[pos++] = ':';
    buffer[pos++] = ' ';
    buffer[pos++] = '"';
    int32_t val_len = field_values[i] ? Moonbit_array_length(field_values[i]) : 0;
    for (int32_t j = 0; j < val_len; j++) {
      buffer[pos++] = ((char*)field_values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = '}';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_append_varchar(mb_append->appender, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// Map Type Appender
// ----------------------------------------------------------------------------

int32_t duckdb_mb_append_map_varchar_varchar(duckdb_mb_appender *mb_append,
                                            moonbit_bytes_t *keys,
                                            moonbit_bytes_t *values,
                                            int32_t entry_count) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  // Serialize map as JSON string for VARCHAR compatibility
  size_t total_len = 2; // '{' + '}'
  for (int32_t i = 0; i < entry_count; i++) {
    if (i > 0) total_len += 2; // ", "
    int32_t key_len = keys[i] ? Moonbit_array_length(keys[i]) : 0;
    int32_t val_len = values[i] ? Moonbit_array_length(values[i]) : 0;
    total_len += (size_t)key_len + (size_t)val_len + 6; // key, ": ", value, quotes
  }

  char *buffer = (char *)malloc(total_len + 1);
  if (!buffer) {
    duckdb_mb_set_error("failed to allocate map buffer");
    return 0;
  }

  size_t pos = 0;
  buffer[pos++] = '{';
  for (int32_t i = 0; i < entry_count; i++) {
    if (i > 0) {
      buffer[pos++] = ',';
      buffer[pos++] = ' ';
    }
    buffer[pos++] = '"';
    int32_t key_len = keys[i] ? Moonbit_array_length(keys[i]) : 0;
    for (int32_t j = 0; j < key_len; j++) {
      buffer[pos++] = ((char*)keys[i])[j];
    }
    buffer[pos++] = '"';
    buffer[pos++] = ':';
    buffer[pos++] = ' ';
    buffer[pos++] = '"';
    int32_t val_len = values[i] ? Moonbit_array_length(values[i]) : 0;
    for (int32_t j = 0; j < val_len; j++) {
      buffer[pos++] = ((char*)values[i])[j];
    }
    buffer[pos++] = '"';
  }
  buffer[pos++] = '}';
  buffer[pos] = '\0';

  duckdb_state state = duckdb_append_varchar(mb_append->appender, buffer);
  free(buffer);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ============================================================================
// DataChunk API for Advanced Type Appending
// ============================================================================

typedef struct {
  duckdb_logical_type type;
} duckdb_mb_logical_type;

typedef struct {
  duckdb_data_chunk chunk;
} duckdb_mb_data_chunk;

typedef struct {
  duckdb_vector vector;
} duckdb_mb_vector;

// ----------------------------------------------------------------------------
// LogicalType Functions
// ----------------------------------------------------------------------------

duckdb_mb_logical_type *duckdb_mb_create_logical_type(duckdb_type type_id) {
  duckdb_logical_type type = duckdb_create_logical_type(type_id);
  if (!type) {
    return NULL;
  }
  duckdb_mb_logical_type *mb_type = (duckdb_mb_logical_type *)malloc(sizeof(duckdb_mb_logical_type));
  if (!mb_type) {
    duckdb_destroy_logical_type(&type);
    return NULL;
  }
  mb_type->type = type;
  return mb_type;
}

duckdb_mb_logical_type *duckdb_mb_create_list_type(duckdb_mb_logical_type *child_type) {
  if (!child_type) {
    return NULL;
  }
  duckdb_logical_type type = duckdb_create_list_type(child_type->type);
  if (!type) {
    return NULL;
  }
  duckdb_mb_logical_type *mb_type = (duckdb_mb_logical_type *)malloc(sizeof(duckdb_mb_logical_type));
  if (!mb_type) {
    duckdb_destroy_logical_type(&type);
    return NULL;
  }
  mb_type->type = type;
  return mb_type;
}

duckdb_mb_logical_type *duckdb_mb_create_struct_type(
    duckdb_logical_type *member_types,
    const char **member_names,
    idx_t member_count) {
  duckdb_logical_type type = duckdb_create_struct_type(member_types, member_names, member_count);
  if (!type) {
    return NULL;
  }
  duckdb_mb_logical_type *mb_type = (duckdb_mb_logical_type *)malloc(sizeof(duckdb_mb_logical_type));
  if (!mb_type) {
    duckdb_destroy_logical_type(&type);
    return NULL;
  }
  mb_type->type = type;
  return mb_type;
}

duckdb_mb_logical_type *duckdb_mb_create_map_type(
    duckdb_logical_type *key_type,
    duckdb_logical_type *value_type) {
  if (!key_type || !value_type) {
    return NULL;
  }
  duckdb_logical_type type = duckdb_create_map_type(*key_type, *value_type);
  if (!type) {
    return NULL;
  }
  duckdb_mb_logical_type *mb_type = (duckdb_mb_logical_type *)malloc(sizeof(duckdb_mb_logical_type));
  if (!mb_type) {
    duckdb_destroy_logical_type(&type);
    return NULL;
  }
  mb_type->type = type;
  return mb_type;
}

void duckdb_mb_destroy_logical_type(duckdb_mb_logical_type *mb_type) {
  if (!mb_type) {
    return;
  }
  if (mb_type->type) {
    duckdb_destroy_logical_type(&mb_type->type);
  }
  free(mb_type);
}

int32_t duckdb_mb_is_null_logical_type(duckdb_mb_logical_type *mb_type) {
  return mb_type == NULL ? 1 : 0;
}

// ----------------------------------------------------------------------------
// DataChunk Functions
// ----------------------------------------------------------------------------

duckdb_mb_data_chunk *duckdb_mb_create_data_chunk(
    duckdb_logical_type *types,
    idx_t column_count) {
  duckdb_data_chunk chunk = duckdb_create_data_chunk(types, column_count);
  if (!chunk) {
    return NULL;
  }
  duckdb_mb_data_chunk *mb_chunk = (duckdb_mb_data_chunk *)malloc(sizeof(duckdb_mb_data_chunk));
  if (!mb_chunk) {
    duckdb_destroy_data_chunk(&chunk);
    return NULL;
  }
  mb_chunk->chunk = chunk;
  return mb_chunk;
}

void duckdb_mb_destroy_data_chunk(duckdb_mb_data_chunk *mb_chunk) {
  if (!mb_chunk) {
    return;
  }
  if (mb_chunk->chunk) {
    duckdb_destroy_data_chunk(&mb_chunk->chunk);
  }
  free(mb_chunk);
}

duckdb_vector duckdb_mb_data_chunk_get_vector(duckdb_mb_data_chunk *mb_chunk, idx_t col_idx) {
  if (!mb_chunk || !mb_chunk->chunk) {
    duckdb_vector v = {0};
    return v;
  }
  return duckdb_data_chunk_get_vector(mb_chunk->chunk, col_idx);
}

void duckdb_mb_data_chunk_set_size(duckdb_mb_data_chunk *mb_chunk, idx_t size) {
  if (!mb_chunk || !mb_chunk->chunk) {
    return;
  }
  duckdb_data_chunk_set_size(mb_chunk->chunk, size);
}

void duckdb_mb_data_chunk_reset(duckdb_mb_data_chunk *mb_chunk) {
  if (!mb_chunk || !mb_chunk->chunk) {
    return;
  }
  duckdb_data_chunk_reset(mb_chunk->chunk);
}

int32_t duckdb_mb_is_null_data_chunk(duckdb_mb_data_chunk *mb_chunk) {
  return mb_chunk == NULL ? 1 : 0;
}

// ----------------------------------------------------------------------------
// Vector Functions
// ----------------------------------------------------------------------------

void *duckdb_mb_vector_get_data(duckdb_vector vector) {
  return duckdb_vector_get_data(vector);
}

uint64_t *duckdb_mb_vector_get_validity(duckdb_vector vector) {
  return duckdb_vector_get_validity(vector);
}

duckdb_vector duckdb_mb_list_vector_get_child(duckdb_vector vector) {
  return duckdb_list_vector_get_child(vector);
}

duckdb_state duckdb_mb_list_vector_set_size(duckdb_vector vector, idx_t size) {
  return duckdb_list_vector_set_size(vector, size);
}

duckdb_state duckdb_mb_list_vector_reserve(duckdb_vector vector, idx_t capacity) {
  return duckdb_list_vector_reserve(vector, capacity);
}

// ----------------------------------------------------------------------------
// DataChunk Appender Function
// ----------------------------------------------------------------------------

int32_t duckdb_mb_append_data_chunk(
    duckdb_mb_appender *mb_append,
    duckdb_mb_data_chunk *mb_chunk) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }
  if (!mb_chunk || !mb_chunk->chunk) {
    duckdb_mb_set_error("data_chunk is null");
    return 0;
  }

  duckdb_state state = duckdb_append_data_chunk(mb_append->appender, mb_chunk->chunk);
  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }
  return 1;
}

// ----------------------------------------------------------------------------
// List Appender using DataChunk API
// ----------------------------------------------------------------------------

// Helper to append a VARCHAR list using duckdb_append_data_chunk
// This follows the approach recommended by DuckDB maintainers in GitHub #3740
int32_t duckdb_mb_append_list_varchar_chunk(
    duckdb_mb_appender *mb_append,
    moonbit_bytes_t *values,
    int32_t count) {
  if (!mb_append || !mb_append->appender) {
    duckdb_mb_set_error("appender is null");
    return 0;
  }

  duckdb_logical_type varchar_type =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  if (!varchar_type) {
    duckdb_mb_set_error("failed to create varchar type");
    return 0;
  }

  duckdb_logical_type list_type = duckdb_create_list_type(varchar_type);
  if (!list_type) {
    duckdb_mb_set_error("failed to create list type");
    return 0;
  }

  duckdb_value *child_values = NULL;
  if (count > 0) {
    child_values = (duckdb_value *)malloc(sizeof(duckdb_value) * (size_t)count);
    if (!child_values) {
      duckdb_mb_set_error("failed to allocate list values");
      duckdb_destroy_logical_type(&list_type);
      return 0;
    }
  }

  for (int32_t i = 0; i < count; i++) {
    int32_t len = values[i] ? Moonbit_array_length(values[i]) : 0;
    const char *str = values[i] ? (const char *)values[i] : "";
    child_values[i] = duckdb_create_varchar_length(str, (idx_t)len);
  }

  duckdb_value list_value =
      duckdb_create_list_value(list_type, child_values, (idx_t)count);

  duckdb_state state = duckdb_append_value(mb_append->appender, list_value);

  duckdb_destroy_value(&list_value);
  for (int32_t i = 0; i < count; i++) {
    duckdb_destroy_value(&child_values[i]);
  }
  free(child_values);
  duckdb_destroy_logical_type(&list_type);

  if (state != DuckDBSuccess) {
    return duckdb_mb_appender_fail(mb_append);
  }

  return 1;
}

// ============================================================================
// Arrow Integration (using standard DuckDB API for data extraction)
// ============================================================================

typedef struct {
  duckdb_result result;  // Store standard DuckDB result
  int32_t closed;
  int32_t column_count;
  int32_t row_count;
} duckdb_mb_arrow_result;

static void duckdb_mb_arrow_result_finalize(void *self) {
  duckdb_mb_arrow_result *arrow_result = (duckdb_mb_arrow_result *)self;
  if (arrow_result && !arrow_result->closed) {
    duckdb_destroy_result(&arrow_result->result);
    arrow_result->closed = 1;
  }
}

duckdb_mb_arrow_result *duckdb_mb_query_arrow(duckdb_mb_connection *handle,
                                              moonbit_bytes_t sql) {
  if (!handle || !handle->conn) {
    duckdb_mb_set_error("connection is null");
    return NULL;
  }

  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    duckdb_mb_set_error("failed to allocate SQL buffer");
    return NULL;
  }

  duckdb_mb_arrow_result *arrow_result = (duckdb_mb_arrow_result *)
      moonbit_make_external_object(duckdb_mb_arrow_result_finalize,
                                   sizeof(duckdb_mb_arrow_result));
  if (!arrow_result) {
    free(sql_c);
    duckdb_mb_set_error("failed to allocate arrow result handle");
    return NULL;
  }

  arrow_result->closed = 0;
  arrow_result->column_count = 0;
  arrow_result->row_count = 0;

  // Use standard DuckDB query instead of Arrow API
  duckdb_state state = duckdb_query(handle->conn, sql_c, &arrow_result->result);
  free(sql_c);

  if (state != DuckDBSuccess) {
    // Copy the diagnostic before destroying the failed result.
    duckdb_error_type error_type =
        duckdb_result_error_type(&arrow_result->result);
    const char *error = duckdb_result_error(&arrow_result->result);
    duckdb_mb_set_error_typed(
        error && error[0] ? error : "duckdb_query failed",
        (int)error_type);
    duckdb_destroy_result(&arrow_result->result);
    arrow_result->closed = 1;
    return arrow_result;
  }

  arrow_result->column_count = (int32_t)duckdb_column_count(&arrow_result->result);
  arrow_result->row_count = (int32_t)duckdb_row_count(&arrow_result->result);

  return arrow_result;
}

int32_t duckdb_mb_arrow_column_count(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result || arrow_result->closed) {
    return 0;
  }
  return arrow_result->column_count;
}

int32_t duckdb_mb_arrow_row_count(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result || arrow_result->closed) {
    return 0;
  }
  return arrow_result->row_count;
}

// Get schema as JSON string for MoonBit parsing
moonbit_bytes_t duckdb_mb_arrow_schema(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("[]", 2);
  }

  int32_t col_count = arrow_result->column_count;
  if (col_count <= 0) {
    return duckdb_mb_make_bytes("[]", 2);
  }

  // Allocate buffer for JSON (rough estimate: 200 bytes per field)
  size_t json_capacity = col_count * 200 + 10;
  char *json_buffer = (char *)malloc(json_capacity);
  if (!json_buffer) {
    return duckdb_mb_make_bytes("[]", 2);
  }

  // Build JSON: [{"name":"...","nullable":true/false,"type_id":"..."},...]
  size_t pos = 0;
  json_buffer[pos++] = '[';

  for (int32_t i = 0; i < col_count; i++) {
    const char *name = duckdb_column_name(&arrow_result->result, i);
    if (!name) name = "";

    // Get type info from column
    duckdb_type type = duckdb_column_type(&arrow_result->result, i);
    const char *type_id = "string";

    switch (type) {
      case DUCKDB_TYPE_INVALID:
        type_id = "string";
        break;
      case DUCKDB_TYPE_BOOLEAN:
        type_id = "bool";
        break;
      case DUCKDB_TYPE_TINYINT:
      case DUCKDB_TYPE_SMALLINT:
      case DUCKDB_TYPE_INTEGER:
        type_id = "int32";
        break;
      case DUCKDB_TYPE_BIGINT:
        type_id = "int64";
        break;
      case DUCKDB_TYPE_FLOAT:
      case DUCKDB_TYPE_DOUBLE:
        type_id = "double";
        break;
      case DUCKDB_TYPE_VARCHAR:
        type_id = "string";
        break;
      default:
        type_id = "string";
        break;
    }

    // Write field JSON
    pos += snprintf(json_buffer + pos, json_capacity - pos,
      "%s{\"name\":\"%s\",\"nullable\":true,\"type_id\":\"%s\"}",
      (i == 0) ? "" : ",",
      name,
      type_id);
  }

  json_buffer[pos++] = ']';
  json_buffer[pos] = '\0';

  moonbit_bytes_t result = duckdb_mb_make_bytes(json_buffer, pos);
  free(json_buffer);
  return result;
}

// Helper to extract column data as primitive arrays
// Returns data in a compact binary format: [count (4 bytes), value1, value2, ...]
moonbit_bytes_t duckdb_mb_arrow_get_column_int32(duckdb_mb_arrow_result *arrow_result,
                                                  int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 4 bytes values]
  int32_t total_size = 4 + row_count * 4;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out = (int32_t *)result;
  out[0] = row_count;

  // Copy values from result
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      out[i + 1] = 0;
    } else {
      // Try to get as integer (duckdb doesn't have direct int32 getter, use int64)
      int64_t val = duckdb_value_int64(&arrow_result->result, col_idx, i);
      out[i + 1] = (int32_t)val;
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_int64(duckdb_mb_arrow_result *arrow_result,
                                                  int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 8 bytes values]
  int32_t total_size = 4 + row_count * 8;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Copy values from result
  int64_t *out_data = (int64_t *)((char *)result + 4);
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      out_data[i] = 0;
    } else {
      out_data[i] = duckdb_value_int64(&arrow_result->result, col_idx, i);
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_double(duckdb_mb_arrow_result *arrow_result,
                                                   int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 8 bytes values]
  int32_t total_size = 4 + row_count * 8;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Copy values from result
  double *out_data = (double *)((char *)result + 4);
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      out_data[i] = 0.0;
    } else {
      out_data[i] = duckdb_value_double(&arrow_result->result, col_idx, i);
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_string(duckdb_mb_arrow_result *arrow_result,
                                                   int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // First pass: calculate total string length
  size_t total_data_len = 0;
  char **strings = (char **)malloc(row_count * sizeof(char *));
  if (!strings) {
    return duckdb_mb_make_bytes("", 0);
  }

  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      strings[i] = NULL;
    } else {
      strings[i] = duckdb_value_varchar(&arrow_result->result, col_idx, i);
      if (strings[i]) {
        total_data_len += strlen(strings[i]) + 1;  // +1 for null terminator
      } else {
        total_data_len += 1;  // Empty string
      }
    }
  }

  // Allocate: [count (4 bytes)][total_data_len (4 bytes)][string data...]
  int32_t total_size = 4 + 4 + (int32_t)total_data_len;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count and total data length
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;
  out_header[1] = (int32_t)total_data_len;

  // Copy string data
  char *out_data = (char *)result + 8;
  size_t out_pos = 0;

  for (int32_t i = 0; i < row_count; i++) {
    if (strings[i]) {
      size_t len = strlen(strings[i]);
      memcpy(out_data + out_pos, strings[i], len);
      out_pos += len;
      out_data[out_pos++] = '\0';
      duckdb_free(strings[i]);
    } else {
      out_data[out_pos++] = '\0';
    }
  }

  free(strings);
  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_bool(duckdb_mb_arrow_result *arrow_result,
                                                 int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count bytes values]
  int32_t total_size = 4 + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Copy values from result
  uint8_t *out_data = (uint8_t *)result + 4;
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      out_data[i] = 0;
    } else {
      out_data[i] = duckdb_value_boolean(&arrow_result->result, col_idx, i) ? 1 : 0;
    }
  }

  return result;
}

// Returns 1 when this call destroyed the result, 0 when it was already
// closed.
int32_t duckdb_mb_arrow_destroy(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result || arrow_result->closed) {
    return 0;
  }
  duckdb_destroy_result(&arrow_result->result);
  arrow_result->closed = 1;
  return 1;
}

int32_t duckdb_mb_is_null_arrow_result(duckdb_mb_arrow_result *arrow_result) {
  return arrow_result == NULL || arrow_result->closed ? 1 : 0;
}

// Helper function to convert 8 bytes at offset to double (IEEE 754 bit-cast)
double duckdb_mb_bytes_to_double(const char *bytes, int32_t offset) {
  double result;
  memcpy(&result, bytes + offset, sizeof(double));
  return result;
}

// ============================================================================
// Nullable Column Getters - Return values with validity mask
// Format: [count (4 bytes)][values...][validity (row_count bytes, 1=true/0=false)]
// ============================================================================

moonbit_bytes_t duckdb_mb_arrow_get_column_int32_nullable(
    duckdb_mb_arrow_result *arrow_result,
    int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 4 bytes values][row_count bytes validity]
  int32_t total_size = 4 + row_count * 4 + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out = (int32_t *)result;
  out[0] = row_count;

  // Data starts after count
  int32_t *values_out = out + 1;
  uint8_t *validity_out = (uint8_t *)result + 4 + row_count * 4;

  // Copy values and validity from result
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      values_out[i] = 0;  // Placeholder value for null
      validity_out[i] = 0;
    } else {
      int64_t val = duckdb_value_int64(&arrow_result->result, col_idx, i);
      values_out[i] = (int32_t)val;
      validity_out[i] = 1;
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_int64_nullable(
    duckdb_mb_arrow_result *arrow_result,
    int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 8 bytes values][row_count bytes validity]
  int32_t total_size = 4 + row_count * 8 + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Data starts after count
  int64_t *values_out = (int64_t *)((char *)result + 4);
  uint8_t *validity_out = (uint8_t *)result + 4 + row_count * 8;

  // Copy values and validity from result
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      values_out[i] = 0;
      validity_out[i] = 0;
    } else {
      values_out[i] = duckdb_value_int64(&arrow_result->result, col_idx, i);
      validity_out[i] = 1;
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_double_nullable(
    duckdb_mb_arrow_result *arrow_result,
    int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count * 8 bytes values][row_count bytes validity]
  int32_t total_size = 4 + row_count * 8 + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Data starts after count
  double *values_out = (double *)((char *)result + 4);
  uint8_t *validity_out = (uint8_t *)result + 4 + row_count * 8;

  // Copy values and validity from result
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      values_out[i] = 0.0;
      validity_out[i] = 0;
    } else {
      values_out[i] = duckdb_value_double(&arrow_result->result, col_idx, i);
      validity_out[i] = 1;
    }
  }

  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_string_nullable(
    duckdb_mb_arrow_result *arrow_result,
    int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // First pass: calculate total string length
  size_t total_data_len = 0;
  char **strings = (char **)malloc(row_count * sizeof(char *));
  if (!strings) {
    return duckdb_mb_make_bytes("", 0);
  }

  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      strings[i] = NULL;
    } else {
      strings[i] = duckdb_value_varchar(&arrow_result->result, col_idx, i);
      if (strings[i]) {
        total_data_len += strlen(strings[i]) + 1;  // +1 for null terminator
      } else {
        total_data_len += 1;  // Empty string
      }
    }
  }

  // Allocate: [count (4 bytes)][total_data_len (4 bytes)][string data...][validity (row_count bytes)]
  int32_t total_size = 4 + 4 + (int32_t)total_data_len + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count and total data length
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;
  out_header[1] = (int32_t)total_data_len;

  // Copy string data
  char *out_data = (char *)result + 8;
  size_t out_pos = 0;

  for (int32_t i = 0; i < row_count; i++) {
    if (strings[i]) {
      size_t len = strlen(strings[i]);
      memcpy(out_data + out_pos, strings[i], len);
      out_pos += len;
      out_data[out_pos++] = '\0';
      duckdb_free(strings[i]);
    } else {
      out_data[out_pos++] = '\0';
    }
  }

  // Write validity at the end
  uint8_t *validity_out = (uint8_t *)result + 8 + total_data_len;
  for (int32_t i = 0; i < row_count; i++) {
    validity_out[i] = (strings[i] != NULL || duckdb_value_is_null(&arrow_result->result, col_idx, i)) ? 0 : 1;
    // Actually, strings[i] was NULL if the value was null
    // So we need to re-check
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      validity_out[i] = 0;
    } else {
      validity_out[i] = 1;
    }
  }

  free(strings);
  return result;
}

moonbit_bytes_t duckdb_mb_arrow_get_column_bool_nullable(
    duckdb_mb_arrow_result *arrow_result,
    int32_t col_idx) {
  if (!arrow_result || arrow_result->closed) {
    return duckdb_mb_make_bytes("", 0);
  }

  int32_t row_count = arrow_result->row_count;
  if (col_idx < 0 || col_idx >= arrow_result->column_count || row_count <= 0) {
    return duckdb_mb_make_bytes("", 0);
  }

  // Allocate: [count (4 bytes)][row_count bytes values][row_count bytes validity]
  int32_t total_size = 4 + row_count + row_count;
  moonbit_bytes_t result = moonbit_make_bytes_raw(total_size);

  // Write count
  int32_t *out_header = (int32_t *)result;
  out_header[0] = row_count;

  // Data starts after count
  uint8_t *values_out = (uint8_t *)result + 4;
  uint8_t *validity_out = values_out + row_count;

  // Copy values and validity from result
  for (int32_t i = 0; i < row_count; i++) {
    if (duckdb_value_is_null(&arrow_result->result, col_idx, i)) {
      values_out[i] = 0;
      validity_out[i] = 0;
    } else {
      values_out[i] = duckdb_value_boolean(&arrow_result->result, col_idx, i) ? 1 : 0;
      validity_out[i] = 1;
    }
  }

  return result;
}

// ============================================================================
// Canonical typed vector readers (issue #61)
// ============================================================================
//
// These functions bulk-copy one column of a fetched data chunk into owned
// MoonBit arrays. Borrowed `duckdb_vector` handles never escape C — each
// call copies into a freshly allocated MoonBit array, so the resulting
// `Vector` outlives the chunk it was decoded from.

// Borrowed vector handle for column `col` of a fetched chunk. The handle is
// only valid while `chunk` is alive — materialize it into owned MoonBit
// arrays before destroying the chunk.
duckdb_vector duckdb_mb_chunk_vector(duckdb_mb_chunk *chunk, int32_t col) {
  if (!chunk || !chunk->chunk || col < 0 || col >= chunk->column_count) {
    return NULL;
  }
  return duckdb_data_chunk_get_vector(chunk->chunk, (idx_t)col);
}

// Column type ids of a fetched chunk (as declared at query time).
duckdb_type duckdb_mb_chunk_column_type(duckdb_mb_chunk *chunk, int32_t col) {
  if (!chunk || !chunk->column_types || col < 0 || col >= chunk->column_count) {
    return DUCKDB_TYPE_INVALID;
  }
  return chunk->column_types[col];
}

// 0/1 validity flags for `rows` rows of `vector` (1 = valid). Returns NULL on
// invalid input; an all-valid column yields all ones.
int32_t *duckdb_mb_vector_validity_flags(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  int32_t *flags = moonbit_make_int32_array_raw(rows);
  if (!flags) {
    return NULL;
  }
  uint64_t *validity = duckdb_vector_get_validity(vector);
  for (int32_t row = 0; row < rows; row++) {
    flags[row] =
        (!validity || duckdb_validity_row_is_valid(validity, (idx_t)row)) ? 1
                                                                        : 0;
  }
  return flags;
}

// BOOLEAN column -> int32 flags.
int32_t *duckdb_mb_vector_bool(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  bool *data = (bool *)duckdb_vector_get_data(vector);
  int32_t *out = moonbit_make_int32_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    out[row] = data[row] ? 1 : 0;
  }
  return out;
}

// TINYINT/SMALLINT/INTEGER/UTINYINT/USMALLINT/DATE column -> int32 values.
int32_t *duckdb_mb_vector_i32(duckdb_vector vector,
                              duckdb_type type,
                              int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int32_t *out = moonbit_make_int32_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  switch (type) {
  case DUCKDB_TYPE_TINYINT: {
    int8_t *src = (int8_t *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row];
    }
    break;
  }
  case DUCKDB_TYPE_SMALLINT: {
    int16_t *src = (int16_t *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row];
    }
    break;
  }
  case DUCKDB_TYPE_UTINYINT: {
    uint8_t *src = (uint8_t *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row];
    }
    break;
  }
  case DUCKDB_TYPE_USMALLINT: {
    uint16_t *src = (uint16_t *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row];
    }
    break;
  }
  default:
    memcpy(out, data, sizeof(int32_t) * (size_t)rows);
    break;
  }
  return out;
}

// BIGINT/UINTEGER/TIME/TIMESTAMP*/TIME_NS column -> int64 values.
// Payloads keep DuckDB's raw per-type unit (TIMESTAMP_S seconds,
// TIMESTAMP_MS millis, TIMESTAMP_NS nanos, others micros/plain ints);
// the MoonBit side renders each `ColumnType` in its stored unit.
int64_t *duckdb_mb_vector_i64(duckdb_vector vector,
                              duckdb_type type,
                              int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  if (type == DUCKDB_TYPE_UINTEGER) {
    uint32_t *src = (uint32_t *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = (int64_t)src[row];
    }
  } else if (type == DUCKDB_TYPE_TIMESTAMP_S) {
    // Seconds -> microseconds (canonical timestamp unit).
    duckdb_timestamp *src = (duckdb_timestamp *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row].micros * 1000000;
    }
  } else if (type == DUCKDB_TYPE_TIMESTAMP_MS) {
    // Millis -> microseconds.
    duckdb_timestamp *src = (duckdb_timestamp *)data;
    for (int32_t row = 0; row < rows; row++) {
      out[row] = src[row].micros * 1000;
    }
  } else {
    // TIMESTAMP_NS keeps nanoseconds; everything else is already
    // stored as int64 micros or plain ints.
    memcpy(out, data, sizeof(int64_t) * (size_t)rows);
  }
  return out;
}

// UBIGINT column -> raw uint64 bits (int64 array reinterpreted on the
// MoonBit side as FixedArray[UInt64]).
int64_t *duckdb_mb_vector_u64(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  memcpy(out, data, sizeof(uint64_t) * (size_t)rows);
  return out;
}

// FLOAT column.
float *duckdb_mb_vector_f32(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  float *out = moonbit_make_float_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  memcpy(out, data, sizeof(float) * (size_t)rows);
  return out;
}

// DOUBLE column.
double *duckdb_mb_vector_f64(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  double *out = moonbit_make_double_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  memcpy(out, data, sizeof(double) * (size_t)rows);
  return out;
}

// VARCHAR/ENUM column -> ref array of utf8 byte strings (MoonBit decodes
// with the same lossy-utf8 path as the legacy row API).
void **duckdb_mb_vector_strings(duckdb_vector vector,
                                duckdb_type type,
                                int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  void **out = moonbit_make_ref_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  duckdb_logical_type ltype = NULL;
  if (type == DUCKDB_TYPE_ENUM) {
    ltype = duckdb_vector_get_column_type(vector);
  }
  for (int32_t row = 0; row < rows; row++) {
    if (type == DUCKDB_TYPE_ENUM) {
      uint32_t dict_size = ltype ? duckdb_enum_dictionary_size(ltype) : 0;
      uint32_t index;
      if (dict_size <= 256) {
        index = ((uint8_t *)data)[row];
      } else if (dict_size <= 65536) {
        index = ((uint16_t *)data)[row];
      } else {
        index = ((uint32_t *)data)[row];
      }
      char *member =
          ltype ? duckdb_enum_dictionary_value(ltype, (idx_t)index) : NULL;
      out[row] = member ? duckdb_mb_make_bytes(member, strlen(member))
                        : moonbit_make_bytes_raw(0);
      if (member) {
        duckdb_free(member);
      }
    } else {
      duckdb_string_t str = ((duckdb_string_t *)data)[row];
      const char *ptr = duckdb_string_t_data(&str);
      idx_t len = duckdb_string_t_length(str);
      out[row] = ptr ? duckdb_mb_make_bytes(ptr, len) : moonbit_make_bytes_raw(0);
    }
  }
  if (ltype) {
    duckdb_destroy_logical_type(&ltype);
  }
  return out;
}

// BLOB column -> ref array of raw byte strings.
void **duckdb_mb_vector_blobs(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  void **out = moonbit_make_ref_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    duckdb_string_t blob = ((duckdb_string_t *)data)[row];
    const char *ptr = duckdb_string_t_data(&blob);
    idx_t len = duckdb_string_t_length(blob);
    out[row] = ptr ? duckdb_mb_make_bytes(ptr, len) : moonbit_make_bytes_raw(0);
  }
  return out;
}

// HUGEINT/UHUGEINT/UUID/DECIMAL 128-bit columns: lower halves as uint64 bits.
int64_t *duckdb_mb_vector_i128_lo(duckdb_vector vector,
                                  duckdb_type type,
                                  int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    if (type == DUCKDB_TYPE_UHUGEINT) {
      out[row] = (int64_t)((duckdb_uhugeint *)data)[row].lower;
    } else if (type == DUCKDB_TYPE_UUID) {
      out[row] = (int64_t)((duckdb_uhugeint *)data)[row].lower;
    } else {
      out[row] = (int64_t)((duckdb_hugeint *)data)[row].lower;
    }
  }
  return out;
}

// HUGEINT/UHUGEINT/UUID 128-bit columns: upper halves. UUID upper bits are
// converted back to uhugeint space (duckdb_uhugeint) by flipping the sign bit.
int64_t *duckdb_mb_vector_i128_hi(duckdb_vector vector,
                                duckdb_type type,
                                int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    if (type == DUCKDB_TYPE_UHUGEINT) {
      out[row] = (int64_t)((duckdb_uhugeint *)data)[row].upper;
    } else if (type == DUCKDB_TYPE_UUID) {
      // Stored as hugeint with the sign bit flipped.
      out[row] = (int64_t)(((duckdb_uhugeint *)data)[row].upper ^
                           0x8000000000000000ULL);
    } else {
      out[row] = ((duckdb_hugeint *)data)[row].upper;
    }
  }
  return out;
}

// DECIMAL columns: width/scale plus int64 lo/hi halves sign-extended from the
// internal physical type. `duckdb_mb_vector_decimal_kind` returns the width
// and scale packed as (width << 16) | scale for a single FFI round trip.
int32_t duckdb_mb_vector_decimal_meta(duckdb_vector vector) {
  if (!vector) {
    return 0;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return 0;
  }
  int32_t meta = ((int32_t)duckdb_decimal_width(ltype) << 16) |
                 (int32_t)duckdb_decimal_scale(ltype);
  duckdb_destroy_logical_type(&ltype);
  return meta;
}

// DECIMAL column -> lo halves sign-extended to 128 bits.
int64_t *duckdb_mb_vector_decimal_lo(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  duckdb_type internal =
      ltype ? duckdb_decimal_internal_type(ltype) : DUCKDB_TYPE_HUGEINT;
  for (int32_t row = 0; row < rows; row++) {
    switch (internal) {
    case DUCKDB_TYPE_SMALLINT:
      out[row] = (int64_t)(uint64_t)(int64_t)((int16_t *)data)[row];
      break;
    case DUCKDB_TYPE_INTEGER:
      out[row] = (int64_t)(uint64_t)(int64_t)((int32_t *)data)[row];
      break;
    case DUCKDB_TYPE_BIGINT:
      out[row] = (int64_t)(uint64_t)((int64_t *)data)[row];
      break;
    default:
      out[row] = (int64_t)((duckdb_hugeint *)data)[row].lower;
      break;
    }
  }
  if (ltype) {
    duckdb_destroy_logical_type(&ltype);
  }
  return out;
}

// DECIMAL column -> hi halves (sign extension for narrow internals).
int64_t *duckdb_mb_vector_decimal_hi(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  duckdb_type internal =
      ltype ? duckdb_decimal_internal_type(ltype) : DUCKDB_TYPE_HUGEINT;
  for (int32_t row = 0; row < rows; row++) {
    switch (internal) {
    case DUCKDB_TYPE_SMALLINT:
      out[row] = ((int64_t)((int16_t *)data)[row]) >> 63;
      break;
    case DUCKDB_TYPE_INTEGER:
      out[row] = ((int64_t)((int32_t *)data)[row]) >> 63;
      break;
    case DUCKDB_TYPE_BIGINT:
      out[row] = ((int64_t *)data)[row] >> 63;
      break;
    default:
      out[row] = ((duckdb_hugeint *)data)[row].upper;
      break;
    }
  }
  if (ltype) {
    duckdb_destroy_logical_type(&ltype);
  }
  return out;
}

// INTERVAL column -> flattened int64 triples [months, days, micros] per row.
int64_t *duckdb_mb_vector_interval(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows * 3);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    duckdb_interval iv = ((duckdb_interval *)data)[row];
    out[row * 3] = iv.months;
    out[row * 3 + 1] = iv.days;
    out[row * 3 + 2] = iv.micros;
  }
  return out;
}

// LIST column -> per-row offsets (uint64 bits) into the child vector.
int64_t *duckdb_mb_vector_list_offsets(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    out[row] = (int64_t)((duckdb_list_entry *)data)[row].offset;
  }
  return out;
}

// LIST column -> per-row entry counts (uint64 bits).
int64_t *duckdb_mb_vector_list_lengths(duckdb_vector vector, int32_t rows) {
  if (!vector || rows < 0) {
    return NULL;
  }
  void *data = duckdb_vector_get_data(vector);
  int64_t *out = moonbit_make_int64_array_raw(rows);
  if (!data || !out) {
    return NULL;
  }
  for (int32_t row = 0; row < rows; row++) {
    out[row] = (int64_t)((duckdb_list_entry *)data)[row].length;
  }
  return out;
}

// LIST column -> borrowed child vector and its element count.
// The child is only valid while the chunk is alive; materialize it before
// destroying the chunk.
duckdb_vector duckdb_mb_vector_list_child(duckdb_vector vector) {
  if (!vector) {
    return NULL;
  }
  return duckdb_list_vector_get_child(vector);
}

int64_t duckdb_mb_vector_list_child_size(duckdb_vector vector) {
  if (!vector) {
    return 0;
  }
  return (int64_t)duckdb_list_vector_get_size(vector);
}

// STRUCT column -> child count, borrowed child vector, and field name.
int32_t duckdb_mb_vector_struct_child_count(duckdb_vector vector) {
  if (!vector) {
    return 0;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return 0;
  }
  int32_t count = (int32_t)duckdb_struct_type_child_count(ltype);
  duckdb_destroy_logical_type(&ltype);
  return count;
}

duckdb_vector duckdb_mb_vector_struct_child(duckdb_vector vector,
                                            int32_t child) {
  if (!vector) {
    return NULL;
  }
  return duckdb_struct_vector_get_child(vector, (idx_t)child);
}

moonbit_bytes_t duckdb_mb_vector_struct_child_name(duckdb_vector vector,
                                                  int32_t child) {
  if (!vector) {
    return moonbit_make_bytes_raw(0);
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return moonbit_make_bytes_raw(0);
  }
  char *name = duckdb_struct_type_child_name(ltype, (idx_t)child);
  moonbit_bytes_t bytes = name ? duckdb_mb_make_bytes(name, strlen(name))
                               : moonbit_make_bytes_raw(0);
  if (name) {
    duckdb_free(name);
  }
  duckdb_destroy_logical_type(&ltype);
  return bytes;
}

// ARRAY column -> fixed child count per row and borrowed child vector.
int32_t duckdb_mb_vector_array_size(duckdb_vector vector) {
  if (!vector) {
    return 0;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return 0;
  }
  int32_t size = (int32_t)duckdb_array_type_array_size(ltype);
  duckdb_destroy_logical_type(&ltype);
  return size;
}

duckdb_vector duckdb_mb_vector_array_child(duckdb_vector vector) {
  if (!vector) {
    return NULL;
  }
  return duckdb_array_vector_get_child(vector);
}

// Logical-type child introspection for LIST/MAP/ARRAY/STRUCT columns, so the
// MoonBit decoder can type nested vectors. Returns DUCKDB_TYPE_INVALID when
// unavailable.
duckdb_type duckdb_mb_vector_list_child_type(duckdb_vector vector) {
  if (!vector) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type child = duckdb_list_type_child_type(ltype);
  duckdb_type id = child ? duckdb_get_type_id(child) : DUCKDB_TYPE_INVALID;
  if (child) {
    duckdb_destroy_logical_type(&child);
  }
  duckdb_destroy_logical_type(&ltype);
  return id;
}

duckdb_type duckdb_mb_vector_map_key_type(duckdb_vector vector) {
  if (!vector) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type child = duckdb_map_type_key_type(ltype);
  duckdb_type id = child ? duckdb_get_type_id(child) : DUCKDB_TYPE_INVALID;
  if (child) {
    duckdb_destroy_logical_type(&child);
  }
  duckdb_destroy_logical_type(&ltype);
  return id;
}

duckdb_type duckdb_mb_vector_map_value_type(duckdb_vector vector) {
  if (!vector) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type child = duckdb_map_type_value_type(ltype);
  duckdb_type id = child ? duckdb_get_type_id(child) : DUCKDB_TYPE_INVALID;
  if (child) {
    duckdb_destroy_logical_type(&child);
  }
  duckdb_destroy_logical_type(&ltype);
  return id;
}

duckdb_type duckdb_mb_vector_array_child_type(duckdb_vector vector) {
  if (!vector) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type child = duckdb_array_type_child_type(ltype);
  duckdb_type id = child ? duckdb_get_type_id(child) : DUCKDB_TYPE_INVALID;
  if (child) {
    duckdb_destroy_logical_type(&child);
  }
  duckdb_destroy_logical_type(&ltype);
  return id;
}

duckdb_type duckdb_mb_vector_struct_child_type(duckdb_vector vector,
                                               int32_t child) {
  if (!vector) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type ltype = duckdb_vector_get_column_type(vector);
  if (!ltype) {
    return DUCKDB_TYPE_INVALID;
  }
  duckdb_logical_type child_type =
      duckdb_struct_type_child_type(ltype, (idx_t)child);
  duckdb_type id =
      child_type ? duckdb_get_type_id(child_type) : DUCKDB_TYPE_INVALID;
  if (child_type) {
    duckdb_destroy_logical_type(&child_type);
  }
  duckdb_destroy_logical_type(&ltype);
  return id;
}

// Single cell rendered as a utf8 string via `duckdb_mb_cell_to_bytes`.
// Fallback path for types without a dedicated vector layout (UNION, BIT,
// TIME_TZ, BIGNUM, ...) — used to fill `VectorData::Any` cells.
moonbit_bytes_t duckdb_mb_vector_cell_string(duckdb_vector vector,
                                             duckdb_type type,
                                             int32_t row) {
  if (!vector || row < 0) {
    return moonbit_make_bytes_raw(0);
  }
  return duckdb_mb_cell_to_bytes(vector, type, (idx_t)row);
}

// Whole column rendered as per-cell utf8 strings, reusing
// `duckdb_mb_cell_to_bytes`. Used by the compat row API so the legacy string
// output flows through the canonical chunk path.
void **duckdb_mb_chunk_col_strings(duckdb_mb_chunk *chunk, int32_t col) {
  if (!chunk || !chunk->chunk || col < 0 || col >= chunk->column_count) {
    return NULL;
  }
  int32_t rows = (int32_t)duckdb_data_chunk_get_size(chunk->chunk);
  void **out = moonbit_make_ref_array_raw(rows);
  if (!out) {
    return NULL;
  }
  duckdb_vector vector =
      duckdb_data_chunk_get_vector(chunk->chunk, (idx_t)col);
  duckdb_type type =
      chunk->column_types ? chunk->column_types[col] : DUCKDB_TYPE_INVALID;
  uint64_t *validity = duckdb_vector_get_validity(vector);
  for (int32_t row = 0; row < rows; row++) {
    if (validity && !duckdb_validity_row_is_valid(validity, (idx_t)row)) {
      out[row] = moonbit_make_bytes_raw(0);
      continue;
    }
    out[row] = duckdb_mb_cell_to_bytes(vector, type, (idx_t)row);
  }
  return out;
}

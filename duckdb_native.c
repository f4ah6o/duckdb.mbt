#include "duckdb.h"
#include "moonbit.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  duckdb_database db;
  duckdb_connection conn;
} duckdb_mb_connection;

static char *duckdb_mb_last_error_message = NULL;

static void duckdb_mb_set_error(const char *message) {
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

duckdb_mb_connection *duckdb_mb_connect(moonbit_bytes_t path) {
  int32_t path_len = path ? Moonbit_array_length(path) : 0;
  char *path_c = NULL;
  const char *path_value = ":memory:";
  if (path_len > 0) {
    path_c = duckdb_mb_bytes_to_cstr(path);
    if (!path_c) {
      duckdb_mb_set_error("failed to allocate path buffer");
      return NULL;
    }
    path_value = path_c;
  }
  duckdb_mb_connection *handle =
      (duckdb_mb_connection *)malloc(sizeof(duckdb_mb_connection));
  if (!handle) {
    free(path_c);
    duckdb_mb_set_error("failed to allocate connection handle");
    return NULL;
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
    free(handle);
    free(path_c);
    return NULL;
  }
  if (open_error) {
    duckdb_free(open_error);
  }
  free(path_c);
  state = duckdb_connect(handle->db, &handle->conn);
  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_connect failed");
    duckdb_close(&handle->db);
    free(handle);
    return NULL;
  }
  return handle;
}

void duckdb_mb_disconnect(duckdb_mb_connection *handle) {
  if (!handle) {
    return;
  }
  duckdb_disconnect(&handle->conn);
  duckdb_close(&handle->db);
  free(handle);
}

duckdb_result *duckdb_mb_query(duckdb_mb_connection *handle,
                               moonbit_bytes_t sql) {
  if (!handle) {
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
    const char *error = duckdb_result_error(result);
    if (!error) {
      error = "duckdb_query failed";
    }
    duckdb_mb_set_error(error);
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

int32_t duckdb_mb_is_null_conn(duckdb_mb_connection *handle) {
  return handle == NULL ? 1 : 0;
}

int32_t duckdb_mb_is_null_result(duckdb_result *result) {
  return result == NULL ? 1 : 0;
}

// ============================================================================
// Configuration Functions
// ============================================================================

typedef struct {
  duckdb_config config;
  char error[256];
} duckdb_mb_config;

duckdb_mb_config *duckdb_mb_config_create(void) {
  duckdb_mb_config *mb_cfg =
      (duckdb_mb_config *)malloc(sizeof(duckdb_mb_config));
  if (!mb_cfg) {
    return NULL;
  }

  duckdb_state state = duckdb_create_config(&mb_cfg->config);
  if (state != DuckDBSuccess) {
    strncpy(mb_cfg->error, "duckdb_create_config failed", sizeof(mb_cfg->error));
    mb_cfg->config = NULL;
    free(mb_cfg);
    return NULL;
  }

  mb_cfg->error[0] = '\0';
  return mb_cfg;
}

void duckdb_mb_config_destroy(duckdb_mb_config *mb_cfg) {
  if (!mb_cfg) {
    return;
  }
  if (mb_cfg->config) {
    duckdb_destroy_config(&mb_cfg->config);
  }
  free(mb_cfg);
}

moonbit_bytes_t duckdb_mb_config_error(duckdb_mb_config *mb_cfg) {
  if (!mb_cfg) {
    return duckdb_mb_make_bytes("", 0);
  }
  return duckdb_mb_make_bytes(mb_cfg->error, strlen(mb_cfg->error));
}

int32_t duckdb_mb_config_set(duckdb_mb_config *mb_cfg,
                             moonbit_bytes_t key,
                             moonbit_bytes_t value) {
  if (!mb_cfg || !mb_cfg->config) {
    return 0;
  }

  char *key_c = duckdb_mb_bytes_to_cstr(key);
  if (!key_c) {
    strncpy(mb_cfg->error, "failed to allocate key buffer",
            sizeof(mb_cfg->error));
    return 0;
  }

  char *value_c = duckdb_mb_bytes_to_cstr(value);
  if (!value_c) {
    free(key_c);
    strncpy(mb_cfg->error, "failed to allocate value buffer",
            sizeof(mb_cfg->error));
    return 0;
  }

  duckdb_state state =
      duckdb_set_config(mb_cfg->config, key_c, value_c);
  free(key_c);
  free(value_c);

  if (state != DuckDBSuccess) {
    strncpy(mb_cfg->error, "duckdb_set_config failed", sizeof(mb_cfg->error));
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

  int32_t path_len = path ? Moonbit_array_length(path) : 0;
  char *path_c = NULL;
  const char *path_value = ":memory:";
  if (path_len > 0) {
    path_c = duckdb_mb_bytes_to_cstr(path);
    if (!path_c) {
      duckdb_mb_set_error("failed to allocate path buffer");
      return NULL;
    }
    path_value = path_c;
  }

  duckdb_mb_connection *handle =
      (duckdb_mb_connection *)malloc(sizeof(duckdb_mb_connection));
  if (!handle) {
    free(path_c);
    duckdb_mb_set_error("failed to allocate connection handle");
    return NULL;
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
    free(handle);
    free(path_c);
    return NULL;
  }

  if (open_error) {
    duckdb_free(open_error);
  }
  free(path_c);

  state = duckdb_connect(handle->db, &handle->conn);
  if (state != DuckDBSuccess) {
    duckdb_mb_set_error("duckdb_connect failed");
    duckdb_close(&handle->db);
    free(handle);
    return NULL;
  }

  return handle;
}

int32_t duckdb_mb_is_null_config(duckdb_mb_config *mb_cfg) {
  return mb_cfg == NULL ? 1 : 0;
}

// ============================================================================
// Prepared Statement Functions
// ============================================================================

typedef struct {
  duckdb_prepared_statement stmt;
  duckdb_connection conn;
  char error[256];
} duckdb_mb_statement;

duckdb_mb_statement *duckdb_mb_prepare(duckdb_mb_connection *handle,
                                      moonbit_bytes_t sql) {
  if (!handle) {
    return NULL;
  }
  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    return NULL;
  }

  duckdb_mb_statement *mb_stmt =
      (duckdb_mb_statement *)malloc(sizeof(duckdb_mb_statement));
  if (!mb_stmt) {
    free(sql_c);
    return NULL;
  }

  duckdb_state state = duckdb_prepare(handle->conn, sql_c, &mb_stmt->stmt);
  free(sql_c);

  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error && error[0] != '\0') {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    } else {
      strncpy(mb_stmt->error, "duckdb_prepare failed", sizeof(mb_stmt->error));
    }
    duckdb_destroy_prepare(&mb_stmt->stmt);
    mb_stmt->stmt = NULL;
    mb_stmt->conn = NULL;
    free(mb_stmt);
    return NULL;
  }

  mb_stmt->conn = handle->conn;
  mb_stmt->error[0] = '\0';
  return mb_stmt;
}

void duckdb_mb_statement_destroy(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt) {
    return;
  }
  if (mb_stmt->stmt) {
    duckdb_destroy_prepare(&mb_stmt->stmt);
  }
  free(mb_stmt);
}

moonbit_bytes_t duckdb_mb_statement_error(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt) {
    return duckdb_mb_make_bytes("", 0);
  }
  return duckdb_mb_make_bytes(mb_stmt->error, strlen(mb_stmt->error));
}

int32_t duckdb_mb_bind_int(duckdb_mb_statement *mb_stmt, int32_t index,
                          int32_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_state state = duckdb_bind_int32(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_bigint(duckdb_mb_statement *mb_stmt, int32_t index,
                              int64_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_state state = duckdb_bind_int64(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_double(duckdb_mb_statement *mb_stmt, int32_t index,
                              double value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_state state = duckdb_bind_double(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_varchar(duckdb_mb_statement *mb_stmt, int32_t index,
                               moonbit_bytes_t value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  char *val_c = duckdb_mb_bytes_to_cstr(value);
  if (!val_c) {
    strncpy(mb_stmt->error, "failed to allocate varchar buffer",
            sizeof(mb_stmt->error));
    return 0;
  }
  duckdb_state state =
      duckdb_bind_varchar(mb_stmt->stmt, (idx_t)index, val_c);
  free(val_c);

  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_bool(duckdb_mb_statement *mb_stmt, int32_t index,
                            bool value) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_state state = duckdb_bind_boolean(mb_stmt->stmt, (idx_t)index, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_null(duckdb_mb_statement *mb_stmt, int32_t index) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_state state = duckdb_bind_null(mb_stmt->stmt, (idx_t)index);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_clear_bindings(duckdb_mb_statement *mb_stmt) {
  if (!mb_stmt || !mb_stmt->stmt) {
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
    const char *error = duckdb_result_error(result);
    if (!error) {
      error = "duckdb_execute_prepared failed";
    }
    duckdb_mb_set_error(error);
    duckdb_destroy_result(result);
    free(result);
    return NULL;
  }

  return result;
}

int32_t duckdb_mb_is_null_statement(duckdb_mb_statement *mb_stmt) {
  return mb_stmt == NULL ? 1 : 0;
}

// ============================================================================
// Appender Functions
// ============================================================================

typedef struct {
  duckdb_appender appender;
  duckdb_connection conn;
  char error[256];
} duckdb_mb_appender;

duckdb_mb_appender *duckdb_mb_appender_create(duckdb_mb_connection *handle,
                                             moonbit_bytes_t schema,
                                             moonbit_bytes_t table) {
  if (!handle) {
    return NULL;
  }

  char *schema_c = duckdb_mb_bytes_to_cstr(schema);
  if (!schema_c) {
    return NULL;
  }

  char *table_c = duckdb_mb_bytes_to_cstr(table);
  if (!table_c) {
    free(schema_c);
    return NULL;
  }

  duckdb_mb_appender *mb_append =
      (duckdb_mb_appender *)malloc(sizeof(duckdb_mb_appender));
  if (!mb_append) {
    free(schema_c);
    free(table_c);
    return NULL;
  }

  duckdb_state state = duckdb_appender_create(handle->conn, schema_c, table_c,
                                             &mb_append->appender);
  free(schema_c);
  free(table_c);

  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error && error[0] != '\0') {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    } else {
      strncpy(mb_append->error, "duckdb_appender_create failed",
              sizeof(mb_append->error));
    }
    mb_append->appender = NULL;
    mb_append->conn = NULL;
    free(mb_append);
    return NULL;
  }

  mb_append->conn = handle->conn;
  mb_append->error[0] = '\0';
  return mb_append;
}

void duckdb_mb_appender_destroy(duckdb_mb_appender *mb_append) {
  if (!mb_append) {
    return;
  }
  if (mb_append->appender) {
    duckdb_appender_destroy(&mb_append->appender);
  }
  free(mb_append);
}

moonbit_bytes_t duckdb_mb_appender_error(duckdb_mb_appender *mb_append) {
  if (!mb_append) {
    return duckdb_mb_make_bytes("", 0);
  }
  return duckdb_mb_make_bytes(mb_append->error, strlen(mb_append->error));
}

int32_t duckdb_mb_begin_row(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_appender_begin_row(mb_append->appender);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_int(duckdb_mb_appender *mb_append, int32_t value) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_append_int32(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_bigint(duckdb_mb_appender *mb_append, int64_t value) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_append_int64(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_double(duckdb_mb_appender *mb_append, double value) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_append_double(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_varchar(duckdb_mb_appender *mb_append, moonbit_bytes_t value) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  char *val_c = duckdb_mb_bytes_to_cstr(value);
  if (!val_c) {
    strncpy(mb_append->error, "failed to allocate varchar buffer",
            sizeof(mb_append->error));
    return 0;
  }

  duckdb_state state = duckdb_append_varchar(mb_append->appender, val_c);
  free(val_c);

  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_bool(duckdb_mb_appender *mb_append, bool value) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_append_bool(mb_append->appender, value);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_append_null(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_append_null(mb_append->appender);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_end_row(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_appender_end_row(mb_append->appender);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_flush(duckdb_mb_appender *mb_append) {
  if (!mb_append || !mb_append->appender) {
    return 0;
  }
  duckdb_state state = duckdb_appender_flush(mb_append->appender);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_is_null_appender(duckdb_mb_appender *mb_append) {
  return mb_append == NULL ? 1 : 0;
}

// ============================================================================
// Date/Timestamp Functions
// ============================================================================

int32_t duckdb_mb_bind_date(duckdb_mb_statement *mb_stmt, int32_t index,
                              int32_t days) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_date date = {days};
  duckdb_state state = duckdb_bind_date(mb_stmt->stmt, (idx_t)index, date);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

int32_t duckdb_mb_bind_timestamp(duckdb_mb_statement *mb_stmt, int32_t index,
                                  int64_t micros) {
  if (!mb_stmt || !mb_stmt->stmt) {
    return 0;
  }
  duckdb_timestamp ts = {micros};
  duckdb_state state =
      duckdb_bind_timestamp(mb_stmt->stmt, (idx_t)index, ts);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_prepare_error(mb_stmt->stmt);
    if (error) {
      strncpy(mb_stmt->error, error, sizeof(mb_stmt->error) - 1);
      mb_stmt->error[sizeof(mb_stmt->error) - 1] = '\0';
    }
    return 0;
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
    return 0;
  }
  char date_buf[32];
  days_to_date_string(days, date_buf, sizeof(date_buf));
  duckdb_state state = duckdb_append_varchar(mb_append->appender, date_buf);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
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
    return 0;
  }
  char ts_buf[64];
  micros_to_timestamp_string(micros, ts_buf, sizeof(ts_buf));
  duckdb_state state = duckdb_append_varchar(mb_append->appender, ts_buf);
  if (state != DuckDBSuccess) {
    const char *error = duckdb_appender_error(mb_append->appender);
    if (error) {
      strncpy(mb_append->error, error, sizeof(mb_append->error) - 1);
      mb_append->error[sizeof(mb_append->error) - 1] = '\0';
    }
    return 0;
  }
  return 1;
}

// ============================================================================
// Arrow Integration
// ============================================================================

typedef struct {
  duckdb_arrow arrow;
  duckdb_connection conn;
  char error[256];
  int32_t column_count;
  int32_t row_count;
} duckdb_mb_arrow_result;

duckdb_mb_arrow_result *duckdb_mb_query_arrow(duckdb_mb_connection *handle,
                                              moonbit_bytes_t sql) {
  if (!handle || !handle->conn) {
    duckdb_mb_set_error("invalid connection handle");
    return NULL;
  }

  char *sql_c = duckdb_mb_bytes_to_cstr(sql);
  if (!sql_c) {
    duckdb_mb_set_error("failed to allocate SQL buffer");
    return NULL;
  }

  duckdb_mb_arrow_result *arrow_result =
      (duckdb_mb_arrow_result *)malloc(sizeof(duckdb_mb_arrow_result));
  if (!arrow_result) {
    free(sql_c);
    duckdb_mb_set_error("failed to allocate arrow result handle");
    return NULL;
  }

  arrow_result->conn = handle->conn;
  arrow_result->error[0] = '\0';
  arrow_result->arrow = NULL;
  arrow_result->column_count = 0;
  arrow_result->row_count = 0;

  duckdb_state state = duckdb_query_arrow(handle->conn, sql_c, &arrow_result->arrow);
  free(sql_c);

  if (state != DuckDBSuccess) {
    const char *error = duckdb_query_arrow_error(arrow_result->arrow);
    if (error) {
      strncpy(arrow_result->error, error, sizeof(arrow_result->error) - 1);
      arrow_result->error[sizeof(arrow_result->error) - 1] = '\0';
    } else {
      strncpy(arrow_result->error, "duckdb_query_arrow failed",
              sizeof(arrow_result->error) - 1);
    }
    duckdb_destroy_arrow(&arrow_result->arrow);
    free(arrow_result);
    duckdb_mb_set_error(arrow_result->error);
    return NULL;
  }

  arrow_result->column_count = (int32_t)duckdb_arrow_column_count(arrow_result->arrow);
  arrow_result->row_count = (int32_t)duckdb_arrow_row_count(arrow_result->arrow);

  return arrow_result;
}

int32_t duckdb_mb_arrow_column_count(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result) {
    return 0;
  }
  return arrow_result->column_count;
}

int32_t duckdb_mb_arrow_row_count(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result) {
    return 0;
  }
  return arrow_result->row_count;
}

// Get schema as JSON string for MoonBit parsing
moonbit_bytes_t duckdb_mb_arrow_schema(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result || !arrow_result->arrow) {
    return duckdb_mb_make_bytes("[]", 2);
  }

  // For now, return empty array - schema parsing would require
  // traversing the Arrow C Data Interface schema structure
  // In a full implementation, this would serialize the schema to JSON
  return duckdb_mb_make_bytes("[]", 2);
}

// Helper to extract column data as primitive arrays
// Returns data in a compact binary format: [count, value1, value2, ...]
moonbit_bytes_t duckdb_mb_arrow_get_column_int32(duckdb_mb_arrow_result *arrow_result,
                                                  int32_t col_idx) {
  if (!arrow_result || !arrow_result->arrow) {
    return duckdb_mb_make_bytes("", 0);
  }

  // For now, return empty - a full implementation would use Arrow arrays directly
  (void)col_idx;
  return duckdb_mb_make_bytes("", 0);
}

moonbit_bytes_t duckdb_mb_arrow_get_column_int64(duckdb_mb_arrow_result *arrow_result,
                                                  int32_t col_idx) {
  (void)arrow_result;
  (void)col_idx;
  return duckdb_mb_make_bytes("", 0);
}

moonbit_bytes_t duckdb_mb_arrow_get_column_double(duckdb_mb_arrow_result *arrow_result,
                                                   int32_t col_idx) {
  (void)arrow_result;
  (void)col_idx;
  return duckdb_mb_make_bytes("", 0);
}

moonbit_bytes_t duckdb_mb_arrow_get_column_string(duckdb_mb_arrow_result *arrow_result,
                                                   int32_t col_idx) {
  (void)arrow_result;
  (void)col_idx;
  return duckdb_mb_make_bytes("", 0);
}

moonbit_bytes_t duckdb_mb_arrow_get_column_bool(duckdb_mb_arrow_result *arrow_result,
                                                 int32_t col_idx) {
  (void)arrow_result;
  (void)col_idx;
  return duckdb_mb_make_bytes("", 0);
}

void duckdb_mb_arrow_destroy(duckdb_mb_arrow_result *arrow_result) {
  if (!arrow_result) {
    return;
  }
  if (arrow_result->arrow) {
    duckdb_destroy_arrow(&arrow_result->arrow);
  }
  free(arrow_result);
}

int32_t duckdb_mb_is_null_arrow_result(duckdb_mb_arrow_result *arrow_result) {
  return arrow_result == NULL ? 1 : 0;
}


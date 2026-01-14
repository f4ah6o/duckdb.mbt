# TODO: duckdb-node-neo SQL extraction

Goal: review duckdb-node-neo tests and extract every SQL query that can be reused
for MoonBit fixture coverage.

- Enumerate files under `https://github.com/duckdb/duckdb-node-neo/tree/main/bindings/test`.
- For each test, list SQL strings and note the API surface used.
- Keep only SQL that fits the current MoonBit API:
  - `connect` / `query` / `close` returning columns + rows + nulls.
  - Avoid prepared statements, appenders, data chunks, and vector APIs.
- Group SQL by expected value shape (single row, multi row, aggregates, errors).
- Extend `scripts/generate_duckdb_fixtures.js` to include these SQL cases or
  add a small parser to extract them directly from the test sources.
- Add notes for cases that differ between node-api JSON output and
  native `duckdb_value_varchar` formatting (dates, structs, arrays).

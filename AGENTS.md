# Project Agents.md Guide

This is a [MoonBit](https://docs.moonbitlang.com) project.

## Project Structure

- MoonBit packages are organized per directory, for each directory, there is a
  `moon.pkg.json` file listing its dependencies. Each package has its files and
  blackbox test files (common, ending in `_test.mbt`) and whitebox test files
  (ending in `_wbtest.mbt`).

- In the toplevel directory, this is a `moon.mod.json` file listing about the
  module and some meta information.

## Coding convention

- MoonBit code is organized in block style, each block is separated by `///|`,
  the order of each block is irrelevant. In some refactorings, you can process
  block by block independently.

- Try to keep deprecated blocks in file called `deprecated.mbt` in each
  directory.

## DuckDB version-specific code

The native shim supports libduckdb 1.5.6 and 2.0.0 from one source file.

- Detect 2.0 with `__has_include("duckdb_v2.h")` (`DUCKDB_MB_HAS_DUCKDB2`).
  Do not use `DUCKDB_API_VERSION_AT_LEAST`: the classic API version stays
  1.5.6 in the 2.0 headers.
- Guard every 2.0-only enum constant or function with
  `#ifdef DUCKDB_MB_HAS_DUCKDB2`; both supported headers must compile without
  warnings.
- Do not read the payload of NULL values. DuckDB leaves it unspecified, and
  2.0 fills it with garbage; check `duckdb_vector_get_validity` first.
- Tests for behavior that differs by engine version branch on
  `SELECT version()` (`v2.` prefix) instead of adding a version API. The
  `native_has_duckdb2()` whitebox helper reports the headers a native test
  was compiled against.
- `scripts/install_libduckdb.sh <version> <prefix>` selects the release asset
  layout (`libduckdb-*.zip` before 2.0, `duckdb-shared-libs-*.tar.gz` after)
  and installs every C header. `scripts/check_duckdb_pins.mjs` keeps the
  `ci.yml` native matrix and the README Native row identical.

## Tooling

- `moon fmt` is used to format your code properly.

- `moon info` is used to update the generated interface of the package, each
  package has a generated interface file `.mbti`, it is a brief formal
  description of the package. If nothing in `.mbti` changes, this means your
  change does not bring the visible changes to the external package users, it is
  typically a safe refactoring.

- In the last step, run `moon info && moon fmt` to update the interface and
  format the code. Check the diffs of `.mbti` file to see if the changes are
  expected.

- Run `moon test` to check the test is passed. MoonBit supports snapshot
  testing, so when your changes indeed change the behavior of the code, you
  should run `moon test --update` to update the snapshot.

- You can run `moon check` to check the code is linted correctly.

- When writing tests, you are encouraged to use `inspect` and run
  `moon test --update` to update the snapshots, only use assertions like
  `assert_eq` when you are in some loops where each snapshot may vary. You can
  use `moon coverage analyze > uncovered.log` to see which parts of your code
  are not covered by tests.

- agent-todo.md has some small tasks that are easy for AI to pick up, agent is
  welcome to finish the tasks and check the box when you are done

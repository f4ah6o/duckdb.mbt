#!/usr/bin/env bash
# Regenerate the "Feature Support Matrix" section of README.mbt.md from the
# runtime BackendCapabilities table (src/duckdb_capabilities.mbt), rendered by
# src/cmd/support_matrix. CI runs this and diffs the README to catch drift.
set -euo pipefail
cd "$(dirname "$0")/.."

MATRIX="$(moon run src/cmd/support_matrix --target js)" \
  python3 - <<'PY'
import os
import pathlib
import sys

readme = pathlib.Path("README.mbt.md")
text = readme.read_text(encoding="utf-8")
begin = "<!-- support-matrix:begin -->"
end = "<!-- support-matrix:end -->"
matrix = os.environ["MATRIX"].rstrip("\n")
generated = (
    begin
    + "\n<!-- Generated from BackendCapabilities (src/duckdb_capabilities.mbt) "
    + "by scripts/support_matrix.sh -- do not edit by hand. -->\n"
    + matrix
    + "\n"
    + end
)
pre, sep, rest = text.partition(begin)
if not sep:
    sys.exit("README.mbt.md is missing the support-matrix:begin marker")
_, sep, post = rest.partition(end)
if not sep:
    sys.exit("README.mbt.md is missing the support-matrix:end marker")
readme.write_text(pre + generated + post, encoding="utf-8")
PY

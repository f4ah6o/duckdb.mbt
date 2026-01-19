#!/bin/bash
# Check PBT Coverage for DuckDB MoonBit bindings
# Analyzes which functions have PBT coverage

set -e

echo "=== PBT Coverage Check ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Count PBT tests
echo -e "${YELLOW}Counting PBT tests...${NC}"

# Count tests in PBT files
pbt_files=(
    "duckdb_pbt_test.mbt"
    "duckdb_blob_pbt_test.mbt"
    "duckdb_decimal_pbt_test.mbt"
    "duckdb_interval_pbt_test.mbt"
    "duckdb_collection_pbt_test.mbt"
    "duckdb_connection_state_machine.mbt"
    "duckdb_appender_state_machine.mbt"
)

total_tests=0
for file in "${pbt_files[@]}"; do
    if [ -f "$file" ]; then
        count=$(grep -c '^test "' "$file" 2>/dev/null || echo 0)
        echo "  $file: $count tests"
        total_tests=$((total_tests + count))
    fi
done

echo ""
echo -e "${GREEN}Total PBT tests: $total_tests${NC}"
echo ""

# Check coverage by type
echo -e "${YELLOW}Coverage by Type:${NC}"
echo "  Date/Timestamp: $(grep -c 'date\|timestamp' duckdb_pbt_test.mbt 2>/dev/null || echo 0) tests"
echo "  Blob: $(grep -c 'blob' duckdb_blob_pbt_test.mbt 2>/dev/null || echo 0) tests"
echo "  Decimal: $(grep -c 'decimal' duckdb_decimal_pbt_test.mbt 2>/dev/null || echo 0) tests"
echo "  Interval: $(grep -c 'interval' duckdb_interval_pbt_test.mbt 2>/dev/null || echo 0) tests"
echo "  Collections: $(grep -c 'list\|struct\|map' duckdb_collection_pbt_test.mbt 2>/dev/null || echo 0) tests"
echo "  Connection: $(grep -c 'connection' duckdb_connection_state_machine.mbt 2>/dev/null || echo 0) tests"
echo "  Appender: $(grep -c 'appender' duckdb_appender_state_machine.mbt 2>/dev/null || echo 0) tests"
echo ""

# Check for missing coverage
echo -e "${YELLOW}Functions needing PBT coverage:${NC}"
echo "  (This is a manual check - review TODO comments in source)"
grep -r "TODO.*pbt\|TODO.*PBT" . --include="*.mbt" 2>/dev/null || echo "  No TODO markers found"
echo ""

echo "=== Coverage Check Complete ==="

#!/bin/bash
# PBT Sync Script for DuckDB MoonBit
# Syncs PBT tests with Aletheia pattern detection

set -e

echo "=== PBT Sync Script ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Run Aletheia pattern detection
echo -e "${YELLOW}Step 1: Running Aletheia pattern detection...${NC}"
if moon run .mooncakes/f4ah6o/aletheia/src/aletheia -- analyze . --explain; then
    echo -e "${GREEN}Pattern detection completed${NC}"
else
    echo "Warning: Pattern detection failed or not available"
fi
echo ""

# Step 2: Generate PBT targets from detected patterns
echo -e "${YELLOW}Step 2: Generating PBT targets...${NC}"
if moon run .mooncakes/f4ah6o/aletheia/src/aletheia -- generate . --format markdown; then
    echo -e "${GREEN}PBT targets generated${NC}"
else
    echo "Warning: PBT generation failed or not available"
fi
echo ""

# Step 3: Sync to test files
echo -e "${YELLOW}Step 3: Syncing to test files...${NC}"
if moon run .mooncakes/f4ah6o/aletheia/src/aletheia -- sync; then
    echo -e "${GREEN}Sync completed${NC}"
else
    echo "Warning: Sync failed or not available"
fi
echo ""

# Step 4: Format and type check
echo -e "${YELLOW}Step 4: Formatting and type checking...${NC}"
moon fmt
moon check
echo ""

# Step 5: Run PBT tests
echo -e "${YELLOW}Step 5: Running PBT tests...${NC}"
moon test --target native --filter "*pbt*"
echo ""

echo -e "${GREEN}=== PBT Sync Complete ===${NC}"

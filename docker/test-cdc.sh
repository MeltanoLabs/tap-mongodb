#!/bin/bash
# Test script for CDC (Change Data Capture) functionality

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "MongoDB CDC Replication Test"
echo "============================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Start MongoDB replica set
echo -e "${BLUE}Step 1: Starting MongoDB replica set...${NC}"
cd "$PROJECT_DIR"
docker-compose up -d
echo ""

# Wait for replica set to be ready
echo -e "${YELLOW}Waiting for replica set to initialize (30 seconds)...${NC}"
sleep 30
echo ""

# Step 2: Seed initial data
echo -e "${BLUE}Step 2: Seeding initial data...${NC}"
uv run "$SCRIPT_DIR/seed-cdc-test-data.py" \
  --connection-string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
  --database test
echo ""

# Step 3: Run tap to get initial state (discovery)
echo -e "${BLUE}Step 3: Running tap discovery...${NC}"
tap-mongodb --config "$SCRIPT_DIR/config-cdc.json" --discover > "$SCRIPT_DIR/catalog-discovered.json"
echo -e "${GREEN}✓ Discovery complete. Catalog saved to docker/catalog-discovered.json${NC}"
echo ""

# Step 4: Run initial sync with LOG_BASED replication
echo -e "${BLUE}Step 4: Running initial CDC sync (LOG_BASED)...${NC}"
echo "This will capture the current state and establish change stream position..."
tap-mongodb \
  --config "$SCRIPT_DIR/config-cdc.json" \
  --catalog "$SCRIPT_DIR/catalog-cdc.json" \
  --state "$SCRIPT_DIR/state.json" \
  > "$SCRIPT_DIR/output-initial.jsonl" 2>&1 || true

echo -e "${GREEN}✓ Initial sync complete${NC}"
echo "Output saved to: docker/output-initial.jsonl"
echo "State saved to: docker/state.json"
echo ""

# Show summary of initial records
if [ -f "$SCRIPT_DIR/output-initial.jsonl" ]; then
    echo "Summary of initial sync:"
    echo "  SCHEMA messages: $(grep -c '"type":"SCHEMA"' "$SCRIPT_DIR/output-initial.jsonl" || echo "0")"
    echo "  RECORD messages: $(grep -c '"type":"RECORD"' "$SCRIPT_DIR/output-initial.jsonl" || echo "0")"
    echo "  STATE messages: $(grep -c '"type":"STATE"' "$SCRIPT_DIR/output-initial.jsonl" || echo "0")"
fi
echo ""

# Step 5: Simulate CDC events
echo -e "${BLUE}Step 5: Simulating CDC events (inserts, updates, deletes)...${NC}"
uv run "$SCRIPT_DIR/seed-cdc-test-data.py" \
  --connection-string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
  --database test \
  --simulate-changes
echo ""

# Step 6: Run tap again to capture changes
echo -e "${BLUE}Step 6: Running incremental CDC sync to capture changes...${NC}"
tap-mongodb \
  --config "$SCRIPT_DIR/config-cdc.json" \
  --catalog "$SCRIPT_DIR/catalog-cdc.json" \
  --state "$SCRIPT_DIR/state.json" \
  > "$SCRIPT_DIR/output-incremental.jsonl" 2>&1 || true

echo -e "${GREEN}✓ Incremental sync complete${NC}"
echo "Output saved to: docker/output-incremental.jsonl"
echo ""

# Show summary of changes captured
if [ -f "$SCRIPT_DIR/output-incremental.jsonl" ]; then
    echo "Summary of incremental sync (CDC events captured):"
    echo "  SCHEMA messages: $(grep -c '"type":"SCHEMA"' "$SCRIPT_DIR/output-incremental.jsonl" || echo "0")"
    echo "  RECORD messages: $(grep -c '"type":"RECORD"' "$SCRIPT_DIR/output-incremental.jsonl" || echo "0")"
    echo "  STATE messages: $(grep -c '"type":"STATE"' "$SCRIPT_DIR/output-incremental.jsonl" || echo "0")"
    echo ""

    if grep -q '"operation_type"' "$SCRIPT_DIR/output-incremental.jsonl"; then
        echo "Operation types captured:"
        grep '"operation_type"' "$SCRIPT_DIR/output-incremental.jsonl" | sed 's/.*"operation_type":"\([^"]*\)".*/  - \1/' | sort | uniq -c
    fi
fi
echo ""

# Step 7: Show sample records
echo -e "${BLUE}Step 7: Sample CDC records${NC}"
echo ""
echo "Sample INSERT event:"
grep '"operation_type":"insert"' "$SCRIPT_DIR/output-incremental.jsonl" | head -n 1 | jq '.' || echo "No insert events found"
echo ""
echo "Sample UPDATE event:"
grep '"operation_type":"update"' "$SCRIPT_DIR/output-incremental.jsonl" | head -n 1 | jq '.' || echo "No update events found"
echo ""
echo "Sample DELETE event:"
grep '"operation_type":"delete"' "$SCRIPT_DIR/output-incremental.jsonl" | head -n 1 | jq '.' || echo "No delete events found"
echo ""

echo "============================================"
echo -e "${GREEN}✓ CDC Test Complete!${NC}"
echo "============================================"
echo ""
echo "Files generated:"
echo "  - docker/catalog-discovered.json (discovered schema)"
echo "  - docker/output-initial.jsonl (initial sync output)"
echo "  - docker/output-incremental.jsonl (CDC events)"
echo "  - docker/state.json (replication state with resume tokens)"
echo ""
echo "To clean up:"
echo "  docker-compose down -v"
echo ""

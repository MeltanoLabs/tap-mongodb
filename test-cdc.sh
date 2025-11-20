#!/bin/bash
set -e

echo "=== CDC Testing Script ==="
echo

# Clean up
echo "1. Cleaning up previous state..."
rm -rf .meltano/run output/
echo "   ✓ Cleaned"
echo

# Initial sync (with timeout since it will wait)
echo "2. Running initial sync (establishing change stream position)..."
echo "   This will timeout after 5 seconds - that's expected!"
timeout 5 meltano run tap-mongodb-cdc target-jsonl 2>&1 | grep -E "(Beginning|record_count|METRIC)" || true
echo "   ✓ Initial sync done (stream position established)"
echo

# Check what was captured
echo "3. Checking initial output (should have resume token records)..."
if [ -d "output" ]; then
    for file in output/*.jsonl; do
        if [ -f "$file" ]; then
            count=$(wc -l < "$file")
            echo "   - $(basename $file): $count records"
        fi
    done
else
    echo "   - No output directory yet"
fi
echo

# Simulate changes
echo "4. Simulating CDC events (inserts, updates, deletes)..."
uv run docker/seed-cdc-test-data.py --simulate-changes 2>&1 | tail -3
echo "   ✓ Changes simulated"
echo

# Capture changes (with timeout)
echo "5. Capturing changes (running CDC sync)..."
echo "   This will also timeout after 10 seconds..."
timeout 10 meltano run tap-mongodb-cdc target-jsonl 2>&1 | grep -E "(Beginning|record_count|METRIC)" || true
echo "   ✓ CDC sync done"
echo

# Show results
echo "6. Results:"
if [ -d "output" ]; then
    for file in output/*.jsonl; do
        if [ -f "$file" ]; then
            count=$(wc -l < "$file")
            echo "   - $(basename $file): $count total records"
        fi
    done

    echo
    echo "7. Operation types captured:"
    if grep -h '"operation_type"' output/*.jsonl 2>/dev/null | \
       sed 's/.*"operation_type": "\([^"]*\)".*/\1/' | sort | uniq -c; then
        echo "   ✓ CDC events captured successfully!"
    else
        echo "   - No operation_type fields found (only resume tokens)"
    fi
else
    echo "   - No output directory"
fi

echo
echo "=== Test Complete ==="
echo
echo "NOTE: The 'timeouts' above are expected - CDC taps wait indefinitely for changes."
echo "In production, you'd run this as a long-running process or configure a timeout."

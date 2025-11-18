# CDC (Change Data Capture) Testing Guide

This repository includes a complete Docker-based setup for testing log-based (CDC) replication with MongoDB Change Streams.

## ✅ LOG_BASED Mode is Working!

The tap's **LOG_BASED mode has been fixed** and now properly captures change stream events including:
- **Insert** operations - New documents created
- **Update** operations - Documents modified
- **Delete** operations - Documents removed
- **Replace** operations - Documents replaced entirely

Change streams capture the complete document state along with operation metadata, cluster timestamps, and namespace information.

## Quick Start with Meltano

1. **Start the MongoDB replica set:**
   ```bash
   docker compose up -d
   ```

2. **Wait for initialization (30-45 seconds):**
   ```bash
   # Check status
   docker compose ps
   docker exec mongodb-primary mongosh --eval "rs.status()" --quiet
   ```

3. **Seed test data:**
   ```bash
   uv run docker/seed-cdc-test-data.py
   ```

4. **Configure Meltano for CDC:**
   ```bash
   # Set connection to local MongoDB replica set
   meltano config tap-mongodb set mongodb_connection_string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
   meltano config tap-mongodb set database test
   ```

5. **Edit `meltano.yml` to enable LOG_BASED replication:**

   Add this to the `metadata` section under `tap-mongodb`:
   ```yaml
   plugins:
     extractors:
       - name: tap-mongodb
         # ... other config ...
         metadata:
           'users':
             replication-method: LOG_BASED
           'posts':
             replication-method: LOG_BASED
   ```

6. **Run initial sync:**
   ```bash
   meltano run tap-mongodb target-jsonl
   ```

   This captures the initial state and establishes the change stream position.

7. **Simulate CDC events:**
   ```bash
   uv run docker/seed-cdc-test-data.py --simulate-changes
   ```

8. **Capture changes with incremental sync:**
   ```bash
   meltano run tap-mongodb target-jsonl
   ```

9. **View captured CDC events:**
   ```bash
   # Check the output file
   grep '"operation_type"' output/*.jsonl | \
     sed 's/.*"operation_type":"\([^"]*\)".*/\1/' | sort | uniq -c
   ```

## What's Included

```
compose.yml                    # 3-node MongoDB replica set (root)
docker/
├── README.md                  # Detailed documentation
├── IMPORTANT.md              # Notes about catalog complexity
├── config-cdc.json           # tap-mongodb CDC configuration
├── seed-cdc-test-data.py     # Data seeding and CDC simulation
└── init-replica-set.sh       # Replica set initialization
```

## Key Features

- **3-node MongoDB replica set** (required for Change Streams)
- **Automated initialization** of replica set
- **Test data generator** with realistic fake data
- **CDC event simulator** (inserts, updates, deletes, replaces)
- **Pre-configured catalogs** for LOG_BASED replication
- **Complete documentation** with troubleshooting guide

## Architecture

The setup creates:
- **Primary node** (mongodb-primary:27017) - accepts writes
- **Secondary node 1** (mongodb-secondary1:27018) - replication
- **Secondary node 2** (mongodb-secondary2:27019) - replication

All nodes are accessible from localhost for testing.

## Connection String

```
mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
```

The `directConnection=true` parameter is important when connecting from outside Docker to avoid hostname resolution issues.

## Cleanup

```bash
# Stop and remove containers
docker compose down -v

# Remove generated files
rm -f docker/state.json docker/output-*.jsonl docker/catalog-discovered.json
```

## Full Documentation

See [docker/README.md](docker/README.md) for:
- Detailed step-by-step instructions
- Configuration options
- Output format reference
- Resume token behavior
- Troubleshooting guide
- Advanced testing scenarios

## Requirements

- Docker and Docker Compose
- Python 3.10+
- Meltano installed (`pip install meltano`)
- This tap-mongodb project (with `meltano install` run)

## Testing Different Scenarios

### Full End-to-End CDC Test
```bash
# 1. Start MongoDB
docker compose up -d && sleep 30

# 2. Seed initial data
uv run docker/seed-cdc-test-data.py

# 3. Configure Meltano
meltano config tap-mongodb set mongodb_connection_string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
meltano config tap-mongodb set database test

# 4. Initial sync (edit meltano.yml first to set LOG_BASED replication)
meltano run tap-mongodb target-jsonl

# 5. Simulate CDC events
uv run docker/seed-cdc-test-data.py --simulate-changes

# 6. Capture changes
meltano run tap-mongodb target-jsonl

# 7. View operation types
grep '"operation_type"' output/*.jsonl | sed 's/.*"operation_type":"\([^"]*\)".*/\1/' | sort | uniq -c
```

### Test Only Data Seeding
```bash
uv run docker/seed-cdc-test-data.py
```

### Test CDC Event Simulation
```bash
uv run docker/seed-cdc-test-data.py --simulate-changes
```

### Manually Inspect Change Streams
```bash
# Connect to MongoDB
docker exec -it mongodb-primary mongosh test

# Open a change stream
db.users.watch()

# In another terminal, make changes
uv run docker/seed-cdc-test-data.py --simulate-changes
```

## Understanding CDC Output

CDC records will have `operation_type` set to one of:
- `insert` - New documents created
- `update` - Documents modified
- `replace` - Documents replaced entirely
- `delete` - Documents deleted

The `document` field contains the full document state (or documentKey for deletes).

## Advanced Topics

### Testing Without Meltano

While possible, using the tap directly requires careful catalog configuration. See `docker/IMPORTANT.md` for details on why Meltano is recommended.

### Resume Token Behavior

MongoDB change streams use resume tokens to track position. The tap stores these in state, allowing it to resume from the exact point even after crashes.

### Testing with Different MongoDB Versions

Edit `compose.yml` to change the image version:
```yaml
image: mongo:8.0  # or mongo:6.0, mongo:5.0
```

## Troubleshooting

### "Change streams not available"
- Ensure you're connecting to the replica set with `?replicaSet=rs0`
- Use `directConnection=true` when connecting from outside Docker

### "Resume point no longer in oplog"
- The oplog window was exceeded
- Delete the state file to start fresh: `rm -rf .meltano/`

For more details, see the comprehensive documentation in `docker/README.md`.

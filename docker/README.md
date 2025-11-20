# Docker CDC Testing Setup

This directory provides a Docker-based MongoDB replica set for testing CDC (Change Data Capture) functionality.

**For complete CDC documentation, see the main [README.md](../README.md#change-data-capture-log_based-mode).**

## Quick Start

```bash
# 1. Start MongoDB replica set
docker compose up -d

# 2. Wait for initialization (30-45 seconds)
docker compose ps

# 3. Seed test data
uv run docker/seed-cdc-test-data.py

# 4. Run CDC sync with Meltano
meltano run tap-mongodb-cdc target-jsonl

# 5. Simulate CDC events
uv run docker/seed-cdc-test-data.py --simulate-changes

# 6. Capture changes
meltano run tap-mongodb-cdc target-jsonl
```

## Files

- `config-cdc.json` - tap-mongodb configuration for CDC
- `seed-cdc-test-data.py` - Data seeding and CDC event simulation
- `init-replica-set.sh` - Replica set initialization script

## Architecture

The Docker setup creates a 3-node MongoDB replica set:
- **mongodb-primary:27017** - Primary node (accepts writes)
- **mongodb-secondary1:27018** - Secondary node (replication)
- **mongodb-secondary2:27019** - Secondary node (replication)

## Connection String

```
mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
```

## Seed Script Usage

```bash
# Seed initial data (users and posts)
uv run docker/seed-cdc-test-data.py

# Simulate CDC events (inserts, updates, deletes, replaces)
uv run docker/seed-cdc-test-data.py --simulate-changes

# Custom connection and database
uv run docker/seed-cdc-test-data.py \
  --connection-string "mongodb://localhost:27017/?replicaSet=rs0" \
  --database mydb
```

## Cleanup

```bash
# Stop and remove containers
docker compose down -v

# Remove generated files
rm -rf output/ .meltano/run/
```

## Full Documentation

See the main [README.md](../README.md#change-data-capture-log_based-mode) for:
- Complete CDC setup guide
- Output format and operation types
- Troubleshooting
- Resume token behavior
- Production deployment guidance

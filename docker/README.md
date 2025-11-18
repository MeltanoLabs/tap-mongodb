# CDC (Change Data Capture) Testing Setup for tap-mongodb

This directory contains Docker Compose configuration and scripts for testing log-based (CDC) replication with tap-mongodb using MongoDB Change Streams.

## Overview

MongoDB Change Streams require a **replica set** to function. This setup creates a 3-node MongoDB replica set that allows you to test CDC functionality locally without needing a cloud MongoDB Atlas cluster or manually setting up replica set configuration.

## Files

```
docker/
├── README.md                  # This file
├── config-cdc.json           # tap-mongodb configuration for CDC
├── catalog-cdc.json          # Pre-configured catalog with LOG_BASED replication
├── seed-cdc-test-data.py     # Script to seed test data and simulate CDC events
├── init-replica-set.sh       # Script to initialize MongoDB replica set
└── test-cdc.sh               # End-to-end CDC test script
```

## Prerequisites

- Docker and Docker Compose installed
- Python 3.10+ with `pymongo` and `faker` libraries
- tap-mongodb installed (`pip install tap-mongodb` or installed in development mode)

## Quick Start

### Option 1: Automated End-to-End Test

Run the complete CDC test workflow:

```bash
./docker/test-cdc.sh
```

This script will:
1. Start the MongoDB replica set
2. Seed initial test data
3. Run tap discovery
4. Perform initial LOG_BASED sync
5. Simulate CDC events (inserts, updates, deletes)
6. Capture changes via incremental sync
7. Display summary of captured events

### Option 2: Manual Step-by-Step Testing

#### 1. Start MongoDB Replica Set

```bash
docker compose up -d
```

Wait for the replica set to initialize (about 30-45 seconds). Check status:

```bash
docker compose logs mongodb-init
docker compose ps
```

#### 2. Verify Replica Set

Connect to MongoDB and check replica set status:

```bash
docker exec -it mongodb-primary mongosh --eval "rs.status()"
```

You should see one PRIMARY and two SECONDARY nodes.

#### 3. Seed Initial Data

```bash
uv run docker/seed-cdc-test-data.py \
  --connection-string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
  --database test
```

This creates:
- 50 users in the `users` collection
- 200 posts in the `posts` collection

#### 4. Discover Available Streams

```bash
tap-mongodb --config docker/config-cdc.json --discover > docker/catalog-discovered.json
```

Or use the pre-configured catalog with LOG_BASED replication:

```bash
cat docker/catalog-cdc.json
```

#### 5. Run Initial CDC Sync

```bash
tap-mongodb \
  --config docker/config-cdc.json \
  --catalog docker/catalog-cdc.json \
  --state docker/state.json \
  > docker/output-initial.jsonl
```

This establishes the initial change stream position and captures the current state.

#### 6. Simulate CDC Events

```bash
uv run docker/seed-cdc-test-data.py \
  --connection-string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
  --database test \
  --simulate-changes
```

This performs:
- 5 new user inserts
- 10 user updates
- 1 user replacement
- 1 user deletion
- 10 new post inserts
- 15 post updates
- 5 post deletions

Total: ~47 change events

#### 7. Capture Changes with Incremental Sync

```bash
tap-mongodb \
  --config docker/config-cdc.json \
  --catalog docker/catalog-cdc.json \
  --state docker/state.json \
  > docker/output-incremental.jsonl
```

#### 8. Analyze Captured Events

```bash
# Count events by type
grep '"type":"RECORD"' docker/output-incremental.jsonl | wc -l

# See operation types
grep '"operation_type"' docker/output-incremental.jsonl | \
  sed 's/.*"operation_type":"\([^"]*\)".*/\1/' | sort | uniq -c

# View a sample insert event
grep '"operation_type":"insert"' docker/output-incremental.jsonl | head -n 1 | jq '.'

# View a sample update event
grep '"operation_type":"update"' docker/output-incremental.jsonl | head -n 1 | jq '.'

# View a sample delete event
grep '"operation_type":"delete"' docker/output-incremental.jsonl | head -n 1 | jq '.'
```

## Configuration Details

### Connection String

**For tap-mongodb (must support replica set discovery):**
```
mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
```

**For manual testing with mongosh:**
```
mongosh "mongodb://localhost:27017/?directConnection=true"
```

**Notes**:
- The `replicaSet=rs0` parameter is **required** for change streams to work with tap-mongodb
- The `directConnection=true` parameter bypasses replica set discovery when connecting from outside Docker
- When connecting from within Docker network, use hostnames: `mongodb://mongodb-primary:27017/?replicaSet=rs0`

### Replica Set Architecture

```
┌─────────────────────┐
│   mongodb-primary   │ :27017  (Priority: 2)
│      (PRIMARY)      │
└─────────────────────┘
          │
          ├─────────────────────┬─────────────────────┐
          │                     │                     │
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│ mongodb-secondary1  │  │ mongodb-secondary2  │  │   mongodb-init      │
│    (SECONDARY)      │  │    (SECONDARY)      │  │  (init container)   │
│       :27018        │  │       :27019        │  │    (exits after     │
└─────────────────────┘  └─────────────────────┘  │   initialization)   │
                                                   └─────────────────────┘
```

### tap-mongodb Configuration

Key configuration parameters for CDC (`docker/config-cdc.json`):

```json
{
  "mongodb_connection_string": "mongodb://localhost:27017/?replicaSet=rs0",
  "database": "test",
  "operation_types": ["insert", "update", "replace", "delete"],
  "add_record_metadata": true
}
```

### Catalog Configuration

To enable LOG_BASED replication on a stream, set:

```json
{
  "tap_stream_id": "test-users",
  "metadata": [
    {
      "breadcrumb": [],
      "metadata": {
        "selected": true,
        "replication-method": "LOG_BASED"
      }
    }
  ]
}
```

## Output Format

CDC records contain these fields:

| Field | Description |
|-------|-------------|
| `operation_type` | Type of change: `insert`, `update`, `replace`, `delete` |
| `document` | Full document after the change (or documentKey for deletes) |
| `cluster_time` | MongoDB cluster timestamp (ISO-8601) |
| `replication_key` | Resume token for the change stream |
| `namespace` | Database and collection info |
| `_sdc_extracted_at` | Extraction timestamp (if `add_record_metadata: true`) |
| `_sdc_batched_at` | Batch timestamp (if `add_record_metadata: true`) |
| `_sdc_deleted_at` | Deletion timestamp (if operation_type is `delete`) |

## State/Bookmark Management

tap-mongodb stores the change stream **resume token** in the state file:

```json
{
  "bookmarks": {
    "test-users": {
      "replication_key": "_data",
      "replication_key_value": "82674B9C3E000000012B042C0100296E5A1004..."
    }
  }
}
```

The resume token allows the tap to resume from the exact position in the change stream, ensuring no events are missed even if the tap crashes or is restarted.

## Resume Token Behavior

### MongoDB 4.2+
Uses `start_after` parameter - graceful handling if resume token expires (falls back to beginning if token is outside oplog window).

### MongoDB < 4.2
Uses `resume_after` parameter - requires resume token to be within oplog window (error 286 if exceeded).

## Troubleshooting

### Error: "Change stream is not available for collections"

**Cause**: Not connected to a replica set

**Solution**: Ensure connection string includes `?replicaSet=rs0`

### Error: "resume point may no longer be in the oplog"

**Cause**: Resume token is too old (oplog window exceeded)

**Solution**: Delete the state file to restart from the beginning:

```bash
rm docker/state.json
```

### No events captured in incremental sync

**Cause**: No changes occurred between syncs, or state wasn't saved

**Solution**:
1. Ensure state file was written after initial sync
2. Make some changes to the database
3. Run incremental sync again

### Replica set not electing primary

**Cause**: Nodes can't communicate or not enough time elapsed

**Solution**:
1. Wait 30-60 seconds for election
2. Check logs: `docker compose logs mongodb-primary`
3. Verify network: `docker compose ps`

## Cleanup

Stop and remove all containers and volumes:

```bash
docker compose down -v
```

Remove generated files:

```bash
rm -f docker/state.json docker/output-*.jsonl docker/catalog-discovered.json
```

## Architecture Notes

### Why Replica Sets are Required

MongoDB Change Streams are built on top of the **oplog** (operations log), which only exists in replica set deployments. Even a single-node "replica set" will work, but you need the `--replSet` flag.

### Change Stream vs Incremental Replication

| Feature | LOG_BASED (CDC) | INCREMENTAL (ObjectId) |
|---------|----------------|------------------------|
| Captures deletes | ✅ Yes | ❌ No |
| Captures updates | ✅ Yes | ⚠️ Only as new inserts |
| Requires replica set | ✅ Yes | ❌ No |
| Performance on large collections | ✅ Excellent | ⚠️ Slower (table scans) |
| Real-time streaming | ✅ Yes | ❌ No (polling) |
| Oplog dependency | ✅ Yes | ❌ No |

## Advanced Testing

### Test with Different MongoDB Versions

Edit `docker-compose.yml` to change the image:

```yaml
image: mongo:8.0  # or mongo:6.0, mongo:5.0
```

### Test Oplog Window Expiry

1. Set a small oplog size in docker-compose.yml:
   ```yaml
   command: ["--replSet", "rs0", "--bind_ip_all", "--oplogSize", "50"]
   ```

2. Generate many changes
3. Wait for the oplog to cycle
4. Try to resume - should get error 286

### Test with AWS DocumentDB

AWS DocumentDB requires change streams to be explicitly enabled per collection:

```json
{
  "mongodb_connection_string": "mongodb://username:password@docdb-cluster.region.docdb.amazonaws.com:27017/?replicaSet=rs0&tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem",
  "database": "test",
  "allow_modify_change_streams": true,
  "operation_types": ["insert", "update", "replace", "delete"]
}
```

The `allow_modify_change_streams` setting allows the tap to automatically enable change streams (note: this incurs AWS costs).

## References

- [MongoDB Change Streams Documentation](https://www.mongodb.com/docs/manual/changeStreams/)
- [PyMongo Change Streams](https://pymongo.readthedocs.io/en/stable/api/pymongo/change_stream.html)
- [tap-mongodb GitHub](https://github.com/MeltanoLabs/tap-mongodb)
- [Singer Spec](https://hub.meltano.com/singer/spec)

## Support

For issues specific to this test setup:
1. Check Docker logs: `docker compose logs`
2. Verify Python dependencies: `pip list | grep -E "(pymongo|faker)"`
3. Test MongoDB connectivity: `docker exec -it mongodb-primary mongosh`

For tap-mongodb issues:
- GitHub Issues: https://github.com/MeltanoLabs/tap-mongodb/issues
- Meltano Slack: https://meltano.com/slack

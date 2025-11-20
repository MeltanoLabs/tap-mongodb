# tap-mongodb

[![Test tap-mongodb](https://github.com/MeltanoLabs/tap-mongodb/actions/workflows/validate.yml/badge.svg)](https://github.com/MeltanoLabs/tap-mongodb/actions/workflows/validate.yml)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/format.json)](https://github.com/astral-sh/ruff)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

`tap-mongodb` is a Singer tap for extracting data from a [MongoDB](https://www.mongodb.com/)
or [AWS DocumentDB](https://aws.amazon.com/documentdb/) database.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Replication Modes

The tap supports two replication modes:

### 1. **INCREMENTAL** (Default)
- Extracts records directly from collections
- Uses `_id` field as replication key
- Suitable for regular MongoDB instances
- No special infrastructure required

### 2. **LOG_BASED** (CDC - Change Data Capture)
- Captures change events via MongoDB Change Streams
- Tracks insert, update, replace, and delete operations
- Requires MongoDB replica set or sharded cluster
- Includes operation metadata, cluster timestamps, and full document state
- Automatically exits after 10 seconds of inactivity (production-ready)

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/MeltanoLabs/tap-mongodb.git@main

# or

uv tool install git+https://github.com/MeltanoLabs/tap-mongodb.git@main
```

## Configuration

### Accepted Config Options

| Setting                                  | Type         | Required |               Default               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:-----------------------------------------|------------- |:--------:|:-----------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| database                                 | string       |   True   |                  -                  | Database from which records will be extracted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| mongodb_connection_string                | password     |  False   |                  -                  | MongoDB connection string. See [the MongoDB documentation](https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-uri-format) for specification. The username and password included in this string must be url-encoded - the tap will not url-encode it.                                                                                                                                                                                                                                                                                                         |
| documentdb_credential_json_string        | password     |  False   |                  -                  | JSON string with keys 'username', 'password', 'engine', 'host', 'port', 'dbClusterIdentifier' or 'dbName', 'ssl'. See example and strucure [in the AWS documentation here](https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_secret_json_structure.html#reference_secret_json_structure_docdb). The password from this JSON object will be url-encoded by the tap before opening the database connection. The intent of this setting is to enable management of an AWS DocumentDB database credential via AWS SecretsManager                                             |
| documentdb_credential_json_extra_options | string       |  False   |                  -                  | JSON string containing key-value pairs which will be added to the connection string options when using documentdb_credential_json_string. For example, when set to the string `{"tls":"true","tlsCAFile":"my-ca-bundle.pem"}`, the options `tls=true&tlsCAFile=my-ca-bundle.pem` will be passed to the MongoClient.                                                                                                                                                                                                                                                                          |
| datetime_conversion                      | string       |  False   |              datetime               | Parameter passed to MongoClient 'datetime_conversion' parameter. See documentation at https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes for details. The default value is 'datetime', which will throw a bson.errors.InvalidBson error if a document contains a date outside the range of datetime.MINYEAR (year 1) to datetime.MAXYEAR (9999).                                                                                                                                                                                               |
| prefix                                   | string       |  False   |                 ''                  | An optional prefix which will be added to the name of each stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| filter_collections                       | string[]     |  False   |                 []                  | Collections to discover (default: all) - filtering is case-insensitive. Useful for improving catalog discovery performance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| start_date                               | date_iso8601 |  False   |             1970-01-01              | Start date - used for incremental replication only. In log-based replication mode, this setting is ignored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| add_record_metadata                      | boolean      |  False   |                False                | When true, _sdc metadata fields will be added to records produced by the tap.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| allow_modify_change_streams              | boolean      |  False   |                False                | In AWS DocumentDB (unlike MongoDB), change streams must be enabled specifically (see the [documentation here](https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html#change_streams-enabling) ). If attempting to open a change stream against a collection on which change streams have not been enabled, an OperationFailure error will be raised. If this property is set to True, when this error is seen, the tap will execute an admin command to enable change streams and then retry the read operation. Note: this may incur new costs in AWS DocumentDB. |
| operation_types                          | list(string) |  False   | create,delete,insert,replace,update | List of MongoDB change stream operation types to include in tap output. The default behavior is to limit to document-level operation types. See full list of operation types in [the MongoDB documentation](https://www.mongodb.com/docs/manual/reference/change-events/#operation-types). Note that the list of allowed_values for this property includes some values not available to all MongoDB versions.                                                                                                                                                                                |                                                                                                                                                                               |

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Configuration

#### MongoDB

#### AWS DocumentDB

### Source Authentication and Authorization

#### Incremental replication mode

In order to run `tap-mongodb` in incremental replication mode, the credential used must have read privileges to the
collections from which you wish to extract records. If your credential has the `readAnyDatabase@admin` permission, for
example, or `read@test_database` (where `test_database` is the `database` setting in the tap's configuration), that
should be sufficient.

Collection-level read permissions are untested but are expected to work as well:

```javascript
privileges: [
    {resource: {db: "test_database", collection: "TestOrders"}, actions: ["find"]}
]
```

The above collection-level read permission should allow the tap to extract from the `test_database.TestOrders`
collection in incremental replication mode.

#### Log-based replication

In order to run `tap-mongodb` in log-based replication mode, which extracts records via the database's Change Streams
API, MongoDB and AWS DocumentDB have different requirements around permissions.

In MongoDB, the credential must have both `find` and `changeStreams` permissions on a database collection in order to
use `tap-mongodb` in log-based replication mode. The `readAnyDatabase@admin` built-in role provides this for all
databases, while `read@test_database` will provide the necessary access for all collections in the `test_database`
database.

## Usage

You can easily run `tap-mongodb` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-mongodb --version
tap-mongodb --help
tap-mongodb --config CONFIG --discover > ./catalog.json
```

## Change Data Capture (LOG_BASED Mode)

The tap supports real-time change data capture using MongoDB Change Streams. This mode captures insert, update, replace, and delete operations as they occur.

### Requirements

- **MongoDB**: Replica set (3+ nodes) or sharded cluster
- **AWS DocumentDB**: Cluster mode with change streams enabled
- **Permissions**: `find` and `changeStream` permissions on collections

### Quick Start with Meltano

This repository includes a complete Docker-based testing setup with a 3-node MongoDB replica set.

#### 1. Start MongoDB Replica Set

```bash
# Start 3-node replica set
docker compose up -d

# Wait for initialization (30-45 seconds)
docker compose ps
docker exec mongodb-primary mongosh --eval "rs.status()" --quiet
```

#### 2. Configure CDC Plugin

The `meltano.yml` includes a pre-configured CDC plugin that inherits from the base tap:

```yaml
- name: tap-mongodb-cdc
  inherit_from: tap-mongodb
  config:
    database: test
    mongodb_connection_string: mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
  metadata:
    '*':
      replication-method: LOG_BASED
      replication-key: replication_key
```

#### 3. Run CDC Sync

```bash
# Install the CDC plugin (first time only)
meltano install extractor tap-mongodb-cdc

# Run CDC sync
meltano run tap-mongodb-cdc target-jsonl
```

The tap will:
1. Open a change stream for each collection
2. Yield a resume token to track position
3. Process any existing change events
4. Automatically exit after 10 seconds of inactivity

### CDC Output Format

Each CDC record includes:

```json
{
  "operation_type": "insert",
  "cluster_time": "2025-11-19T20:28:33+00:00",
  "namespace": {
    "database": "test",
    "collection": "users"
  },
  "document": {
    "_id": "...",
    "name": "John Doe",
    "email": "john@example.com"
  },
  "replication_key": "82691E3EFE000000012B0429296E1404"
}
```

**Operation Types:**
- `insert` - New documents created (includes full document)
- `update` - Documents modified (includes full document via updateLookup)
- `replace` - Documents replaced entirely (includes full document)
- `delete` - Documents removed (includes only _id)

### Testing CDC Locally

#### Seed Test Data

```bash
# Generate initial test data (users and posts)
uv run docker/seed-cdc-test-data.py

# Run initial sync to establish change stream position
meltano run --full-refresh tap-mongodb-cdc target-jsonl

# Simulate CDC events (inserts, updates, deletes, replaces)
uv run docker/seed-cdc-test-data.py --simulate-changes

# Capture the changes
meltano run tap-mongodb-cdc target-jsonl

# View captured events by operation type
grep '"operation_type"' output/*.jsonl | \
  sed 's/.*"operation_type": "\([^"]*\)".*/\1/' | sort | uniq -c
```

#### Full End-to-End Test

```bash
# Complete test workflow
docker compose up -d && sleep 30
uv run docker/seed-cdc-test-data.py
meltano run tap-mongodb-cdc target-jsonl
uv run docker/seed-cdc-test-data.py --simulate-changes
meltano run tap-mongodb-cdc target-jsonl
```

### Resume Tokens and State

MongoDB change streams use resume tokens to track position in the oplog. The tap:
- Stores resume tokens in state after each sync
- Can resume from the exact position after crashes or restarts
- Handles oplog window expiration gracefully (MongoDB 4.2+)

### Connection String Format

**MongoDB:**
```
mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
```

**DocumentDB:**
```
mongodb://username:password@cluster.region.docdb.amazonaws.com:27017/?replicaSet=rs0&tls=true&tlsCAFile=rds-combined-ca-bundle.pem
```

Key parameters:
- `replicaSet=rs0` - Required for change streams
- `directConnection=true` - Recommended when connecting from outside Docker (MongoDB)
- `tls=true&tlsCAFile=...` - Required for DocumentDB

### AWS DocumentDB Configuration

DocumentDB requires change streams to be enabled at the collection level:

```bash
# Enable change streams (requires admin privileges)
db.adminCommand({
  modifyChangeStreams: 1,
  database: "test",
  collection: "users",
  enable: true
})
```

Or set `allow_modify_change_streams: true` in tap config to auto-enable.

### Idle Timeout Behavior

The tap automatically exits after 10 seconds of inactivity:
- **No manual intervention needed** - No Ctrl+C required
- **Production-ready** - Safe for automated pipelines
- **Resource-efficient** - Doesn't hang indefinitely
- **Preserves resume tokens** - Can resume from exact position

### Switching Between Modes

**From INCREMENTAL to LOG_BASED:**
1. Ensure MongoDB is running as replica set
2. Update connection string to include `?replicaSet=rs0`
3. Clear existing state: `rm -rf .meltano/extractors/tap-mongodb/`
4. Use `tap-mongodb-cdc` plugin or update metadata to `LOG_BASED`

**From LOG_BASED to INCREMENTAL:**
1. Update connection string (can remove replica set parameters)
2. Clear existing state
3. Use base `tap-mongodb` plugin or update metadata to `INCREMENTAL`

### Troubleshooting

**"Change streams not available"**
- Ensure you're connecting to a replica set with `?replicaSet=rs0`
- For DocumentDB, verify change streams are enabled on collections

**"Resume point no longer in oplog"**
- The oplog window was exceeded (too much time between syncs)
- Delete state file to start fresh: `rm -rf .meltano/`
- Consider running syncs more frequently

**No events captured**
- Ensure changes occurred AFTER establishing the change stream position
- Change streams only capture events after the stream opens
- Run an initial sync first to establish position, then make changes

**Cleanup Docker Environment**
```bash
# Stop and remove containers
docker compose down -v

# Remove generated files
rm -rf output/ .meltano/run/
```

### Additional Resources

- [MongoDB Change Streams Documentation](https://www.mongodb.com/docs/manual/changeStreams/)
- [DocumentDB Change Streams Guide](https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html)
- Testing helpers: `docker/seed-cdc-test-data.py`
- Docker setup: `compose.yml` (3-node replica set)

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tap_mongodb/tests` subfolder and then run:

```bash
uv run pytest
```

You can also test the `tap-mongodb` CLI interface directly using `uv run`:

```bash
uv run tap-mongodb --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mongodb --version

# OR run a test EL pipeline:
meltano run tap-mongodb target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

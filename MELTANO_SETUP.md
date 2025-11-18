# Meltano Configuration Guide

This repository uses Meltano's plugin inheritance feature to provide both INCREMENTAL and LOG_BASED (CDC) replication modes in a single `meltano.yml` file.

## Plugin Structure

The `meltano.yml` defines two extractors:

### 1. `tap-mongodb` (Base Plugin)
- **Replication Method**: INCREMENTAL
- **Replication Key**: `_id`
- **Use Case**: Standard incremental sync using document IDs

### 2. `tap-mongodb-cdc` (Inherited Plugin)
- **Inherits From**: `tap-mongodb`
- **Replication Method**: LOG_BASED
- **Replication Key**: `replication_key`
- **Use Case**: Change Data Capture using MongoDB Change Streams
- **Requires**: MongoDB replica set

## Configuration

### INCREMENTAL Mode (tap-mongodb)

```bash
# Already configured in meltano.yml
meltano run tap-mongodb target-jsonl
```

Configuration in `meltano.yml`:
```yaml
- name: tap-mongodb
  config:
    database: test_database
    mongodb_connection_string: mongodb://admin:password@localhost:27017/
  metadata:
    '*':
      replication-method: INCREMENTAL
      replication-key: _id
```

### LOG_BASED Mode (tap-mongodb-cdc)

```bash
# Install the CDC plugin
meltano install extractor tap-mongodb-cdc

# Run CDC sync
meltano run tap-mongodb-cdc target-jsonl
```

Configuration in `meltano.yml`:
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

## Benefits of Plugin Inheritance

1. **No Duplication**: The CDC plugin inherits all settings, capabilities, and configuration from the base plugin
2. **DRY Principle**: Settings schema is defined once, reused twice
3. **Easy Testing**: Switch between modes by changing which plugin you run
4. **Maintainability**: Updates to settings or capabilities only need to be made in one place

## Standalone Plugin Definition

For use in other Meltano projects, this repository includes `tap-mongodb.yml` which can be referenced via `meltano add --from-ref`:

```bash
meltano add extractor tap-mongodb \
  --from-ref https://raw.githubusercontent.com/MeltanoLabs/tap-mongodb/main/tap-mongodb.yml
```

See [PLUGIN_DEFINITION.md](./PLUGIN_DEFINITION.md) for details.

## Testing CDC

To test CDC functionality, see the [CDC Testing Guide](./CDC_TESTING.md) which covers:
- MongoDB replica set setup with Docker Compose
- Test data generation
- CDC event simulation
- End-to-end testing workflow

## Switching Replication Methods

### From INCREMENTAL to LOG_BASED

If you want to switch from INCREMENTAL to LOG_BASED:

1. Ensure MongoDB is running as a replica set
2. Update connection string to include `?replicaSet=rs0&directConnection=true`
3. Clear existing state: `rm -rf .meltano/extractors/tap-mongodb/`
4. Update metadata to use LOG_BASED mode

Or simply use the pre-configured `tap-mongodb-cdc` plugin.

### From LOG_BASED to INCREMENTAL

If switching back from CDC to incremental:

1. Update connection string (can remove replica set parameters)
2. Clear existing state
3. Update metadata to use INCREMENTAL mode with `_id` as replication key

Or simply use the base `tap-mongodb` plugin.

## Common Commands

```bash
# List all extractors
meltano list extractors

# View tap-mongodb configuration
meltano config tap-mongodb list

# View tap-mongodb-cdc configuration
meltano invoke tap-mongodb-cdc --help

# Test tap-mongodb connection
meltano invoke tap-mongodb --discover

# Test tap-mongodb-cdc connection
meltano invoke tap-mongodb-cdc --discover

# Run full sync with tap-mongodb
meltano run tap-mongodb target-jsonl

# Run CDC sync with tap-mongodb-cdc
meltano run tap-mongodb-cdc target-jsonl
```

## See Also

- [CDC Testing Guide](./CDC_TESTING.md) - Complete CDC testing setup
- [Plugin Definition](./PLUGIN_DEFINITION.md) - Standalone plugin definition usage
- [Docker README](./docker/README.md) - MongoDB replica set setup details
- [Meltano Docs](https://docs.meltano.com/) - Official Meltano documentation

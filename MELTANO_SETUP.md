# Meltano Configuration

**Note**: All Meltano and CDC configuration documentation has been consolidated into the main [README.md](README.md).

Please see the README for:
- [Replication Modes](README.md#replication-modes) (INCREMENTAL vs LOG_BASED)
- [Change Data Capture (LOG_BASED Mode)](README.md#change-data-capture-log_based-mode)
- [Configuration Options](README.md#accepted-config-options)

## Quick Reference

### INCREMENTAL Mode (Default)

```bash
# Standard incremental sync
meltano run tap-mongodb target-jsonl
```

Uses `_id` as replication key. Suitable for regular MongoDB instances.

### LOG_BASED Mode (CDC)

```bash
# Install CDC plugin (first time)
meltano install extractor tap-mongodb-cdc

# Run CDC sync
meltano run tap-mongodb-cdc target-jsonl
```

Requires MongoDB replica set. See [CDC documentation](README.md#change-data-capture-log_based-mode) for full details.

## Plugin Inheritance

The `meltano.yml` uses Meltano's plugin inheritance feature:

```yaml
- name: tap-mongodb-cdc
  inherit_from: tap-mongodb
  config:
    mongodb_connection_string: mongodb://localhost:27017/?replicaSet=rs0&directConnection=true
  metadata:
    '*':
      replication-method: LOG_BASED
      replication-key: replication_key
```

This allows both modes to share configuration while having different replication methods.

## See Also

- [README.md](README.md) - Complete documentation
- [Change Data Capture](README.md#change-data-capture-log_based-mode)
- [Configuration Options](README.md#accepted-config-options)

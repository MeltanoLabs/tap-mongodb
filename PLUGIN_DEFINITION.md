# tap-mongodb Plugin Definition

This repository includes a standalone plugin definition file (`tap-mongodb.yml`) that can be used to add tap-mongodb to any Meltano project using `meltano add --from-ref`.

**For complete CDC documentation, see the main [README.md](README.md#change-data-capture-log_based-mode).**

## Usage

### Adding tap-mongodb from URL

You can add the tap from a URL reference to this plugin definition file:

```bash
meltano add extractor tap-mongodb \
  --from-ref https://raw.githubusercontent.com/MeltanoLabs/tap-mongodb/main/tap-mongodb.yml
```

Then configure it for your use case:

**For INCREMENTAL replication:**
```bash
meltano config tap-mongodb set database your_database
meltano config tap-mongodb set mongodb_connection_string mongodb://localhost:27017/
meltano select tap-mongodb '*.*'
meltano config tap-mongodb set --metadata '*' replication-method INCREMENTAL
meltano config tap-mongodb set --metadata '*' replication-key _id
```

**For LOG_BASED replication (CDC):**
```bash
meltano config tap-mongodb set database your_database
meltano config tap-mongodb set mongodb_connection_string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
meltano select tap-mongodb '*.*'
meltano config tap-mongodb set --metadata '*' replication-method LOG_BASED
meltano config tap-mongodb set --metadata '*' replication-key replication_key
```

### Adding tap-mongodb from local file

If you're developing locally or testing changes:

```bash
meltano add extractor tap-mongodb \
  --from-ref /path/to/tap-mongodb/tap-mongodb.yml
```

## Direct meltano.yml Configuration

Alternatively, you can directly add the plugin definition to your `meltano.yml`. See the examples in this repository:

### INCREMENTAL Replication Example

See: `meltano.yml` (root)

```yaml
plugins:
  extractors:
    - name: tap-mongodb
      namespace: tap_mongodb
      pip_url: -e .
      capabilities:
        - state
        - catalog
        - discover
        - about
        - stream-maps
        - structured-logging
      config:
        database: test_database
        mongodb_connection_string: mongodb://admin:password@localhost:27017/
      select:
        - '*.*'
      metadata:
        '*':
          replication-method: INCREMENTAL
          replication-key: _id
```

### LOG_BASED Replication Example (CDC)

The main `meltano.yml` includes an inherited plugin variant for CDC testing:

```yaml
plugins:
  extractors:
    # Base plugin definition
    - name: tap-mongodb
      namespace: tap_mongodb
      pip_url: -e .
      capabilities: [state, catalog, discover, about, stream-maps, structured-logging]
      settings: [... all settings ...]
      select: ['*.*']
      metadata:
        '*':
          replication-method: INCREMENTAL
          replication-key: _id

    # CDC variant using LOG_BASED replication with Change Streams
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

This approach uses Meltano's `inherit_from` to create a CDC-specific variant without duplicating the full plugin definition.

**Note**: LOG_BASED replication requires a MongoDB replica set.

## Plugin Definition Contents

The `tap-mongodb.yml` file includes:

- **Full settings schema** with all configuration options
- **Settings validation** for required parameter groups
- **Default capabilities** (state, catalog, discover, about, stream-maps, structured-logging)
- **Operation types** default configuration for CDC
- **Documentation** for all settings

## Benefits

1. **Standardized Definition**: Single source of truth for plugin schema and settings
2. **Easy Distribution**: Share plugin definition via URL or file path
3. **Version Control**: Track changes to plugin configuration over time
4. **Portability**: Use the same definition across teams and projects
5. **Hub Alternative**: Use `--from-ref` for custom/unreleased versions instead of waiting for Meltano Hub

## See Also

- [Meltano Plugin Definition Documentation](https://docs.meltano.com/concepts/plugins#plugin-definitions)
- [CDC Testing Guide](./CDC_TESTING.md)
- [Main README](./README.md)

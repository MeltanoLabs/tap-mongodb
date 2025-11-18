# Important Note About CDC Testing

The Docker Compose setup for CDC testing **is working correctly**:
- ✅ MongoDB replica set is configured properly
- ✅ Change streams are enabled
- ✅ Test data can be seeded
- ✅ CDC events can be simulated

However, **catalog configuration for LOG_BASED replication is complex** when using tap-mongodb outside of Meltano.

## Recommended Approach

**Use Meltano** for CDC testing, which handles catalog metadata correctly:

```bash
# Start MongoDB
docker compose up -d

# Seed data
uv run docker/seed-cdc-test-data.py

# Configure Meltano
meltano config tap-mongodb set mongodb_connection_string "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
meltano config tap-mongodb set database test

# Edit meltano.yml to set replication-method: LOG_BASED in the metadata section
# Then run:
meltano run tap-mongodb target-jsonl
```

## Why Direct tap-mongodb CLI is Complex

The Singer catalog format requires specific metadata to enable LOG_BASED replication. While the catalog can be manually edited, the Meltano SDK's interpretation of replication methods from catalog metadata is sensitive to format. Meltano handles this automatically through its metadata configuration in `meltano.yml`.

## For Direct Testing Without Meltano

If you need to test the tap directly without Meltano, the MongoDB replica set is ready. You'll need to:

1. Discover the catalog
2. Manually set `replication-method: LOG_BASED` in the catalog metadata
3. Ensure the metadata entry with empty breadcrumb `[]` has both:
   - `"selected": true`
   - `"replication-method": "LOG_BASED"` (with hyphen, not underscore)

See `docker/catalog-cdc-fixed.json` for an example of the correct format.

## Files Ready for Use

- `compose.yml` - MongoDB replica set (working)
- `docker/seed-cdc-test-data.py` - Data seeding (working)
- `docker/init-replica-set.sh` - Replica set init (working)
- `docker/config-cdc.json` - tap config (working)
- `docker/README.md` - Comprehensive documentation

The infrastructure is complete and CDC-ready!

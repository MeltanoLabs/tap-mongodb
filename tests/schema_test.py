"""Tests for the schema advertised in discovery."""

import pytest

from tap_mongodb.connector import MongoDBConnector


def _connector(*, add_record_metadata: bool) -> MongoDBConnector:
    return MongoDBConnector(
        "mongodb://localhost:27017",
        {},
        "db",
        "datetime",
        add_record_metadata=add_record_metadata,
    )


@pytest.mark.parametrize("add_record_metadata", [False, True])
def test_sdc_properties_only_when_metadata_enabled(add_record_metadata: bool):
    """`_sdc_*` properties should only be advertised if they will actually be emitted."""
    entry = _connector(add_record_metadata=add_record_metadata).discover_catalog_entry("things")
    sdc_properties = {name for name in entry.schema.properties if name.startswith("_sdc_")}

    if add_record_metadata:
        assert sdc_properties == {"_sdc_batched_at", "_sdc_extracted_at"}
    else:
        assert not sdc_properties

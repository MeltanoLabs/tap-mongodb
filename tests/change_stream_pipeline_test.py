"""Tests for the change_stream_pipeline config setting."""

from unittest.mock import MagicMock

from singer_sdk.streams.core import REPLICATION_LOG_BASED

from tap_mongodb.streams import MongoDBCollectionStream


def _make_stream(config: dict) -> tuple[MongoDBCollectionStream, MagicMock]:
    """Build a MongoDBCollectionStream with a mocked tap, connector, and collection."""
    tap = MagicMock()
    tap.config = {
        "operation_types": ["create", "delete", "insert", "replace", "update"],
        **config,
    }
    tap.state = {}
    tap.initialized_at = 0
    tap.name = "tap-mongodb"

    connector = MagicMock()
    connector.version = (6, 0)
    collection = MagicMock()
    connector.database.__getitem__.return_value = collection

    catalog_entry = {
        "table_name": "my_collection",
        "tap_stream_id": "my_collection",
        "schema": {"type": "object", "properties": {}},
    }

    stream = MongoDBCollectionStream(tap=tap, catalog_entry=catalog_entry, connector=connector)
    stream.forced_replication_method = REPLICATION_LOG_BASED

    # A dead change stream so get_records() returns immediately after opening it.
    change_stream = MagicMock()
    change_stream.__enter__.return_value = change_stream
    change_stream.__exit__.return_value = False
    change_stream.alive = False
    collection.watch.return_value = change_stream

    return stream, collection


def test_change_stream_pipeline_is_forwarded_to_watch():
    """When change_stream_pipeline is configured, it is passed as the `pipeline` kwarg to collection.watch()."""
    pipeline = [{"$unset": "updateDescription"}]
    stream, collection = _make_stream({"change_stream_pipeline": pipeline})

    list(stream.get_records(context=None))

    collection.watch.assert_called_once_with(full_document="updateLookup", pipeline=pipeline)


def test_change_stream_pipeline_defaults_to_empty_list():
    """When change_stream_pipeline is not configured, collection.watch() is called with an empty pipeline (no
    behavior change from before this setting existed)."""
    stream, collection = _make_stream({})

    list(stream.get_records(context=None))

    collection.watch.assert_called_once_with(full_document="updateLookup", pipeline=[])

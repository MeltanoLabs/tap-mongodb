"""Tests for the end of a LOG_BASED stream on a collection with no new changes."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from singer_sdk.streams.core import REPLICATION_LOG_BASED

from tap_mongodb.streams import MongoDBCollectionStream


def test_slow_open_saves_the_current_resume_token(monkeypatch):
    """A change stream that takes longer than the idle timeout to open still moves the bookmark to its token."""
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "tap_mongodb.streams.time", SimpleNamespace(time=lambda: clock.now, sleep=lambda _seconds: None)
    )

    tap = MagicMock()
    tap.config = {"operation_types": ["insert", "update"]}
    tap.state = {
        "bookmarks": {
            "my_collection": {"replication_key": "replication_key", "replication_key_value": "old-token"},
        },
    }
    tap.initialized_at = 0
    tap.name = "tap-mongodb"

    connector = MagicMock()
    connector.version = (6, 0)
    collection = MagicMock()
    connector.mongo_client.__getitem__.return_value.__getitem__.return_value = collection

    change_stream = MagicMock()
    change_stream.__enter__.return_value = change_stream
    change_stream.__exit__.return_value = False
    change_stream.alive = True
    change_stream.resume_token = {"_data": "new-token"}
    change_stream.try_next.return_value = None

    def slow_watch(**_kwargs):
        clock.now += 15
        return change_stream

    collection.watch.side_effect = slow_watch

    catalog_entry = {
        "database_name": "my_database",
        "table_name": "my_collection",
        "tap_stream_id": "my_collection",
        "schema": {"type": "object", "properties": {}},
    }
    stream = MongoDBCollectionStream(tap=tap, catalog_entry=catalog_entry, connector=connector)
    stream.forced_replication_method = REPLICATION_LOG_BASED

    stream.sync()

    assert stream.get_context_state(None)["replication_key_value"] == "new-token"

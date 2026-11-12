"""Tests for a LOG_BASED stream with no state, which extracts every document before it reads changes."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from uuid import uuid4

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from tap_mongodb.streams import MongoDBCollectionStream
from tap_mongodb.tap import TapMongoDB

MONGODB_URI = os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING", "mongodb://admin:password@localhost:27017/")


@pytest.fixture
def collection():
    client: MongoClient = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
    try:
        hello = client.admin.command("hello")
    except PyMongoError:
        pytest.skip(f"MongoDB isn't reachable at {MONGODB_URI}")
    if "setName" not in hello:
        pytest.skip("Change streams need a replica set")
    coll = client[f"tap_mongodb_no_state_{uuid4().hex[:8]}"]["docs"]
    yield coll
    client.drop_database(coll.database.name)
    client.close()


def _sync(collection, state: dict | None = None) -> tuple[list[dict], dict, list[str]]:
    """Sync the collection with LOG_BASED replication, and return its records, final state and key properties."""
    config = {
        "mongodb_connection_string": MONGODB_URI,
        "database": collection.database.name,
        "filter_collections": [collection.name],
    }
    catalog = TapMongoDB(config=config, parse_env_config=False).catalog_dict
    (stream,) = catalog["streams"]
    stream["replication_method"] = "LOG_BASED"
    next(entry for entry in stream["metadata"] if not entry["breadcrumb"])["metadata"]["selected"] = True

    buf = io.StringIO()
    with redirect_stdout(buf):
        TapMongoDB(config=config, catalog=catalog, state=state, parse_env_config=False).sync_all()
    messages = [json.loads(line) for line in buf.getvalue().splitlines()]
    records = [message["record"] for message in messages if message["type"] == "RECORD"]
    final_state = next(message["value"] for message in reversed(messages) if message["type"] == "STATE")
    key_properties = next(message["key_properties"] for message in messages if message["type"] == "SCHEMA")
    return records, final_state, key_properties


def _ids(count: int) -> list[str]:
    """`_id` values that aren't ObjectIds, which the extraction doesn't need, and that sort after any resume token."""
    return [f"z{uuid4().hex}" for _ in range(count)]


def test_no_state_extracts_every_document_then_reads_changes(collection, monkeypatch):
    existing = _ids(3)
    collection.insert_many([{"_id": id_} for id_ in existing])
    (during,) = _ids(1)

    def extract_all_then_insert(self, coll, **kwargs):
        start = yield from extract_all(self, coll, **kwargs)
        coll.insert_one({"_id": during})
        return start

    extract_all = MongoDBCollectionStream._extract_all
    monkeypatch.setattr(MongoDBCollectionStream, "_extract_all", extract_all_then_insert)

    records, state, _ = _sync(collection)

    extracted = [r for r in records if r["operation_type"] is None]
    assert sorted(r["object_id"] for r in extracted) == sorted(existing)
    assert all(r["replication_key"] is None for r in extracted)
    # An extracted document sorts before any change made after the extraction started
    assert all(r["cluster_time"] <= records[-1]["cluster_time"] for r in extracted)
    # The change stream starts with the latest write before the sync with no state, so it may repeat what the sync read
    assert records[-1]["operation_type"] == "insert"
    assert records[-1]["object_id"] == during
    (bookmark,) = state["bookmarks"].values()
    assert bookmark["replication_key_value"] == records[-1]["replication_key"]

    monkeypatch.undo()
    (after,) = _ids(1)
    collection.insert_one({"_id": after})
    resumed, _, _ = _sync(collection, state)

    assert [(r["operation_type"], r["object_id"]) for r in resumed] == [("insert", after)]


def test_documents_are_keyed_on_id_and_a_quiet_sync_emits_no_row(collection):
    existing = _ids(2)
    collection.insert_many([{"_id": id_} for id_ in existing])

    records, state, key_properties = _sync(collection)

    assert key_properties == ["object_id"]
    # No placeholder row, whose `object_id` would be null. A replayed insert has the same key as its document's row
    assert {r["object_id"] for r in records} == set(existing)

    # A sync with no changes emits no placeholder row, but still stores the resume token
    quiet, state, _ = _sync(collection, state)
    assert quiet == []

    (after,) = _ids(1)
    collection.insert_one({"_id": after})
    resumed, _, _ = _sync(collection, state)

    assert [(r["operation_type"], r["object_id"]) for r in resumed] == [("insert", after)]


def test_update_of_a_since_deleted_document_keeps_its_id(collection):
    (id_,) = _ids(1)
    collection.insert_one({"_id": id_})
    _, state, _ = _sync(collection)
    collection.update_one({"_id": id_}, {"$set": {"status": "sent"}})
    collection.delete_one({"_id": id_})

    resumed, _, _ = _sync(collection, state)

    assert [(r["operation_type"], r["object_id"]) for r in resumed] == [("update", id_), ("delete", id_)]

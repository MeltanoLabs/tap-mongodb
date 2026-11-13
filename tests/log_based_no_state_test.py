"""Tests for a LOG_BASED stream with no state, which extracts every document before it reads changes."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from datetime import datetime, timezone
from uuid import uuid4

import pyarrow as pa
import pytest
from bson import Binary, DatetimeMS, Decimal128, Int64, ObjectId
from bson.binary import UuidRepresentation
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from tap_mongodb.arrow import ArrowExtractor
from tap_mongodb.streams import MongoDBCollectionStream
from tap_mongodb.tap import TapMongoDB
from tests.arrow_test import _normalize_batch_document, _with_sql_nulls

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


def _sync(
    collection,
    state: dict | None = None,
    batch_config: dict | None = None,
    uri: str = MONGODB_URI,
    declared: dict | None = None,
) -> tuple[list[dict], dict, list[str]]:
    """Sync the collection with LOG_BASED replication, and return its records, final state and key properties.

    The records include the rows of Arrow batch files, where the message has a `BATCH` type. `declared` fields select
    the flat document layout.
    """
    config = {
        "mongodb_connection_string": uri,
        "database": collection.database.name,
        "filter_collections": [collection.name],
    }
    if batch_config:
        config["batch_config"] = batch_config
    catalog = TapMongoDB(config=config, parse_env_config=False).catalog_dict
    (stream,) = catalog["streams"]
    stream["replication_method"] = "LOG_BASED"
    next(entry for entry in stream["metadata"] if not entry["breadcrumb"])["metadata"]["selected"] = True
    if declared:
        stream["schema"]["properties"].update(declared)
        config["document_layout"] = "flat"

    buf = io.StringIO()
    with redirect_stdout(buf):
        TapMongoDB(config=config, catalog=catalog, state=state, parse_env_config=False).sync_all()
    messages = [json.loads(line) for line in buf.getvalue().splitlines()]
    records = []
    for message in messages:
        if message["type"] == "RECORD":
            records.append(message["record"])
        elif message["type"] == "BATCH":
            (url,) = message["manifest"]
            with pa.ipc.open_file(url.removeprefix("file://")) as reader:
                records.extend({**row, "message": "BATCH"} for row in reader.read_all().to_pylist())
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


@pytest.mark.parametrize("arrow", [False, True], ids=["records", "arrow"])
def test_update_of_a_since_deleted_document_keeps_its_id(collection, tmp_path, arrow):
    (id_,) = _ids(1)
    collection.insert_one({"_id": id_})
    _, state, _ = _sync(collection)
    collection.update_one({"_id": id_}, {"$set": {"status": "sent"}})
    collection.delete_one({"_id": id_})

    # One row per file, so the update has a file with only a null document
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}, "batch_size": 1}
    resumed, _, _ = _sync(collection, state, batch_config=batch_config if arrow else None)

    assert [(r["operation_type"], r["object_id"], r.get("message")) for r in resumed] == [
        ("update", id_, "BATCH" if arrow else None),
        ("delete", id_, "BATCH" if arrow else None),
    ]
    assert resumed[0]["document"] is None


def test_arrow_change_events_match_record_mode(collection, tmp_path):
    collection.insert_one({"count": 0})
    _, state, _ = _sync(collection)
    first, second = collection.insert_many(
        [
            {
                "count": 1,
                "big": Int64(2),
                "ratio": 1.5,
                "name": "a",
                "flag": True,
                "at": datetime(2026, 1, 1, 12, 0, 0, 123000, tzinfo=timezone.utc),
                "ref": ObjectId(),
                "price": Decimal128("1.5"),
                "nested": {"x": 1, "y": [1, "a"]},
                "tags": [1, 2],
                "mixed": 1,
            },
            {"count": 2, "mixed": "x"},
        ]
    ).inserted_ids
    collection.update_one({"_id": second}, {"$set": {"name": "b"}})
    collection.replace_one({"_id": second}, {"count": 3})
    collection.delete_one({"_id": first})

    records, record_state, _ = _sync(collection, state)
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}
    rows, row_state, _ = _sync(collection, state, batch_config=batch_config)

    assert [r["operation_type"] for r in rows] == ["insert", "insert", "update", "replace", "delete"]
    assert all(r["message"] == "BATCH" for r in rows)
    assert row_state == record_state
    for row, record in zip(rows, records, strict=True):
        assert row["replication_key"] == record["replication_key"]
        assert row["object_id"] == record["object_id"]
        assert row["cluster_time"].isoformat() == record["cluster_time"]
        assert row["namespace"] == record["namespace"]
        expected = _with_sql_nulls(record["document"], list(row["document"]))
        assert _normalize_batch_document(row["document"], {"nested", "tags", "mixed"}) == expected


def test_arrow_change_events_with_the_flat_layout(collection, tmp_path):
    declared = {
        "n": {"type": ["integer", "null"]},
        "at": {"type": ["string", "null"], "format": "date-time"},
        "tags": {"type": ["array", "null"]},
    }
    collection.insert_one({"n": 0})
    _, state, _ = _sync(collection, declared=declared)
    at = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    collection.insert_one({"n": 1, "at": at, "tags": [1], "other": True})

    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}
    (row,) = _sync(collection, state, batch_config=batch_config, declared=declared)[0]

    assert (row["message"], row["n"], row["at"], json.loads(row["tags"])) == ("BATCH", 1, at, [1])
    assert "document" not in row
    assert "other" not in row


def test_no_state_with_arrow_batches_writes_every_document_then_reads_changes(collection, tmp_path, monkeypatch):
    existing = _ids(3)
    collection.insert_many([{"_id": id_, "n": 1} for id_ in existing])
    (during,) = _ids(1)

    def all_tables_then_insert(self, cluster_time):
        yield from all_tables(self, cluster_time)
        collection.insert_one({"_id": during})

    all_tables = ArrowExtractor.all_tables
    monkeypatch.setattr(ArrowExtractor, "all_tables", all_tables_then_insert)
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}

    records, state, _ = _sync(collection, batch_config=batch_config)

    assert all(r["message"] == "BATCH" for r in records)
    assert sorted(r["object_id"] for r in records) == sorted(existing)
    assert all(r["replication_key"] is None and r["operation_type"] is None for r in records)
    assert all(r["cluster_time"] is not None for r in records)

    # The bookmark predates the read, so the change made during it comes with the next sync
    monkeypatch.undo()
    resumed, _, _ = _sync(collection, state, batch_config=batch_config)
    assert [(r["operation_type"], r["object_id"], r["message"]) for r in resumed] == [("insert", during, "BATCH")]


def test_arrow_batches_leave_documents_inserted_during_the_read_to_the_change_stream(collection, tmp_path, monkeypatch):
    # More than one raw batch, so the insert comes before the cursor reaches the end. The `_id` types are mixed, and
    # the newest `_id` is the ObjectId, which sorts after every string.
    existing = [*_ids(150), ObjectId()]
    collection.insert_many([{"_id": id_, "n": 1} for id_ in existing])
    during = ObjectId()

    def insert_then_table(self, *args, **kwargs):
        if not collection.find_one({"_id": during}):
            collection.insert_one({"_id": during, "n": "one"})
        return table(self, *args, **kwargs)

    table = ArrowExtractor._table
    monkeypatch.setattr(ArrowExtractor, "_table", insert_then_table)
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}

    records, state, _ = _sync(collection, batch_config=batch_config)

    assert len(records) == len(existing)
    assert all(r["document"]["n"] == 1 for r in records)

    monkeypatch.undo()
    resumed, _, _ = _sync(collection, state, batch_config=batch_config)
    assert [(r["operation_type"], r["object_id"]) for r in resumed] == [("insert", str(during))]


def test_arrow_batches_key_each_row_like_the_change_events_for_any_id_type(collection, tmp_path):
    collection.insert_many([{"_id": id_} for id_ in [datetime(2024, 1, 1, tzinfo=timezone.utc), {"a": 1}, "z", 5]])
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}
    records, state, _ = _sync(collection, batch_config=batch_config)

    collection.update_many({}, {"$set": {"n": 1}})
    resumed, _, _ = _sync(collection, state, batch_config=batch_config)

    assert sorted(r["object_id"] for r in resumed) == sorted(r["object_id"] for r in records)


@pytest.mark.parametrize("arrow", [False, True], ids=["records", "arrow"])
@pytest.mark.parametrize(
    ("uri_option", "stored_as"),
    [("", UuidRepresentation.STANDARD), ("uuidRepresentation=javaLegacy", UuidRepresentation.JAVA_LEGACY)],
    ids=["default", "connection-string"],
)
def test_uuids_are_emitted_as_uuid_strings(collection, tmp_path, arrow, uri_option, stored_as):
    value = uuid4()
    collection.insert_one({"_id": Binary.from_uuid(value, stored_as), "ref": Binary.from_uuid(value, stored_as)})
    batch_config = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}} if arrow else None
    uri = f"{MONGODB_URI}{'&' if '?' in MONGODB_URI else '?'}{uri_option}" if uri_option else MONGODB_URI
    records, state, _ = _sync(collection, batch_config=batch_config, uri=uri)

    collection.update_one({}, {"$set": {"n": 1}})
    resumed, _, _ = _sync(collection, state, batch_config=batch_config, uri=uri)

    assert [r["operation_type"] for r in resumed] == ["update"]
    assert {(r["object_id"], r["document"]["ref"]) for r in records + resumed} == {(str(value), str(value))}


def test_a_date_out_of_range_loads_with_the_connection_string_conversion(collection):
    collection.insert_one({"date": DatetimeMS(-62135596800001)})  # A millisecond before the year 1
    uri = f"{MONGODB_URI}{'&' if '?' in MONGODB_URI else '?'}datetime_conversion=DATETIME_CLAMP"
    records, _, _ = _sync(collection, uri=uri)

    assert {r["document"]["date"] for r in records} == {"0001-01-01T00:00:00"}

"""Tests for Arrow BATCH extraction."""

from __future__ import annotations

import io
import json
import logging
import math
import os
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pyarrow as pa
import pytest
from bson import Binary, Decimal128, Int64, ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongoarrow.types import ObjectIdType
from singer_sdk.contrib.batch_decoder_arrow import ArrowReader
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED

from tap_mongodb import arrow
from tap_mongodb.arrow import DocumentPlan, incremental_ids, object_id_hex
from tap_mongodb.streams import MongoDBCollectionStream
from tap_mongodb.tap import TapMongoDB
from tap_mongodb.types import IncrementalId

MONGODB_URI = os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING", "mongodb://admin:password@localhost:27017/")


def test_document_plan(caplog: pytest.LogCaptureFixture):
    field_types = {
        "_id": {"objectId"},
        "count": {"int", "long"},
        "name": {"string", "null"},
        "empty": {"null"},
        "price": {"decimal"},
        "nested": {"object"},
        "tags": {"array"},
        "mixed": {"int", "string"},
    }
    with caplog.at_level(logging.WARNING):
        plan = DocumentPlan.from_field_types(field_types, stream_name="docs", logger=logging.getLogger("test"))

    assert plan.native == {
        "_id": ObjectIdType(),
        "count": pa.int64(),
        "empty": pa.string(),
        "name": pa.string(),
    }
    assert next(iter(plan.native)) == "_id"
    assert plan.as_string == ["price"]
    assert plan.as_json == ["mixed", "nested", "tags"]
    assert "Field 'mixed' in stream 'docs' has values of more than one BSON type (int, string)" in caplog.text
    assert "'nested'" not in caplog.text


def test_object_id_hex_and_incremental_ids():
    object_ids = [ObjectId() for _ in range(3)]
    array = pa.array([oid.binary for oid in object_ids], type=pa.binary(12)).slice(1)

    hexes = object_id_hex(array)
    assert hexes.to_pylist() == [str(oid) for oid in object_ids[1:]]
    assert incremental_ids(array, hexes).to_pylist() == [
        str(IncrementalId.from_object_id(oid)) for oid in object_ids[1:]
    ]


def test_object_id_hex_keeps_nulls():
    array = pa.array([ObjectId().binary, None], type=pa.binary(12))
    assert object_id_hex(array).to_pylist()[1] is None


def _mock_stream(replication_method: str) -> MongoDBCollectionStream:
    tap = MagicMock()
    tap.config = {}
    tap.state = {}
    tap.initialized_at = 0
    tap.name = "tap-mongodb"
    catalog_entry = {
        "database_name": "my_database",
        "table_name": "my_collection",
        "tap_stream_id": "my_collection",
        "schema": {"type": "object", "properties": {}},
    }
    stream = MongoDBCollectionStream(tap=tap, catalog_entry=catalog_entry, connector=MagicMock())
    stream.forced_replication_method = replication_method
    return stream


def test_batch_config_arrow_defaults_to_larger_files():
    stream = _mock_stream(REPLICATION_INCREMENTAL)
    batch_config = stream.get_batch_config({"batch_config": {"encoding": {"format": "arrow"}}})
    assert batch_config is not None
    assert batch_config.batch_size == 100_000

    batch_config = stream.get_batch_config({"batch_config": {"encoding": {"format": "arrow"}, "batch_size": 5}})
    assert batch_config is not None
    assert batch_config.batch_size == 5


def test_batch_config_arrow_change_streams_emit_records():
    stream = _mock_stream(REPLICATION_LOG_BASED)
    assert stream.get_batch_config({"batch_config": {"encoding": {"format": "arrow"}}}) is None
    assert stream.get_record_batches(None) is None


def test_batch_config_jsonl_change_streams_unchanged():
    stream = _mock_stream(REPLICATION_LOG_BASED)
    batch_config = stream.get_batch_config({"batch_config": {"encoding": {"format": "jsonl"}}})
    assert batch_config is not None
    assert batch_config.encoding.format == "jsonl"


@pytest.fixture(scope="module")
def database():
    client: MongoClient = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip(f"MongoDB isn't reachable at {MONGODB_URI}")
    db = client[f"tap_mongodb_arrow_{uuid4().hex[:8]}"]
    yield db
    client.drop_database(db.name)
    client.close()


@pytest.fixture
def collection(database):
    coll = database[f"docs_{uuid4().hex[:8]}"]
    yield coll
    coll.drop()


def _sync(collection, tmp_path, *, batch: bool, state: dict | None = None) -> list[dict]:
    config = {
        "mongodb_connection_string": MONGODB_URI,
        "database": collection.database.name,
        "filter_collections": [collection.name],
    }
    if batch:
        config["batch_config"] = {
            "encoding": {"format": "arrow"},
            "storage": {"root": f"file://{tmp_path}"},
            "batch_size": 100,
        }
    tap = TapMongoDB(config=config, state=state, parse_env_config=False)
    buf = io.StringIO()
    with redirect_stdout(buf):
        tap.sync_all()
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def _batch_rows(messages: list[dict]) -> list[dict]:
    rows = []
    for message in messages:
        if message["type"] == "BATCH":
            (url,) = message["manifest"]
            with pa.ipc.open_file(url.removeprefix("file://")) as reader:
                rows.extend(reader.read_all().to_pylist())
    return rows


def _target_records(messages: list[dict]) -> list[dict]:
    """Read the batch files the way SDK targets do."""
    reader = ArrowReader(BaseBatchFileEncoding(format="arrow"))
    records = []
    for message in messages:
        if message["type"] == "BATCH":
            (url,) = message["manifest"]
            with open(url.removeprefix("file://"), "rb") as file:
                records.extend(reader.read(file))
    return records


def _normalize_batch_document(document: dict, as_json: set[str]) -> dict:
    """Turn an Arrow `document` into what RECORD mode's JSON would decode to."""
    normalized = {}
    for name, value in document.items():
        if value is not None and name in as_json:
            value = json.loads(value)
        elif isinstance(value, datetime):
            value = value.replace(tzinfo=None).isoformat()
        normalized[name] = value
    return normalized


def _with_sql_nulls(record_document: dict, fields: list[str]) -> dict:
    """RECORD mode's document with SQL semantics: a missing field is null, like an explicit null.

    An Arrow struct has a fixed set of fields, so it can't tell a missing field from a null one.
    """
    assert set(record_document) <= set(fields), "a RECORD field is missing from the Arrow struct"
    return {name: record_document.get(name) for name in fields}


def test_arrow_batches_match_record_mode(collection, tmp_path, caplog: pytest.LogCaptureFixture):
    collection.insert_many(
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
                "inf": math.inf,
            },
            {"count": Int64(2), "name": None, "mixed": "x", "nested": {"x": "y"}, "nan": math.nan},
            {"count": 3, "ratio": -math.inf, "blob": Binary(b"\x01\x02")},
            *({"count": i} for i in range(4, 251)),
        ],
    )

    record_messages = _sync(collection, tmp_path, batch=False)
    records = [m["record"] for m in record_messages if m["type"] == "RECORD"]
    with caplog.at_level(logging.WARNING):
        batch_messages = _sync(collection, tmp_path, batch=True)
    rows = _batch_rows(batch_messages)

    assert not [m for m in batch_messages if m["type"] == "RECORD"]
    assert len(rows) == len(records) == 250
    assert "Field 'mixed'" in caplog.text
    document_type = (
        pa.ipc.open_file(
            next(m for m in batch_messages if m["type"] == "BATCH")["manifest"][0].removeprefix("file://"),
        )
        .schema.field("document")
        .type
    )
    assert {document_type.field(name).type.extension_name for name in ("nested", "tags", "mixed")} == {"arrow.json"}

    for row, record in zip(rows, records, strict=True):
        assert row["replication_key"] == record["replication_key"]
        assert row["object_id"] == record["object_id"]
        assert row["namespace"] == record["namespace"]
        assert row["operation_type"] is None
        assert row["cluster_time"] is None
        expected = _with_sql_nulls(record["document"], list(row["document"]))
        assert _normalize_batch_document(row["document"], {"nested", "tags", "mixed"}) == expected

    # SDK targets decode `arrow.json` values, so they get the same documents as from RECORD messages
    for target_record, record in zip(_target_records(batch_messages), records, strict=True):
        document = target_record["document"]
        assert _normalize_batch_document(document, set()) == _with_sql_nulls(record["document"], list(document))


def test_arrow_batches_state_and_resume(collection, tmp_path):
    collection.insert_many([{"count": i} for i in range(250)])

    messages = _sync(collection, tmp_path, batch=True)
    batches = [i for i, m in enumerate(messages) if m["type"] == "BATCH"]
    assert len(batches) == 3  # 100, 100 and 50 rows
    bookmarks = [messages[i + 1]["value"]["bookmarks"][collection.name.lower()] for i in batches]
    rows = _batch_rows(messages)
    assert [b["replication_key_value"] for b in bookmarks] == [
        rows[99]["replication_key"],
        rows[199]["replication_key"],
        rows[249]["replication_key"],
    ]

    # Resuming from the final state extracts only documents inserted since
    state = next(m for m in reversed(messages) if m["type"] == "STATE")["value"]
    assert not _batch_rows(_sync(collection, tmp_path, batch=True, state=state))
    collection.insert_one({"count": 250})
    resumed = _batch_rows(_sync(collection, tmp_path, batch=True, state=state))
    assert [row["document"]["count"] for row in resumed] == [250]


def test_arrow_batches_empty_collection(collection, tmp_path):
    collection.insert_one({"count": 1})
    collection.delete_many({})
    assert not _batch_rows(_sync(collection, tmp_path, batch=True))


def test_arrow_batches_add_record_metadata(collection, tmp_path):
    collection.insert_one({"count": 1})
    config_messages = _sync_with_metadata(collection, tmp_path)
    (row,) = _batch_rows(config_messages)
    assert isinstance(row["_sdc_batched_at"], datetime)
    assert row["_sdc_batched_at"].tzinfo is not None
    assert row["_sdc_batched_at"] <= datetime.now(timezone.utc)


def _sync_with_metadata(collection, tmp_path) -> list[dict]:
    config = {
        "mongodb_connection_string": MONGODB_URI,
        "database": collection.database.name,
        "filter_collections": [collection.name],
        "add_record_metadata": True,
        "batch_config": {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}},
    }
    buf = io.StringIO()
    with redirect_stdout(buf):
        TapMongoDB(config=config, parse_env_config=False).sync_all()
    return [json.loads(line) for line in buf.getvalue().splitlines()]


@pytest.mark.parametrize(
    "seed,update,bson_type",
    [
        pytest.param({"count": 1}, {"count": 1.5}, "double", id="int-to-double"),
        pytest.param({"count": 1}, {"count": True}, "bool", id="int-to-bool"),
        pytest.param({"ratio": 0.5}, {"ratio": True}, "bool", id="double-to-bool"),
    ],
)
def test_arrow_batches_fail_on_silently_coerced_update(collection, tmp_path, monkeypatch, seed, update, bson_type):
    """A document updated during the sync can't be silently truncated or coerced by pymongoarrow."""
    collection.insert_many([dict(seed) for _ in range(3)])
    updated = collection.find_one(sort=[("_id", -1)])["_id"]
    scan = arrow.scan_field_types

    def scan_then_update(*args, **kwargs):
        field_types = scan(*args, **kwargs)
        collection.update_one({"_id": updated}, {"$set": update})  # After the scan, before extraction
        return field_types

    monkeypatch.setattr(arrow, "scan_field_types", scan_then_update)
    (field,) = update
    with pytest.raises(RuntimeError, match=f"Field '{field}' .* BSON type '{bson_type}' in document {updated}"):
        _sync(collection, tmp_path, batch=True)

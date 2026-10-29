"""Tests for the flat document layout."""

from __future__ import annotations

import io
import json
import logging
import math
from contextlib import redirect_stdout
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from bson import Decimal128, Int64, ObjectId
from singer_sdk.contrib.batch_decoder_arrow import ArrowReader
from singer_sdk.helpers._batch import BaseBatchFileEncoding

from tap_mongodb import arrow
from tap_mongodb.flat import DeclaredField, DeclaredTypeError, declared_fields, flatten_document
from tap_mongodb.tap import TapMongoDB

DECLARED = {
    "age": {"type": ["integer", "null"]},
    "score": {"type": ["number", "null"]},
    "active": {"type": ["boolean", "null"]},
    "joined": {"type": ["string", "null"], "format": "date-time"},
    "name": {"type": ["string", "null"]},
    "ref": {"type": ["string", "null"]},
    "price": {"type": ["string", "null"]},
    "address": {"type": ["object", "null"]},
    "tags": {"type": ["array", "null"]},
    "misc": {"type": ["integer", "string", "null"]},
}


@pytest.mark.parametrize(
    "schema,kind",
    [
        ({"type": ["integer", "null"]}, "integer"),
        ({"type": "number"}, "number"),
        ({"type": ["boolean", "null"]}, "boolean"),
        ({"type": ["string", "null"], "format": "date-time"}, "date-time"),
        ({"type": ["string", "null"]}, "string"),
        ({"type": ["object", "null"]}, "object"),
        ({"type": ["array", "null"]}, "array"),
        ({"type": ["integer", "string", "null"]}, "json"),
        ({}, "json"),
    ],
)
def test_declared_field_kind(schema, kind):
    assert DeclaredField.from_property("f", schema).kind == kind


def test_declared_fields_skip_the_envelope():
    properties = {"replication_key": {}, "object_id": {}, "document": {}, "_sdc_batched_at": {}, **DECLARED}
    assert [field.name for field in declared_fields(properties, stream_name="s")] == list(DECLARED)


def test_declared_fields_required():
    with pytest.raises(ValueError, match="requires the schema of stream 's' to declare the document fields"):
        declared_fields({"replication_key": {}, "document": {}}, stream_name="s")


def test_declared_fields_reject_paths():
    with pytest.raises(ValueError, match=r"contain '\.' or start with '\$'.*: a\.b, \$c"):
        declared_fields({"a.b": {"type": "string"}, "$c": {"type": "string"}}, stream_name="s")


def test_flatten_document_converts_values():
    fields = declared_fields(DECLARED, stream_name="s")
    ref = ObjectId()
    joined = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values, undeclared = flatten_document(
        {
            "_id": ObjectId(),
            "age": Int64(30),
            "score": math.inf,
            "joined": joined,
            "name": None,
            "ref": ref,
            "price": Decimal128("1.5"),
            "address": {"city": "Oslo"},
            "misc": "a",
            "extra": 1,
        },
        fields,
        stream_name="s",
    )
    assert values == {
        "age": 30,
        "score": None,
        "active": None,
        "joined": joined,
        "name": None,
        "ref": str(ref),
        "price": "1.5",
        "address": {"city": "Oslo"},
        "tags": None,
        "misc": "a",
    }
    assert undeclared == {"extra"}


@pytest.mark.parametrize(
    "name,value,bson_type",
    [
        ("age", 1.5, "double"),
        ("age", True, "bool"),
        ("score", "1", "string"),
        ("active", 1, "int"),
        ("joined", "2026-01-01", "string"),
        ("name", 1, "int"),
        ("address", [1], "array"),
        ("tags", {"a": 1}, "object"),
    ],
)
def test_flatten_document_rejects_mismatches(name, value, bson_type):
    fields = declared_fields(DECLARED, stream_name="s")
    with pytest.raises(DeclaredTypeError, match=f"Field '{name}' in stream 's' .* BSON type '{bson_type}'"):
        flatten_document({"_id": 1, name: value}, fields, stream_name="s")


def test_flatten_document_none():
    fields = declared_fields({"age": {"type": "integer"}}, stream_name="s")
    assert flatten_document(None, fields, stream_name="s") == ({"age": None}, set())


@pytest.fixture(scope="module")
def database(mongodb_client):
    db = mongodb_client[f"tap_mongodb_flat_{uuid4().hex[:8]}"]
    yield db
    mongodb_client.drop_database(db.name)


@pytest.fixture
def collection(database):
    coll = database[f"docs_{uuid4().hex[:8]}"]
    yield coll
    coll.drop()


def _sync(
    mongodb_uri, collection, tmp_path, *, batch: bool, declared: dict, deselected: tuple[str, ...] = ()
) -> list[dict]:
    config = {
        "mongodb_connection_string": mongodb_uri,
        "database": collection.database.name,
        "filter_collections": [collection.name],
        "document_layout": "flat",
    }
    if batch:
        config["batch_config"] = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}

    catalog = TapMongoDB(config={**config, "document_layout": "object"}, parse_env_config=False).catalog_dict
    (stream,) = catalog["streams"]
    stream["schema"]["properties"].update(declared)
    for entry in stream["metadata"]:
        if entry["breadcrumb"] == []:
            entry["metadata"]["selected"] = True
        elif entry["breadcrumb"][-1] in deselected:
            entry["metadata"]["selected"] = False
    stream["metadata"].extend(
        {"breadcrumb": ["properties", name], "metadata": {"selected": False}} for name in deselected
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        TapMongoDB(config=config, catalog=catalog, parse_env_config=False).sync_all()
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def _rows(messages: list[dict]) -> list[dict]:
    """RECORD messages' records, or BATCH files read the way SDK targets read them."""
    reader = ArrowReader(BaseBatchFileEncoding(format="arrow"))
    rows = [m["record"] for m in messages if m["type"] == "RECORD"]
    for message in messages:
        if message["type"] == "BATCH":
            with open(message["manifest"][0].removeprefix("file://"), "rb") as file:
                rows.extend(reader.read(file))
    return rows


def _normalize(row: dict) -> dict:
    """Make RECORD and Arrow values comparable: timestamps and numbers."""
    normalized = {}
    for name, value in row.items():
        if isinstance(value, datetime):
            value = value.replace(tzinfo=None).isoformat()
        elif isinstance(value, float) and value.is_integer():
            value = int(value)
        normalized[name] = value
    return normalized


def test_flat_layout_record_and_arrow_match(mongodb_uri, collection, tmp_path, caplog: pytest.LogCaptureFixture):
    ref = ObjectId()
    collection.insert_many(
        [
            {
                "age": 30,
                "score": 1,
                "active": True,
                "joined": datetime(2026, 1, 1, 12, 0, 0, 123000, tzinfo=timezone.utc),
                "name": "Ada",
                "ref": ref,
                "price": Decimal128("1.5"),
                "address": {"city": "Oslo", "zip": "0150"},
                "tags": ["vip", 1],
                "misc": 1,
                "extra": "x",
            },
            {"age": Int64(2), "score": 2.5, "name": None, "price": "3", "misc": "a"},
        ],
    )

    with caplog.at_level(logging.WARNING):
        record_messages = _sync(mongodb_uri, collection, tmp_path, batch=False, declared=DECLARED)
        batch_messages = _sync(mongodb_uri, collection, tmp_path, batch=True, declared=DECLARED)

    for messages in (record_messages, batch_messages):
        schema = next(m for m in messages if m["type"] == "SCHEMA")["schema"]
        assert "document" not in schema["properties"]
        assert set(DECLARED) <= set(schema["properties"])
    assert not [m for m in batch_messages if m["type"] == "RECORD"]
    assert caplog.text.count("these undeclared document fields are dropped: extra") == 2

    record_rows, batch_rows = _rows(record_messages), _rows(batch_messages)
    assert [_normalize(row) for row in batch_rows] == [_normalize(row) for row in record_rows]
    assert record_rows[0]["ref"] == str(ref)
    assert record_rows[0]["price"] == "1.5"
    assert record_rows[0]["address"] == {"city": "Oslo", "zip": "0150"}
    assert record_rows[1]["age"] == 2
    assert record_rows[1]["active"] is None  # missing and null are both null


@pytest.mark.parametrize("batch", [False, True], ids=["record", "arrow"])
def test_flat_layout_type_mismatch_fails(mongodb_uri, collection, tmp_path, batch: bool):
    collection.insert_one({"age": 1})
    bad = collection.insert_one({"age": 1.5}).inserted_id

    with pytest.raises(DeclaredTypeError, match=f"Field 'age' .* 'integer', but document {bad} .* 'double'"):
        _sync(mongodb_uri, collection, tmp_path, batch=batch, declared={"age": {"type": ["integer", "null"]}})


def test_flat_layout_arrow_fails_before_writing_files(mongodb_uri, collection, tmp_path):
    collection.insert_many([{"age": i} for i in range(5)] + [{"age": "five"}])
    with pytest.raises(DeclaredTypeError):
        _sync(mongodb_uri, collection, tmp_path, batch=True, declared={"age": {"type": ["integer", "null"]}})
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("batch", [False, True], ids=["record", "arrow"])
def test_flat_layout_skips_deselected_fields(
    mongodb_uri, collection, tmp_path, caplog: pytest.LogCaptureFixture, batch: bool
):
    collection.insert_one({"age": 1, "score": "not a number", "extra": 1})
    with caplog.at_level(logging.WARNING):
        messages = _sync(
            mongodb_uri,
            collection,
            tmp_path,
            batch=batch,
            declared={"age": {"type": ["integer", "null"]}, "score": {"type": ["number", "null"]}},
            deselected=("score",),
        )
    (row,) = _rows(messages)
    assert row["age"] == 1
    assert "score" not in row
    # Deselected declared fields aren't reported as undeclared
    assert "these undeclared document fields are dropped: extra\n" in caplog.text


@pytest.mark.parametrize("batch", [False, True], ids=["record", "arrow"])
def test_flat_layout_requires_declared_fields(mongodb_uri, collection, tmp_path, batch: bool):
    collection.insert_one({"age": 1})
    with pytest.raises(ValueError, match=r"requires the schema of stream .* to declare the document fields"):
        _sync(mongodb_uri, collection, tmp_path, batch=batch, declared={})


@pytest.mark.parametrize("update,bson_type", [(1.5, "double"), (True, "bool")])
def test_flat_layout_arrow_fails_on_silently_coerced_update(
    mongodb_uri, collection, tmp_path, monkeypatch, update, bson_type
):
    """A declared integer updated during the sync can't be silently truncated or coerced by pymongoarrow."""
    collection.insert_many([{"age": i} for i in range(3)])
    updated = collection.find_one(sort=[("_id", -1)])["_id"]
    scan = arrow.scan_declared_types

    def scan_then_update(*args, **kwargs):
        result = scan(*args, **kwargs)
        collection.update_one({"_id": updated}, {"$set": {"age": update}})  # After the check, before extraction
        return result

    monkeypatch.setattr(arrow, "scan_declared_types", scan_then_update)
    with pytest.raises(RuntimeError, match=f"Field 'age' .* BSON type '{bson_type}' in document {updated}"):
        _sync(mongodb_uri, collection, tmp_path, batch=True, declared={"age": {"type": ["integer", "null"]}})

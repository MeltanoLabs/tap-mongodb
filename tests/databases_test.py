"""Tests for extracting from more than one database."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from uuid import uuid4

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from tap_mongodb.tap import TapMongoDB

MONGODB_URI = os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING", "mongodb://admin:password@localhost:27017/")


@pytest.fixture(scope="module")
def client():
    client: MongoClient = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip(f"MongoDB isn't reachable at {MONGODB_URI}")
    yield client
    client.close()


@pytest.fixture(scope="module")
def databases(client):
    """Two databases, each with a collection of the same name holding one document."""
    collection = f"docs_{uuid4().hex[:8]}"
    names = [f"tap_mongodb_databases_{uuid4().hex[:8]}" for _ in range(2)]
    for name in names:
        client[name][collection].insert_one({"source": name})
    yield names, collection
    for name in names:
        client.drop_database(name)


def _streams(config: dict) -> list[dict]:
    tap = TapMongoDB(config={"mongodb_connection_string": MONGODB_URI, **config}, parse_env_config=False)
    return tap.catalog_dict["streams"]


def test_discovers_every_database_except_system_ones(databases):
    names, collection = databases

    streams = _streams({})

    discovered = {(stream["database_name"], stream["tap_stream_id"]) for stream in streams}
    assert {(name, f"{name}-{collection}") for name in names} <= discovered
    assert not {stream["database_name"] for stream in streams} & {"admin", "config", "local"}


def test_database_keeps_stream_names(databases):
    names, collection = databases

    streams = _streams({"database": names[0], "filter_collections": [collection]})

    assert [(stream["database_name"], stream["tap_stream_id"]) for stream in streams] == [(names[0], collection)]


def test_databases_takes_precedence_over_database(databases):
    names, collection = databases

    streams = _streams({"database": names[0], "databases": [names[1]], "filter_collections": [collection]})

    assert [(stream["database_name"], stream["tap_stream_id"]) for stream in streams] == [
        (names[1], f"{names[1]}-{collection}")
    ]


def test_sync_reads_each_stream_from_its_database(databases):
    names, collection = databases
    config = {"mongodb_connection_string": MONGODB_URI, "databases": names, "filter_collections": [collection]}

    buf = io.StringIO()
    with redirect_stdout(buf):
        TapMongoDB(config=config, parse_env_config=False).sync_all()

    messages = [json.loads(line) for line in buf.getvalue().splitlines()]
    records = {
        message["stream"]: message["record"]["document"]["source"]
        for message in messages
        if message["type"] == "RECORD"
    }
    assert records == {f"{name}-{collection}": name for name in names}

"""Tests for the credentials given outside the connection string."""

from __future__ import annotations

import os
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from tap_mongodb.tap import TapMongoDB

MONGODB_URI = os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING", "mongodb://admin:password@localhost:27017/")


@pytest.fixture(scope="module")
def collection():
    client: MongoClient = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip(f"MongoDB isn't reachable at {MONGODB_URI}")
    coll = client[f"tap_mongodb_credentials_{uuid4().hex[:8]}"]["docs"]
    coll.insert_one({})
    yield coll
    client.drop_database(coll.database.name)
    client.close()


def test_username_and_password_authenticate(collection):
    uri = urlsplit(MONGODB_URI)
    config = {
        "mongodb_connection_string": uri._replace(netloc=uri.netloc.rpartition("@")[2]).geturl(),
        "username": unquote(uri.username),
        "password": unquote(uri.password),
        "database": collection.database.name,
    }

    streams = TapMongoDB(config=config, parse_env_config=False).catalog_dict["streams"]

    assert [stream["tap_stream_id"] for stream in streams] == [collection.name]

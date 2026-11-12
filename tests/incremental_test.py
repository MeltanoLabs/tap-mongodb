"""Tests for incremental replication of collections whose `_id` isn't always an ObjectId."""

from __future__ import annotations

import io
import os
from contextlib import redirect_stdout
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from bson import Binary, ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from tap_mongodb.tap import TapMongoDB

MONGODB_URI = os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING", "mongodb://admin:password@localhost:27017/")


@pytest.fixture(scope="module")
def database():
    client: MongoClient = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip(f"MongoDB isn't reachable at {MONGODB_URI}")
    db = client[f"tap_mongodb_incremental_{uuid4().hex[:8]}"]
    yield db
    client.drop_database(db.name)
    client.close()


@pytest.mark.parametrize("batch", [False, True], ids=["records", "arrow"])
@pytest.mark.parametrize(
    "other_id",
    [Binary(uuid4().bytes, 4), "a-string", datetime(2026, 1, 1, tzinfo=timezone.utc)],
    ids=["uuid", "string", "date"],
)
def test_incremental_fails_on_a_non_object_id(database, tmp_path, batch, other_id):
    collection = database[f"docs_{uuid4().hex[:8]}"]
    collection.insert_many([{"_id": ObjectId()}, {"_id": other_id}])
    config = {
        "mongodb_connection_string": MONGODB_URI,
        "database": database.name,
        "filter_collections": [collection.name],
    }
    if batch:
        config["batch_config"] = {"encoding": {"format": "arrow"}, "storage": {"root": f"file://{tmp_path}"}}

    with pytest.raises(TypeError, match="requires every `_id` to be an ObjectId"), redirect_stdout(io.StringIO()):
        TapMongoDB(config=config, parse_env_config=False).sync_all()

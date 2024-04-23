"""Test incremental replication for tap-mongodb."""

import os
from datetime import datetime, timezone

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


def test_incremental_replication() -> None:
    """Test incremental replication"""
    # GIVEN a MongoDB database with a record in it
    mongo_client: MongoClient = MongoClient(os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"])
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    collection_name: str = "IncrementalTestCollection"
    document: dict = {
        "id": "765a9d71-84de-482b-aa53-46d32753175a",
        "numeric_field": 42,
        "string_field": "Hello, World!",
        "date_field": datetime.now(timezone.utc),
    }

    database: Database = mongo_client[database_name]
    collection: Collection = database[collection_name]
    inserted_id: ObjectId = collection.insert_one(document).inserted_id

    assert inserted_id is not None
    assert 1 == collection.count_documents({})

    # WHEN tap-mongodb is run in incremental mode with a start date that predates the oldest record in the collection

    # THEN all documents from the collection should be returned


def test_incremental_replication_two() -> None:
    """Test incremental replication"""
    # GIVEN a MongoDB database with a record in it
    mongo_client: MongoClient = MongoClient(os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"])
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    collection_name: str = "IncrementalTestCollection"
    document: dict = {
        "id": "765a9d71-84de-482b-aa53-46d32753175a",
        "numeric_field": 42,
        "string_field": "Hello, World!",
        "date_field": datetime.now(timezone.utc),
    }

    database: Database = mongo_client[database_name]
    collection: Collection = database[collection_name]
    inserted_id: ObjectId = collection.insert_one(document).inserted_id

    assert inserted_id is not None
    assert 1 == collection.count_documents({})

    # WHEN tap-mongodb is run in incremental mode with a start date that predates the oldest record in the collection

    # THEN all documents from the collection should be returned

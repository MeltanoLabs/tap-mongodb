"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_mongodb.tap import TapMongoDB
from tap_mongodb.streams import CollectionStream
from pymongo.mongo_client import MongoClient
from pymongo.database import Database

included_database = {
    "database": "test_service",
    "collection": "TestDocument",
}

SAMPLE_CONFIG = {
    "mongodb_connection_string": "mongodb://admin:password@localhost:27017/?replicaSet=rs0",
    "database_includes:": [
        included_database,
    ],
}

# Run standard built-in tap tests from the SDK:
TestTapMongoDB = get_tap_test_class(tap_class=TapMongoDB, config=SAMPLE_CONFIG)


def test_one_stream_is_discovered():
    # given a collection in the database
    with MongoClient(SAMPLE_CONFIG["mongodb_connection_string"]) as client:
        database: Database = client.get_database(included_database["database"])
        database.create_collection(included_database["collection"])

        # when the tap's discover_streams method is invoked
        tap: TapMongoDB = TapMongoDB(config=SAMPLE_CONFIG)
        streams: list[CollectionStream] = tap.discover_streams()

        # then a stream for that collection is returned
        assert len(streams) == 1
        stream: CollectionStream = streams[0]
        assert (
            stream.tap_stream_id
            == f"{included_database['database']}_{included_database['collection']}"
        )

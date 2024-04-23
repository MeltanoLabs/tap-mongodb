"""Test log-based replication for tap-mongodb."""

import json
import os
from datetime import datetime, timezone
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from loguru import logger
from rich import print_json

from integration_tests.test_core import MongoDBTestRunner
from tap_mongodb.tap import TapMongoDB


def test_log_based_replication_first_run_on_empty_collection() -> None:
    """Test log-based replication behavior on a mongodb database with one empty collection"""
    # GIVEN a MongoDB database collection with no records in it
    mongo_connection_string: str = os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"]
    mongo_client: MongoClient = MongoClient(mongo_connection_string)
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    collection_name: str = "LogBasedTestCollection"
    database: Database = mongo_client[database_name]
    database.create_collection(collection_name)
    collection: Collection = database[collection_name]

    assert 0 == collection.count_documents({})

    # WHEN tap-mongodb is run in log-based mode
    log_based_config: dict = {"database": database_name, "mongodb_connection_string": mongo_connection_string}
    tap_instance: TapMongoDB = TapMongoDB(config=log_based_config)

    tap_catalog: dict = json.loads(tap_instance.catalog_json_text)

    stream: dict
    for stream in tap_catalog["streams"]:
        stream["replication_key"] = "replication_key"
        stream["replication_method"] = "LOG_BASED"
        for metadata in stream["metadata"]:
            metadata["metadata"]["selected"] = True
            # if metadata["breadcrumb"] == []:
            #     metadata["metadata"]["replication-method"] = "LOG_BASED"

    # logger.info("tap_catalog:")
    # print_json(data=tap_catalog)

    test_runner: MongoDBTestRunner = MongoDBTestRunner(
        tap_class=TapMongoDB,
        config=log_based_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()

    # THEN all documents from the collection should be returned
    logger.info("test_runner.records:")
    print_json(data=test_runner.records)

    records: dict[str, list[dict]] = test_runner.records
    # Note that the tap uses lowercased collection names, we account for that here
    collection_name_lower: str = collection_name.lower()
    assert 1 == len(records[collection_name_lower])
    record: dict = records[collection_name_lower][0]

    # logger.info("record:")
    # print_json(data=record)

    # THEN the tap should capture a resume token, emit a "dummy" record, and exit

"""Test incremental replication for tap-mongodb."""

import json
import os
from datetime import datetime, timezone

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from test_core import MongoDBTestRunner

from tap_mongodb.tap import TapMongoDB
from tap_mongodb.types import IncrementalId


def test_incremental_replication_collection_with_one_record() -> None:
    """Test incremental replication behavior on a mongodb database with one collection containing one record."""
    # GIVEN a MongoDB database with a record in it
    mongo_connection_string: str = os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"]
    mongo_client: MongoClient = MongoClient(mongo_connection_string)
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    datetime_value: datetime = datetime(2024, 4, 23, 14, 8, 28, tzinfo=timezone.utc)

    collection_name: str = "IncrementalTestCollection"
    document: dict = {
        "id": "765a9d71-84de-482b-aa53-46d32753175a",
        "numeric_field": 42,
        "string_field": "Hello, World!",
        "date_field": datetime_value,
    }

    database: Database = mongo_client[database_name]
    collection: Collection = database[collection_name]
    inserted_id: ObjectId = collection.insert_one(document).inserted_id

    assert inserted_id is not None
    assert 1 == collection.count_documents({})

    # WHEN tap-mongodb is run in incremental mode with a start date that predates the oldest record in the collection
    incremental_config: dict = {"database": database_name, "mongodb_connection_string": mongo_connection_string}
    tap_instance: TapMongoDB = TapMongoDB(config=incremental_config)

    tap_catalog: dict = json.loads(tap_instance.catalog_json_text)

    stream: dict
    for stream in tap_catalog["streams"]:
        stream["replication_key"] = "replication_key"
        stream["replication_method"] = "INCREMENTAL"
        for metadata in stream["metadata"]:
            metadata["metadata"]["selected"] = True
            # if metadata["breadcrumb"] == []:
            #     metadata["metadata"]["replication-method"] = "INCREMENTAL"

    # logger.info("tap_catalog:")
    # print_json(data=tap_catalog)

    test_runner: MongoDBTestRunner = MongoDBTestRunner(
        tap_class=TapMongoDB,
        config=incremental_config,
        catalog=tap_catalog,
    )
    test_runner.sync_all()

    # THEN all documents from the collection should be returned
    # logger.info("test_runner.records:")
    # print_json(data=test_runner.records)

    records: dict[str, list[dict]] = test_runner.records
    # Note that the tap uses lowercased collection names, we account for that here
    collection_name_lower: str = collection_name.lower()
    assert 1 == len(records[collection_name_lower])
    record: dict = records[collection_name_lower][0]

    # replication_key should be present
    assert "replication_key" in record
    # replication key should be a valid IncrementalId string representation
    incremental_id: IncrementalId = IncrementalId.from_string(record["replication_key"])
    assert incremental_id is not None

    # object_id field should be present and should match the ObjectId of the inserted document
    assert "object_id" in record
    assert inserted_id == ObjectId(record["object_id"])
    assert str(inserted_id) == record["object_id"]

    # the replication key should match the ObjectId of the document
    assert ObjectId(record["object_id"]) == incremental_id.object_id

    # document field should be defined and should match the document that was inserted into the collection
    assert "document" in record
    assert str(inserted_id) == record["document"]["_id"]
    assert document["id"] == record["document"]["id"]
    assert document["numeric_field"] == record["document"]["numeric_field"]
    # Note that datetimes are serialized as strings in the tap output
    assert "2024-04-23T14:08:28" == record["document"]["date_field"]

    # operation_type field is only set in log-based replication mode. It should be defined here as None.
    assert "operation_type" in record
    assert record["operation_type"] is None

    # cluster_time field is only set in log-based replication mode. It should be defined here as None.
    assert "cluster_time" in record
    assert record["cluster_time"] is None

    # update_description field is only set in log-based replication mode. It should be defined here as None.
    assert "update_description" in record
    assert record["cluster_time"] is None

    # namespace field should be defined and should match the expected database and collection name
    assert "namespace" in record
    expected_namespace: dict[str, str] = {
        "database": database_name,
        "collection": collection_name,
    }
    assert expected_namespace == record["namespace"]

    # to field is only set in log-based replication mode. It should be defined here as None.
    assert "to" in record
    assert record["to"] is None

    # logger.info("record:")
    # print_json(data=record)

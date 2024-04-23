import os
import re
import pytest
from pytest import FixtureRequest
from testcontainers.mongodb import MongoDbContainer
from pymongo import MongoClient
from loguru import logger

mongodb_version: str = os.environ.get("TEST_MONGODB_VERSION", "4.4")
mongo_image_tag: str = f"mongo:{mongodb_version}"

mongodb: MongoDbContainer = MongoDbContainer(mongo_image_tag)    


def is_test_mongodb_connection_string(connection_string: str) -> bool:
    """Returns True if the connection string matches the expected test format, False otherwise.
    
    The intent is to guard against accidentally running tests against a non-testing database."""
    pattern: re.Pattern = re.compile(r"mongodb://test:test@localhost:\d+")
    return bool(pattern.match(connection_string))


@pytest.fixture(scope="module", autouse=True)
def setup(request: FixtureRequest) -> None:
    """Setup the MongoDB container."""
    mongodb.start()

    def remove_container():
        mongodb.stop()

    request.addfinalizer(remove_container)
    os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"] = mongodb.get_connection_url()
    os.environ["TAP_MONGODB_DATABASE"] = mongodb.MONGO_DB


@pytest.fixture(scope="function", autouse=True)
def clear_mongodb_data() -> None:
    mongo_connection_string: str = os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"]
    mongo_client: MongoClient = MongoClient(mongo_connection_string)
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    if is_test_mongodb_connection_string(mongo_connection_string):
        logger.info(f"Clearing data from test database: {database_name}")
        mongo_client.drop_database(database_name)
    else:
        raise ValueError(f"NOT clearing data! Connection string appears to be for a non-testing database.")


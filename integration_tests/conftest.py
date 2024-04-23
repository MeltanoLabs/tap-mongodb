"""Pytest configuration for the integration_tests package.

The integration_tests package leverages the testcontainers library to spin up a MongoDB container for testing purposes.
Setup and teardown of that container are managed in this module.
"""

import os
import re

import pytest
from loguru import logger
from pymongo import MongoClient
from pytest import FixtureRequest
from testcontainers.mongodb import MongoDbContainer

mongodb_version: str = os.environ.get("TEST_MONGODB_VERSION", "4.4")
mongo_image_tag: str = f"mongo:{mongodb_version}"

test_username: str = "test_mongodb_username"
test_password: str = "test_mongodb_password"
test_database: str = "test_mongodb_database"


mongodb: MongoDbContainer = MongoDbContainer(
    mongo_image_tag,
    username=test_username,
    password=test_password,
    dbname=test_database,
)


def is_test_mongodb_connection_string(connection_string: str) -> bool:
    """Returns True if the connection string matches the expected test format, False otherwise.

    The intent is to guard against accidentally running tests against a non-testing database.

    Note that this specifically checks for username and password "test" and the localhost hostname. These are set
    by the MongoDB testcontainer and don't seem to be possible to override on the current version.

    Args:
        connection_string (str): MongDB connection string

    Returns:
        bool: True if the provided connection string matches the expected test format, False otherwise
    """
    pattern: re.Pattern = re.compile(r"mongodb://test_mongodb_username:test_mongodb_password@localhost:\d+")
    return bool(pattern.match(connection_string))


@pytest.fixture(scope="module", autouse=True)
def setup(request: FixtureRequest) -> None:
    """Setup the MongoDB container."""

    mongodb.start()

    def remove_container():
        """Define container shutdown method in a function that we can register as a Pytest fixture finalizer."""
        mongodb.stop()

    request.addfinalizer(remove_container)

    # make some properties of the MongoDB container available as environment variables (for the tap to use)
    os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"] = mongodb.get_connection_url()
    os.environ["TAP_MONGODB_DATABASE"] = mongodb.dbname


@pytest.fixture(scope="function", autouse=True)
def clear_mongodb_data() -> None:
    """Clear data from the MongoDB test database (by dropping the database used in tests)

    Raises:
        ValueError: if the MongoDB connection string does not match the expected test format
    """
    mongo_connection_string: str = os.environ["TAP_MONGODB_MONGODB_CONNECTION_STRING"]
    mongo_client: MongoClient = MongoClient(mongo_connection_string)
    database_name: str = os.environ["TAP_MONGODB_DATABASE"]

    if is_test_mongodb_connection_string(mongo_connection_string):
        logger.info(f"Clearing data from test database: {database_name}")
        mongo_client.drop_database(database_name)
    else:
        error_message: str = "NOT clearing data! Connection string appears to be for a non-testing database."
        logger.error(error_message)
        raise ValueError(error_message)

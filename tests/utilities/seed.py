"""Utility module that tests may use to seed a MongoDB database in order to test tap-mongodb behavior."""


import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult


@dataclass
class User:  # pylint: disable=too-many-instance-attributes
    """Generic User model containing members with a mix of data types."""

    first_name: str
    last_name: str
    age: int
    created_at: datetime = datetime.now(timezone.utc)
    updated_at: datetime = datetime.now(timezone.utc)
    deleted: bool = False
    user_id: str = str(uuid4())
    hobbies: List[str] = field(default_factory=list)


def insert_user(collection: Collection, user: User) -> User:
    """Insert the provided User record into the provided Collection."""
    now: datetime = datetime.now(timezone.utc)
    user.created_at = now
    user.updated_at = now
    result: InsertOneResult = collection.insert_one(asdict(user))
    logger.info(f"Inserted User {user.id}, resulting _id {result.inserted_id}")
    return user


def update_user(collection: Collection, user_id: str, user: User) -> User:  # pylint: disable=unused-argument
    """Update the provided User record in the provided Collection."""


def get_user(collection: Collection, user_id: str) -> Optional[User]:  # pylint: disable=unused-argument
    """Return one User by id from the provided Collection."""


def delete_user(collection: Collection, user_id: str) -> User:  # pylint: disable=unused-argument
    """Delete (soft-delete) one User by id from the provided Collection."""


def get_collection() -> Collection:
    """Returns a MongoDB Collection based on configured settings."""
    connection_string: str = os.getenv("TAP_MONGODB_TEST_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("connection_string must be provided")
    database_name: str = "test_db"
    collection_name: str = "User"
    client: MongoClient = MongoClient(connection_string)
    database: Database = client.get_database(database_name)
    collection: Collection = database.get_collection(collection_name)
    return collection


def main() -> None:
    """Run a series of seed operations on a MongoDB database.."""
    collection: Collection = get_collection()
    user: User = User(first_name="Ted", last_name="Lasso", age=48, hobbies=["football"])
    insert_user(collection, user)


if __name__ == "__main__":
    main()

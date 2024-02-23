"""Tests of tap-mongodb custom types."""

from datetime import datetime

from bson.objectid import ObjectId

from tap_mongodb.types import IncrementalId


def test_from_str_date_only():
    """Test that the expected ObjectId is produced for an input string in YYYY-MM-DD format."""
    id_string = "2021-09-22"
    assert IncrementalId.from_string(id_string).object_id == ObjectId("614a72000000000000000000")


def test_from_str_datetime_only():
    """Test that the expected ObjectId is produced for an input string in YYYY-MM-DDThh:mm:ss+00:00 format."""
    id_string = "2021-09-22T01:02:48+00:00"
    assert IncrementalId.from_string(id_string).object_id == ObjectId("614a80b80000000000000000")


def test_from_str_datetime_and_object_id():
    """Test that the expected ObjectId is produced for an input string containing both a datetime and an ObjectId."""
    id_string = "2021-09-22T01:02:48+00:00|614a80b81ad8c60001b7d5f3"
    assert IncrementalId.from_string(id_string).object_id == ObjectId("614a80b81ad8c60001b7d5f3")


def test_to_string_datetime_only():
    """Test that a deserialization followed by a serialization returns the original string value."""
    dt_string = datetime.fromisoformat("2021-09-22T01:02:48+00:00")
    id_string = "2021-09-22T01:02:48+00:00"
    assert str(IncrementalId(dt_string)) == id_string


def test_to_string_datetime_and_object_id():
    """Test that a deserialization followed by a serialization returns the original string value."""
    dt_string = datetime.fromisoformat("2021-09-22T01:02:48+00:00")
    oid_string = "614a80b81ad8c60001b7d5f3"
    id_string = "2021-09-22T01:02:48+00:00|614a80b81ad8c60001b7d5f3"
    assert str(IncrementalId(dt_string, oid_string)) == id_string

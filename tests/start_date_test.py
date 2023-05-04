"""Test for the start date/ObjectId behavior."""

from bson.objectid import ObjectId

from tap_mongodb.streams import to_object_id


def test_to_object_id():
    """The default start date should be parsed correctly into the expected ObjectId value."""
    assert to_object_id("1970-01-01") == ObjectId("000000000000000000000000")

"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_mongodb.tap import TapMongoDB

SAMPLE_CONFIG = {
    "mongodb_connection_string": "mongodb://admin:password@localhost:27017/",
    "database": "test",
}

# Run standard built-in tap tests from the SDK:
TestTapMongoDB = get_tap_test_class(tap_class=TapMongoDB, config=SAMPLE_CONFIG)

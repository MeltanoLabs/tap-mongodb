"""Tests of the utils module."""

import pytest

from tap_mongodb.types import MongoVersion, ResumeStrategy
from tap_mongodb.utils import get_resume_strategy


def test_that_invalid_setting_throws() -> None:
    """Test that an invalid change_stream_resume_strategy value throws the expected exception."""
    mongo_version: MongoVersion = (4, 0)
    change_stream_resume_strategy: str = "fake_value"
    with pytest.raises(ValueError):
        get_resume_strategy(mongo_version, change_stream_resume_strategy)


def test_that_resume_after_is_returned_on_mongo_3_6_if_start_after_is_set() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 even though start_after was configured"""
    mongo_version: MongoVersion = (3, 6)
    change_stream_resume_strategy: str = "start_after"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.RESUME_AFTER


def test_that_resume_after_is_returned_on_mongo_3_6_if_start_at_operation_time_is_set() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 even though start_at_operation_time was configured"""
    mongo_version: MongoVersion = (3, 6)
    change_stream_resume_strategy: str = "start_at_operation_time"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.RESUME_AFTER


def test_that_resume_after_is_returned_on_mongo_3_6() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 when resume_after is configured"""
    mongo_version: MongoVersion = (3, 6)
    change_stream_resume_strategy: str = "resume_after"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.RESUME_AFTER


def test_that_resume_after_is_allowed_on_mongo_4_0() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 when resume_after is configured"""
    mongo_version: MongoVersion = (4, 0)
    change_stream_resume_strategy: str = "resume_after"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.RESUME_AFTER


def test_that_resume_after_is_allowed_on_mongo_4_2() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 when resume_after is configured"""
    mongo_version: MongoVersion = (4, 2)
    change_stream_resume_strategy: str = "resume_after"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.RESUME_AFTER


def test_that_start_at_operation_time_is_allowed_on_mongo_4_0() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 when resume_after is configured"""
    mongo_version: MongoVersion = (4, 0)
    change_stream_resume_strategy: str = "start_at_operation_time"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.START_AT_OPERATION_TIME


def test_that_start_at_operation_time_is_allowed_on_mongo_4_2() -> None:
    """Test that resume_after is returned for MongoDB version 3.6 when resume_after is configured"""
    mongo_version: MongoVersion = (4, 2)
    change_stream_resume_strategy: str = "start_at_operation_time"
    assert get_resume_strategy(mongo_version, change_stream_resume_strategy) == ResumeStrategy.START_AT_OPERATION_TIME

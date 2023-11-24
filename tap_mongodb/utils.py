"""Utility module containing some functions that don't fit well in other modules."""


from bson.objectid import ObjectId

from tap_mongodb.types import IncrementalId, MongoVersion, ResumeStrategy


def get_resume_strategy(mongo_version: MongoVersion, change_stream_resume_strategy: str) -> ResumeStrategy:
    """Determine the appropriate ResumeStrategy based on MongoDB version and configured setting."""
    # validate that change_stream_resume_strategy is one of the allowed values
    if change_stream_resume_strategy not in {
        "resume_after",
        "start_after",
        "start_at_operation_time",
    }:
        raise ValueError("unsupported change_stream_resume_strategy setting")
    # validate that the mongodb version is at least 3.6
    if mongo_version < (3, 6):
        raise ValueError("unsupported version of MongoDB")
    if (4, 0) <= mongo_version and change_stream_resume_strategy == "start_at_operation_time":
        return ResumeStrategy.START_AT_OPERATION_TIME
    if (4, 2) <= mongo_version and change_stream_resume_strategy == "start_after":
        return ResumeStrategy.START_AFTER
    return ResumeStrategy.RESUME_AFTER


def to_object_id(replication_key_value: str) -> ObjectId:
    """Converts an ISO-8601 date string into a BSON ObjectId."""
    incremental_id: IncrementalId = IncrementalId.from_string(replication_key_value)

    return incremental_id.object_id

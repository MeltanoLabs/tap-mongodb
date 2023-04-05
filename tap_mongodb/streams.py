"""mongodb streams class."""

from __future__ import annotations

from os import PathLike
from typing import Iterable, Any
from datetime import datetime

from singer_sdk import PluginBase as TapBaseClass, _singerlib as singer
from singer_sdk.streams import Stream
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.collection import Collection
from pymongo import ASCENDING
from singer_sdk.streams.core import (
    TypeConformanceLevel,
    REPLICATION_LOG_BASED,
    REPLICATION_INCREMENTAL,
)
from singer_sdk.helpers._state import get_starting_replication_value
from singer_sdk._singerlib.utils import strptime_to_utc


class CollectionStream(Stream):
    """Stream class for mongodb streams."""

    # The output stream will always have _id as the primary key
    primary_keys = ["_id"]
    replication_key = "_id"

    # Disable timestamp replication keys. One caveat is this relies on an
    # alphanumerically sortable replication key. Python __gt__ and __lt__ are
    # used to compare the replication key values. This works for most cases.
    is_timestamp_replication_key = False

    # No conformance level is set by default since this is a generic stream
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        collection: Collection | None = None,
        max_await_time_ms: int | None = None,
    ) -> None:
        super().__init__(tap, schema, name)
        self._collection: Collection = collection
        self._max_await_time_ms = max_await_time_ms

    def get_starting_replication_key_value(
        self, context: dict | None
    ) -> ObjectId | str | None:
        state = self.get_context_state(context)
        replication_key_value = get_starting_replication_value(state)

        if self.replication_method == REPLICATION_INCREMENTAL:
            if replication_key_value:
                try:
                    return ObjectId(replication_key_value)
                except InvalidId:
                    self.logger.warning(
                        f"Replication key value {replication_key_value} cannot be parsed into ObjectId."
                    )
            start_date_str = self.config.get("start_date", "1970-01-01")
            start_date: datetime = strptime_to_utc(start_date_str)
            return ObjectId.from_datetime(start_date)
        elif self.replication_method == REPLICATION_LOG_BASED:
            return replication_key_value
        else:
            msg = f"Unrecognized replication strategy {self.replication_method}"
            self.logger.critical(msg)
            raise RuntimeError(msg)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        bookmark = self.get_starting_replication_key_value(context)

        if self.replication_method == REPLICATION_INCREMENTAL:
            if bookmark is None:
                for record in self._collection.find({}).sort([("_id", ASCENDING)]):
                    object_id: ObjectId = record["_id"]
                    yield {"_id": str(object_id), "document": record}
            elif bookmark is not None and isinstance(bookmark, ObjectId):
                for record in self._collection.find({"_id": {"$gt": bookmark}}).sort(
                    [("_id", ASCENDING)]
                ):
                    object_id: ObjectId = record["_id"]
                    yield {"_id": str(object_id), "document": record}

        elif self.replication_method == REPLICATION_LOG_BASED:
            if bookmark is None:
                with self._collection.watch(
                    full_document="updateLookup"
                ) as change_stream:
                    for record in change_stream:
                        self.logger.info(f"b change_stream record: {record}")
                        yield {
                            "_id": record["_id"]["_data"],
                            "document": record["fullDocument"],
                            "operationType": record["operationType"],
                            "clusterTime": int(
                                record["clusterTime"].as_datetime().timestamp()
                            ),
                            "ns": record["ns"],
                        }
            elif bookmark is not None and isinstance(bookmark, str):
                resume_token = {"_data": bookmark}
                with self._collection.watch(
                    full_document="updateLookup", resume_after=resume_token
                ) as change_stream:
                    for record in change_stream:
                        self.logger.info(f"b change_stream record: {record}")
                        yield {
                            "_id": record["_id"]["_data"],
                            "document": record["fullDocument"],
                            "operationType": record["operationType"],
                            "clusterTime": int(
                                record["clusterTime"].as_datetime().timestamp()
                            ),
                            "ns": record["ns"],
                        }
        else:
            msg = f"Unrecognized replication strategy {self.replication_method}"
            self.logger.critical(msg)
            raise RuntimeError(msg)

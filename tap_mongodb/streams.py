"""mongodb streams class."""

from __future__ import annotations

from os import PathLike
from typing import Iterable, Any
from datetime import datetime

import bson
from singer_sdk import PluginBase as TapBaseClass, _singerlib as singer
from singer_sdk.streams import Stream
from bson.objectid import ObjectId
from bson.errors import InvalidId
from pymongo.collection import Collection
from pymongo import ASCENDING
from singer_sdk.streams.core import (
    TypeConformanceLevel,
)
from singer_sdk.helpers._state import get_starting_replication_value


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
    ) -> ObjectId | str:
        state = self.get_context_state(context)
        self.logger.info(f"state: {state}")

        replication_key_value = get_starting_replication_value(state)
        if replication_key_value:
            try:
                return ObjectId(replication_key_value)
            except InvalidId as invalid_id_error:
                # TODO if it can't be parsed as an ObjectId, parse it as a BSON resume token
                self.logger.info(
                    f"Replication key value {replication_key_value} cannot be parsed into ObjectId: {invalid_id_error}"
                )
                return replication_key_value
        else:
            self.logger.info(f"None replication_key_value: {replication_key_value}")
            zero_time = datetime(1970, 1, 1)
            return ObjectId.from_datetime(zero_time)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        bookmark = self.get_starting_replication_key_value(context)
        change_stream_opts: dict = {"full_document": "updateLookup"}
        if self._max_await_time_ms is not None:
            change_stream_opts["max_await_time_ms"] = self._max_await_time_ms
        self.logger.info(f"change_stream_opts: {change_stream_opts}")
        if bookmark is None:
            for record in self._collection.find({}).sort([("_id", ASCENDING)]):
                object_id: ObjectId = record["_id"]
                yield {"_id": str(object_id), "document": record}
            with self._collection.watch(**change_stream_opts) as change_stream:
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
        elif bookmark is not None and isinstance(bookmark, ObjectId):
            for record in self._collection.find({"_id": {"$gt": bookmark}}).sort(
                [("_id", ASCENDING)]
            ):
                object_id: ObjectId = record["_id"]
                yield {"_id": str(object_id), "document": record}
            with self._collection.watch(**change_stream_opts) as change_stream:
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
            resume_token = bson.encode({"_data": bookmark})
            change_stream_opts["resume_after"] = resume_token
            with self._collection.watch(**change_stream_opts) as change_stream:
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
            self.logger.info(f"d: {bookmark}")
            self.logger.info(f"else bookmark: {bookmark}")
            self.logger.info(f"else type(bookmark): {type(bookmark)}")

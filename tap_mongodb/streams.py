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
    ) -> None:
        super().__init__(tap, schema, name)
        self._collection: Collection = collection

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
        self.logger.info(f"bookmark: {bookmark}")
        if bookmark is None:
            self.logger.info(f"a: {bookmark}")
            for record in self._collection.find({}).sort([("_id", ASCENDING)]):
                object_id: ObjectId = record["_id"]
                self.logger.info(f"record: {record}")
                self.logger.info(f"object_id: {object_id}")
                self.logger.info(f"str(object_id): {str(object_id)}")
                self.logger.info("a before yield")
                yield {"_id": str(object_id), "document": record}
                self.logger.info("a after yield")
                self.logger.info("a: we've iterated through the entire collection now")
            self.logger.info("a after for loop")
            with self._collection.watch() as change_stream:
                self.logger.info("a: opened change stream")
                for record in change_stream:
                    self.logger.info("a: got record in change stream")
                    self.logger.info(f"a change_stream record: {record}")
        elif bookmark is not None and isinstance(bookmark, ObjectId):
            self.logger.info(f"b: {bookmark}")
            # TODO how to turn "bookmark" into an object ID _id that can be compared against?
            for record in self._collection.find({"_id": {"$gt": bookmark}}).sort(
                [("_id", ASCENDING)]
            ):
                object_id: ObjectId = record["_id"]
                self.logger.info(f"record: {record}")
                self.logger.info(f"object_id: {object_id}")
                self.logger.info(f"str(object_id): {str(object_id)}")
                self.logger.info("a after yield")
                yield {"_id": str(object_id), "document": record}
                self.logger.info("b after yield")
                self.logger.info("b: we've iterated through the entire collection now")
            self.logger.info("b after for loop")
            with self._collection.watch(full_document="updateLookup") as change_stream:
                self.logger.info("b: opened change stream")
                for record in change_stream:
                    self.logger.info("b: got record in change stream")
                    self.logger.info(f"b change_stream record: {record}")
        elif bookmark is not None:
            self.logger.info(f"c: {bookmark}")
            self.logger.info(f"type(bookmark): {type(bookmark)}")
        else:
            self.logger.info(f"d: {bookmark}")
            self.logger.info(f"else bookmark: {bookmark}")
            self.logger.info(f"else type(bookmark): {type(bookmark)}")

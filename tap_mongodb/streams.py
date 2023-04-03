"""mongodb streams class."""

from __future__ import annotations

from os import PathLike
from typing import Iterable, Any

from singer_sdk import PluginBase as TapBaseClass, _singerlib as singer
from singer_sdk.streams import Stream
from pymongo.collection import Collection
from pymongo import ASCENDING
from singer_sdk.streams.core import (
    TypeConformanceLevel,
)


class CollectionStream(Stream):
    """Stream class for mongodb streams."""

    # The output stream will always have _id as the primary key
    primary_keys = ["_id"]

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

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        bookmark = self.get_starting_replication_key_value(context)
        self.logger.info(f"collection name: {self._collection.name}")
        if bookmark is None:
            for record in self._collection.find({}).sort([("_id", ASCENDING)]):
                self.logger.info(f"record: {record}")
                yield {"_id": record["_id"], "document": record}

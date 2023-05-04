"""mongodb streams class."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generator, Iterable

from bson.errors import InvalidId
from bson.objectid import ObjectId
from pendulum import DateTime
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import OperationFailure
from singer_sdk import PluginBase as TapBaseClass
from singer_sdk import _singerlib as singer
from singer_sdk._singerlib.utils import strptime_to_utc
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams.core import (
    REPLICATION_INCREMENTAL,
    REPLICATION_LOG_BASED,
    Stream,
    TypeConformanceLevel,
)

from tap_mongodb.connector import MongoDBConnector


def to_object_id(start_date_str: str) -> ObjectId:
    """Converts an ISO-8601 date string into a BSON ObjectId."""
    start_date_dt: datetime = strptime_to_utc(start_date_str)
    return ObjectId.from_datetime(start_date_dt)


class MongoDBCollectionStream(Stream):
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
        catalog_entry: dict,
        connector: MongoDBConnector,
    ) -> None:
        """Initialize the database stream.

        If `connector` is omitted, a new connector will be created.

        Args:
            tap: The parent tap object.
            catalog_entry: Catalog entry dict.
            connector: Connector to reuse.
        """
        self._connector: MongoDBConnector = connector
        self.catalog_entry = catalog_entry
        self._collection_name: str = self.catalog_entry["table_name"]
        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["tap_stream_id"],
        )

    def _increment_stream_state(
        self, latest_record: dict[str, Any], *, context: dict | None = None
    ) -> None:
        """Update state of stream or partition with data from the provided record.

        Raises `InvalidStreamSortException` is `self.is_sorted = True` and unsorted data
        is detected.

        Args:
            latest_record: TODO
            context: Stream partition or context dictionary.

        Raises:
            ValueError: if configured replication method is unsupported, or if replication key is absent
        """
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if self.replication_method not in {
            REPLICATION_INCREMENTAL,
            REPLICATION_LOG_BASED,
        }:
            msg = (
                f"Unrecognized replication method {self.replication_method}. Only {REPLICATION_INCREMENTAL} and"
                f" {REPLICATION_LOG_BASED} replication methods are supported."
            )
            self.logger.critical(msg)
            raise ValueError(msg)

        if not self.replication_key:
            raise ValueError(
                f"Could not detect replication key for '{self.name}' stream"
                f"(replication method={self.replication_method})",
            )
        treat_as_sorted = self.is_sorted
        if not treat_as_sorted and self.state_partitioning_keys is not None:
            # Streams with custom state partitioning are not resumable.
            treat_as_sorted = False
        increment_state(
            state_dict,
            replication_key=self.replication_key,
            latest_record=latest_record,
            is_sorted=treat_as_sorted,
            check_sorted=self.check_sorted,
        )

    def _generate_record_messages(
        self, record: dict
    ) -> Generator[singer.RecordMessage, None, None]:
        """Write out a RECORD message.

        We are overriding the default implementation of this (private) method because the default behavior is to set
        time_extracted to utc_now() and we do not want that behavior in all scenarios. If we are processing records
        from a MongoDB change stream, we want time_extracted to be set to the cluster_time value on that change stream
        event, even if it's in the past relative to "now".

        Args:
            record: A single stream record.

        Yields:
            Record message objects.
        """
        extracted_at: DateTime = record.pop("_sdc_extracted_at", utc_now())
        pop_deselected_record_properties(record, self.schema, self.mask, self.logger)
        record = conform_record_data_types(
            stream_name=self.name,
            record=record,
            schema=self.schema,
            level=self.TYPE_CONFORMANCE_LEVEL,
            logger=self.logger,
        )
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                record_message = singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=extracted_at,
                )

                yield record_message

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        bookmark: str = self.get_starting_replication_key_value(context)

        should_add_metadata: bool = self.config.get("add_record_metadata", False)

        collection: Collection = self._connector.database[self._collection_name]

        if self.replication_method == REPLICATION_INCREMENTAL:
            if bookmark:
                try:
                    start_date = ObjectId(bookmark)
                except InvalidId:
                    self.logger.warning(
                        f"Replication key value {bookmark} cannot be parsed into ObjectId, falling back to default."
                    )
                    start_date_str = self.config.get("start_date", "1970-01-01")
                    self.logger.debug(f"using start_date_str: {start_date_str}")
                    start_date = to_object_id(start_date_str)
            else:
                start_date_str = self.config.get("start_date", "1970-01-01")
                self.logger.debug(f"using start_date_str: {start_date_str}")
                start_date = to_object_id(start_date_str)

            for record in collection.find({"_id": {"$gt": start_date}}).sort(
                [("_id", ASCENDING)]
            ):
                object_id: ObjectId = record["_id"]
                parsed_record = {
                    "_id": str(object_id),
                    "document": record,
                }
                if should_add_metadata:
                    parsed_record["_sdc_batched_at"] = datetime.utcnow()
                yield parsed_record

        elif self.replication_method == REPLICATION_LOG_BASED:
            change_stream_options = {"full_document": "updateLookup"}
            if bookmark is not None:
                change_stream_options["resume_after"] = {"_data": bookmark}
            operation_types_allowlist: set = set(self.config.get("operation_types"))
            has_seen_a_record: bool = False
            keep_open: bool = True

            try:
                change_stream = collection.watch(**change_stream_options)
            except OperationFailure as operation_failure:
                if (
                    operation_failure.code == 136
                    and "modifyChangeStreams has not been run"
                    in operation_failure.details["errmsg"]
                    and self.config["allow_modify_change_streams"]
                ):
                    admin_db: Database = self._connector.mongo_client["admin"]
                    result = admin_db.command(
                        "modifyChangeStreams",
                        database=collection.database.name,
                        collection=collection.name,
                        enable=True,
                    )
                    if result and result["ok"]:
                        change_stream = collection.watch(**change_stream_options)
                    else:
                        raise RuntimeError(
                            f"Unable to enable change streams on collection {collection.name}"
                        ) from operation_failure
                else:
                    raise operation_failure
            except Exception as exception:
                self.logger.critical(exception)
                raise exception

            with change_stream:
                while change_stream.alive and keep_open:
                    record = change_stream.try_next()
                    # if we have processed any records, a None record means that we've caught up to the end of the
                    # stream - set keep_open to False so that the change stream is closed and the tap exits.
                    # if no records have been processed, a None record means that there has been no activity in the
                    # collection since the change stream was opened. MongoDB and DocumentDB have different behavior here
                    # (MongoDB change streams have a valid/resumable resume_token immediately, while DocumentDB change
                    # streams have a None resume_token until there has been an event published to the change stream).
                    # The intent of the following code is the following:
                    #  - If a change stream is opened and there are no records, hold it open until a record appears,
                    #    then yield that record (whose _id is set to the change stream's resume token, so that the
                    #    change stream can be resumed from this point by a later running of the tap).
                    #  - If a change stream is opened and there is at least one record, yield all records
                    if record is None and has_seen_a_record:
                        keep_open = False
                    if record is not None:
                        operation_type = record["operationType"]
                        if operation_type not in operation_types_allowlist:
                            continue
                        cluster_time: datetime = record["clusterTime"].as_datetime()
                        parsed_record = {
                            "_id": record["_id"]["_data"],
                            "document": record["fullDocument"],
                            "operationType": operation_type,
                            "clusterTime": cluster_time.isoformat(),
                            "ns": record["ns"],
                        }
                        if should_add_metadata:
                            parsed_record["_sdc_extracted_at"] = cluster_time
                            parsed_record["_sdc_batched_at"] = datetime.utcnow()
                            if operation_type == "delete":
                                parsed_record["_sdc_deleted_at"] = cluster_time
                        yield parsed_record
                        has_seen_a_record = True

        else:
            msg = (
                f"Unrecognized replication method {self.replication_method}. Only {REPLICATION_INCREMENTAL} and"
                f" {REPLICATION_LOG_BASED} replication methods are supported."
            )
            self.logger.critical(msg)
            raise ValueError(msg)

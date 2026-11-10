"""mongodb streams class."""

from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from bson.objectid import ObjectId
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
from singer_sdk import Tap
from singer_sdk import singerlib as singer
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED, Stream, TypeConformanceLevel

from tap_mongodb.arrow import ArrowExtractor
from tap_mongodb.flat import DeclaredField, declared_fields, flatten_document
from tap_mongodb.types import IncrementalId

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator, Mapping

    import pyarrow as pa
    from bson.timestamp import Timestamp
    from pymongo.collection import Collection
    from pymongo.database import Database
    from pymongo.typings import _DocumentType
    from singer_sdk.helpers._batch import BatchConfig

    from tap_mongodb.connector import MongoDBConnector

DEFAULT_START_DATE: str = "1970-01-01"
DEFAULT_ARROW_BATCH_SIZE: int = 100_000


def recursive_replace_empty_in_dict(dct):
    """
    Recursively replace empty values with None in a dictionary.
    NaN, inf, and -inf are unable to be parsed by the json library, so these values will be replaced with None.
    """
    for key, value in dct.items():
        if value in [-math.inf, math.inf, math.nan]:
            dct[key] = None
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    recursive_replace_empty_in_dict(item)
                elif item in [-math.inf, math.inf, math.nan]:
                    value[i] = None
        elif isinstance(value, dict):
            recursive_replace_empty_in_dict(value)
    return dct


def to_object_id(replication_key_value: str) -> ObjectId:
    """Converts an ISO-8601 date string into a BSON ObjectId."""
    incremental_id: IncrementalId = IncrementalId.from_string(replication_key_value)

    return incremental_id.object_id


class MongoDBCollectionStream(Stream):
    """Stream class for mongodb streams."""

    replication_key = "replication_key"

    # Disable timestamp replication keys. One caveat is this relies on an
    # alphanumerically sortable replication key. Python __gt__ and __lt__ are
    # used to compare the replication key values. This works for most cases.
    is_timestamp_replication_key = False

    # No conformance level is set by default since this is a generic stream
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    def __init__(
        self,
        tap: Tap,
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
        # A catalog discovered before multiple databases were supported has no database name.
        self._database_name: str = self.catalog_entry.get("database_name") or connector.default_database_name()
        self._collection_name: str = self.catalog_entry["table_name"]
        self._declared: list[DeclaredField] = []
        self._deselected: list[str] = []
        self._warned_undeclared: set[str] = set()
        super().__init__(
            tap=tap,
            schema=self.catalog_entry["schema"],
            name=self.catalog_entry["tap_stream_id"],
        )

    @property
    def is_flat_layout(self) -> bool:
        """Whether each declared document field is its own column (see `tap_mongodb.flat`)."""
        return self.config.get("document_layout") == "flat"

    def _selected_declared_fields(self) -> tuple[list[DeclaredField], list[str]]:
        """The document fields declared in the stream schema, for the flat document layout.

        Returns:
            The selected declared fields, and the names of the deselected ones.
        """
        fields = declared_fields(self.schema.get("properties", {}), stream_name=self.name)
        selected = [declared for declared in fields if self.mask["properties", declared.name]]
        deselected = [declared.name for declared in fields if not self.mask["properties", declared.name]]
        if not selected:
            msg = f"Stream '{self.name}' uses the flat document layout, but all of its declared fields are deselected."
            raise ValueError(msg)
        return selected, deselected

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """With the flat document layout, replace `document` with its declared fields' values."""
        if not self.is_flat_layout:
            return row

        values, undeclared = flatten_document(
            row.pop("document", None),
            self._declared,
            stream_name=self.name,
            deselected=self._deselected,
        )
        if new := undeclared - self._warned_undeclared:
            self._warned_undeclared |= new
            self.logger.warning(
                "Stream '%s' uses the flat document layout, so these undeclared document fields are dropped: %s",
                self.name,
                ", ".join(sorted(new)),
            )
        row.update(values)
        return row

    @property
    def is_sorted(self) -> bool:
        """Return a boolean indicating whether the replication key is alphanumerically sortable.

        When the tap is running in incremental mode, it is sorted - the replication key value is an ISO-8601-formatted
        string, and these are alphanumerically sortable.

        When the tap is running in log-based mode, it is not sorted - the replication key value here is a hex string."""

        return self.replication_method == REPLICATION_INCREMENTAL

    def _increment_stream_state(self, latest_record: dict[str, Any], *, context: dict | None = None) -> None:
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
            msg = (
                f"Could not detect replication key for '{self.name}' stream"
                f"(replication method={self.replication_method})"
            )
            raise ValueError(msg)
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

    def _generate_record_messages(self, record: dict) -> Generator[singer.RecordMessage, None, None]:
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
        extracted_at: datetime = record.pop("_sdc_extracted_at", utc_now())
        pop_deselected_record_properties(record, self.schema, self.mask)
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

    def get_batch_config(self, config: Mapping) -> BatchConfig | None:
        """Return the batch config, or None to emit RECORD messages.

        With the Arrow format, a LOG_BASED stream writes Arrow files only for the documents it reads when it has no
        bookmark. Its change events emit RECORD messages: they arrive one at a time and are already small. The default
        `batch_size` for Arrow is raised to 100,000 rows per file.
        """
        raw = config.get("batch_config") or {}
        if (raw.get("encoding") or {}).get("format") == "arrow":
            reads_documents = self.replication_method == REPLICATION_INCREMENTAL or (
                self.replication_method == REPLICATION_LOG_BASED
                and "replication_key_value" not in self.get_context_state(None)
            )
            if not reads_documents:
                return None
            if "batch_size" not in raw:
                config = {**config, "batch_config": {**raw, "batch_size": DEFAULT_ARROW_BATCH_SIZE}}
        return super().get_batch_config(config)

    def _check_object_ids(self, collection: Collection) -> None:
        """Fail if an `_id` isn't an ObjectId, as incremental replication can't extract that document."""
        # `_id` values sort by BSON type first, so every `_id` is an ObjectId when the lowest and highest are.
        for direction in (ASCENDING, DESCENDING):
            document = collection.find_one({}, {"_id": 1}, sort=[("_id", direction)])
            if document is not None and not isinstance(document["_id"], ObjectId):
                msg = (
                    f"Incremental replication of stream '{self.name}' requires every `_id` to be an ObjectId, but "
                    f"the collection has a `{type(document['_id']).__name__}` `_id`. Use LOG_BASED replication instead."
                )
                raise TypeError(msg)

    def get_record_batches(self, context: dict | None) -> Iterable[pa.Table] | None:
        """Return the stream's documents as Arrow tables, for Arrow BATCH files.

        Incremental replication and a LOG_BASED stream with no bookmark produce Arrow data; see `tap_mongodb.arrow`
        for how documents are converted.
        """
        if self.replication_method not in {REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED}:
            return None

        collection: Collection = self._connector.mongo_client[self._database_name][self._collection_name]
        extractor = ArrowExtractor(
            collection=collection,
            stream_name=self.name,
            logger=self.logger,
            add_record_metadata=self.config.get("add_record_metadata", False),
        )
        if self.is_flat_layout:
            extractor.declared, extractor.deselected = self._selected_declared_fields()
        if self.replication_method == REPLICATION_LOG_BASED:
            return self._extract_all_tables(collection, extractor, context)

        bookmark: str | None = self.get_starting_replication_key_value(context)
        start = to_object_id(bookmark or self.config.get("start_date", DEFAULT_START_DATE))
        self._check_object_ids(collection)
        return extractor.tables(start)

    def _extract_all_tables(
        self, collection: Collection, extractor: ArrowExtractor, context: dict | None
    ) -> Iterator[pa.Table]:
        """Yield every document as Arrow tables, then store a resume token from before the read as the bookmark.

        The sync ends with the documents. The next one reads the changes made since the token as RECORD messages,
        which can repeat a change that the read already saw.
        """
        with collection.watch() as change_stream:
            token = change_stream.resume_token
        if token is None:
            msg = (
                f"Arrow batches for LOG_BASED stream '{self.name}' need a resume token before the change stream's "
                "first event, which DocumentDB doesn't report. Use RECORD messages instead."
            )
            raise RuntimeError(msg)
        start = collection.database.command("ping")["operationTime"]
        yield from extractor.all_tables(start.as_datetime())
        # Only after the last document, so an interrupted sync reads them all again
        self._increment_stream_state({"replication_key": token["_data"]}, context=context)

    def _extract_all(self, collection: Collection, *, should_add_metadata: bool) -> Generator[dict, None, Timestamp]:
        """Yield every document in the collection, and return the cluster time from before the first one was read.

        A change stream opened at that time sees every change made while the documents are read.
        """
        with self._connector.mongo_client.start_session() as session:
            collection.database.command("ping", session=session)
            start = session.operation_time
            if start is None:
                msg = "LOG_BASED replication requires a replica set or a sharded cluster, which report a cluster time."
                raise RuntimeError(msg)
            cluster_time: str = start.as_datetime().isoformat()
            # A read in the same causally consistent session waits until the server has applied `start`, so a lagging
            # secondary can't return documents older than the change stream's start.
            for document in collection.find({}, session=session):
                recursive_replace_empty_in_dict(document)
                record = {
                    # No replication key, so a document never becomes the bookmark
                    "replication_key": None,
                    "object_id": str(document["_id"]),
                    "document": document,
                    "operation_type": None,
                    "cluster_time": cluster_time,
                    "namespace": {
                        "database": collection.database.name,
                        "collection": collection.name,
                    },
                }
                if should_add_metadata:
                    record["_sdc_batched_at"] = datetime.now(timezone.utc)
                yield record
        return start

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        if self.is_flat_layout:
            self._declared, self._deselected = self._selected_declared_fields()
        bookmark: str | None = self.get_starting_replication_key_value(context)
        should_add_metadata: bool = self.config.get("add_record_metadata", False)
        collection: Collection = self._connector.mongo_client[self._database_name][self._collection_name]
        self.logger.info(
            "replication method: %s, bookmark: %s, context: %s",
            self.replication_method,
            bookmark,
            context,
        )
        self.logger.info("state: %s", self.get_context_state(context))

        if self.replication_method == REPLICATION_INCREMENTAL:
            self._check_object_ids(collection)
            if bookmark:
                self.logger.debug("using existing bookmark: %s", bookmark)
                start_date = to_object_id(bookmark)
            else:
                start_date_str = self.config.get("start_date", DEFAULT_START_DATE)
                self.logger.debug("no bookmark - using start date: %s", start_date_str)
                start_date = to_object_id(start_date_str)

            for record in collection.find({"_id": {"$gt": start_date}}).sort([("_id", ASCENDING)]):
                object_id: ObjectId = record["_id"]
                incremental_id: IncrementalId = IncrementalId.from_object_id(object_id)

                recursive_replace_empty_in_dict(record)

                parsed_record = {
                    "replication_key": str(incremental_id),
                    "object_id": str(object_id),
                    "document": record,
                    "operation_type": None,
                    "cluster_time": None,
                    "namespace": {
                        "database": collection.database.name,
                        "collection": collection.name,
                    },
                }
                if should_add_metadata:
                    parsed_record["_sdc_batched_at"] = datetime.now(timezone.utc)
                yield parsed_record

        elif self.replication_method == REPLICATION_LOG_BASED:
            change_stream_options = {
                "full_document": "updateLookup",
                "pipeline": self.config.get("change_stream_pipeline", []),
            }
            if bookmark is not None and bookmark != DEFAULT_START_DATE:
                self.logger.info("using bookmark: %s", bookmark)
                # if on mongo version 4.2 or above, use start_after instead of resume_after, as the former will
                # gracefully open a new change stream if the resume token's event is not present in the oplog, while
                # the latter will error in that scenario.
                if self._connector.version and self._connector.version >= (4, 2):
                    change_stream_options["start_after"] = {"_data": bookmark}
                else:
                    change_stream_options["resume_after"] = {"_data": bookmark}
            else:
                self.logger.info("no bookmark - extracting every document before reading changes")
                change_stream_options["start_at_operation_time"] = yield from self._extract_all(
                    collection, should_add_metadata=should_add_metadata
                )
            operation_types_allowlist: set = set(self.config.get("operation_types"))
            has_seen_a_record: bool = False
            has_yielded_dummy_record: bool = False
            keep_open: bool = True
            idle_timeout_seconds: float = 10.0
            last_record_time: float = time.time()

            try:
                change_stream = collection.watch(**change_stream_options)
            except OperationFailure as operation_failure:
                if (
                    operation_failure.code == 136
                    and "modifyChangeStreams has not been run" in operation_failure.details["errmsg"]
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
                        msg = f"Unable to enable change streams on collection {collection.name}"
                        raise RuntimeError(msg) from operation_failure
                elif (
                    self._connector.version
                    and self._connector.version < (4, 2)
                    and operation_failure.code == 286
                    and "as the resume point may no longer be in the oplog." in operation_failure.details["errmsg"]
                ):
                    self.logger.warning("Unable to resume change stream from resume token. Resetting resume token.")
                    change_stream_options.pop("resume_after", None)
                    change_stream = collection.watch(**change_stream_options)
                else:
                    self.logger.critical("operation_failure on collection.watch: %s", operation_failure)
                    raise

            except Exception as exception:
                self.logger.critical(exception)
                raise

            with change_stream:
                self.logger.info(
                    "change_stream.alive: %s, keep_open: %s, resume_token: %s",
                    change_stream.alive,
                    keep_open,
                    change_stream.resume_token,
                )
                while change_stream.alive and keep_open:
                    # Check for idle timeout
                    current_time = time.time()
                    idle_duration = current_time - last_record_time
                    if idle_duration >= idle_timeout_seconds:
                        self.logger.info(
                            "Change stream idle for %.1f seconds (timeout: %.1f seconds). Ending stream.",
                            idle_duration,
                            idle_timeout_seconds,
                        )
                        keep_open = False
                        break

                    record: _DocumentType | None
                    try:
                        record = change_stream.try_next()
                        # Add small sleep to prevent CPU spinning while polling
                        if record is None:
                            time.sleep(0.1)
                    except OperationFailure as operation_failure:
                        if (
                            self._connector.version < (4, 2)
                            and operation_failure.code == 286
                            and "as the resume point may no longer be in the oplog."
                            in operation_failure.details["errmsg"]
                        ):
                            self.logger.warning("operation_failure on try_next: %s", operation_failure)
                            record = None
                        else:
                            self.logger.critical("operation_failure on try_next: %s", operation_failure)
                            raise
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
                    if (
                        record is None
                        and not has_seen_a_record
                        and not has_yielded_dummy_record
                        and change_stream.resume_token is not None
                    ):
                        # if we're in this block, we're in MongoDB specifically - DocumentDB will have a None resume
                        # token here. If we take no action, the tap will remain open and idle until a message appears
                        # in the change stream, then it will yield that record and close. That's not ideal because it
                        # doesn't need to wait around for activity. It can just store the change stream's resume token
                        # as the bookmark, without a record for the target, exit immediately, and then pick up
                        # processing the change stream from this point the next time the tap is run. So that's what we
                        # do.
                        self._increment_stream_state(
                            {"replication_key": change_stream.resume_token["_data"]}, context=context
                        )
                        has_yielded_dummy_record = True

                    if record is None and (has_yielded_dummy_record or has_seen_a_record):
                        keep_open = False
                    if record is not None:
                        # Reset idle timer when we receive a record
                        last_record_time = time.time()

                        operation_type = record["operationType"]
                        if operation_type not in operation_types_allowlist:
                            continue
                        cluster_time: datetime = record["clusterTime"].as_datetime()
                        # fullDocument key is not present on delete events - if it is missing, fall back to documentKey
                        # instead. If that is missing, pass None/null to avoid raising an error.
                        document = record.get("fullDocument", record.get("documentKey", None))
                        # A document deleted before the lookup has no `fullDocument`, but its event keeps the `_id`.
                        object_id: ObjectId | None = record["documentKey"]["_id"] if "documentKey" in record else None
                        parsed_record = {
                            "replication_key": record["_id"]["_data"],
                            "object_id": str(object_id) if object_id is not None else None,
                            "document": document,
                            "operation_type": operation_type,
                            "cluster_time": cluster_time.isoformat(),
                            "namespace": {
                                "database": record["ns"]["db"],
                                "collection": record["ns"]["coll"],
                            },
                        }
                        if should_add_metadata:
                            parsed_record["_sdc_extracted_at"] = cluster_time
                            parsed_record["_sdc_batched_at"] = datetime.now(timezone.utc)
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

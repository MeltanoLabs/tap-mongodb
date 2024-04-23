"""mongodb streams class."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Iterable, List, Optional, Union

from bson.objectid import ObjectId
from loguru import logger
from pendulum import DateTime
from pymongo import ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import OperationFailure
from pymongo.typings import _DocumentType
from singer_sdk import PluginBase as TapBaseClass
from singer_sdk import _singerlib as singer
from singer_sdk.helpers._catalog import pop_deselected_record_properties
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED, Stream, TypeConformanceLevel

from tap_mongodb.connector import MongoDBConnector
from tap_mongodb.types import IncrementalId, MongoVersion, ResumeStrategy
from tap_mongodb.utils import get_resume_strategy, to_object_id

DEFAULT_START_DATE: str = "1970-01-01"


def recursive_replace_empty_in_dict(dct: Dict) -> Dict:
    """
    Recursively replace empty values with None in a dictionary.
    NaN, inf, and -inf are unable to be parsed by the json library, so these values will be replaced with None.
    """
    for key, value in dct.items():
        if value in [-math.inf, math.inf, math.nan]:
            dct[key] = None
        elif isinstance(value, List):
            for i, item in enumerate(value):
                if isinstance(item, Dict):
                    recursive_replace_empty_in_dict(item)
                elif item in [-math.inf, math.inf, math.nan]:
                    value[i] = None
        elif isinstance(value, Dict):
            recursive_replace_empty_in_dict(value)
    return dct


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
        tap: TapBaseClass,
        catalog_entry: Dict,
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

    @property
    def primary_keys(self) -> Optional[List[str]]:
        """If running in log-based replication mode, use the Change Event ID as the primary key. If running instead in
        incremental replication mode, use the document's ObjectId."""
        if self.replication_method == REPLICATION_LOG_BASED:
            return ["replication_key"]
        return ["object_id"]

    @primary_keys.setter
    def primary_keys(self, new_value: List[str]) -> None:
        """Set primary keys for the stream."""
        self._primary_keys = new_value

    @property
    def is_sorted(self) -> bool:
        """Return a boolean indicating whether the replication key is alphanumerically sortable.

        When the tap is running in incremental mode, it is sorted - the replication key value is an ISO-8601-formatted
        string, and these are alphanumerically sortable.

        When the tap is running in log-based mode, it is not sorted - the replication key value here is a hex string."""

        return self.replication_method == REPLICATION_INCREMENTAL

    def _increment_stream_state(self, latest_record: Dict[str, Any], *, context: Optional[Dict] = None) -> None:
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
            unsupported_replication_method_msg: str = (
                f"Unrecognized replication method {self.replication_method}. Only {REPLICATION_INCREMENTAL} and"
                f" {REPLICATION_LOG_BASED} replication methods are supported."
            )
            logger.error(unsupported_replication_method_msg)
            raise ValueError(unsupported_replication_method_msg)

        if not self.replication_key:
            missing_replication_key_msg: str = (
                f"Could not detect replication key for '{self.name}' stream"
                f"(replication method={self.replication_method})"
            )
            logger.error(missing_replication_key_msg)
            raise ValueError(missing_replication_key_msg)

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

    def _generate_record_messages(self, record: Dict) -> Generator[singer.RecordMessage, None, None]:
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
                    stream=stream_map.stream_alias, record=mapped_record, version=None, time_extracted=extracted_at
                )
                yield record_message

    def _get_records_incremental(
        self, bookmark: str, should_add_metadata: bool, collection: Collection
    ) -> Iterable[Dict]:
        """Return a generator of record-type dictionary objects when running in incremental replication mode."""
        if bookmark:
            logger.info(f"using existing bookmark: {bookmark}")
            start_date = to_object_id(bookmark)
        else:
            start_date_str = self.config.get("start_date", DEFAULT_START_DATE)
            logger.info(f"no bookmark - using start date: {start_date_str}")
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
                "update_description": None,
                "namespace": {
                    "database": collection.database.name,
                    "collection": collection.name,
                },
                "to": None,
            }
            if should_add_metadata:
                parsed_record["_sdc_batched_at"] = datetime.now(timezone.utc)
            yield parsed_record

    def _get_records_log_based(
        self, bookmark: str, should_add_metadata: bool, collection: Collection
    ) -> Iterable[Dict]:
        """Return a generator of record-type dictionary objects when running in log-based replication mode."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        change_stream_options: Dict[str, Union[str, Dict[str, str]]] = {"full_document": "updateLookup"}
        mongo_version: MongoVersion = self._connector.version
        change_stream_resume_strategy: str = self.config.get("change_stream_resume_strategy", "resume_after")
        resume_strategy: ResumeStrategy = get_resume_strategy(mongo_version, change_stream_resume_strategy)

        if bookmark is not None and bookmark != DEFAULT_START_DATE:
            logger.info(f"bookmark is not None and not default start date. It is {bookmark}")
            if resume_strategy == ResumeStrategy.START_AFTER:
                logger.info(f"strategy is START_AFTER, bookmark is {bookmark}")
                change_stream_options["start_after"] = {"_data": bookmark}
            elif resume_strategy == ResumeStrategy.RESUME_AFTER:
                logger.info(f"strategy is RESUME_AFTER, bookmark is {bookmark}")
                change_stream_options["resume_after"] = {"_data": bookmark}
        operation_types_allowlist: set = set(self.config.get("operation_types"))
        has_seen_a_real_record: bool = False
        keep_open: bool = True

        try:
            change_stream = collection.watch(**change_stream_options)
            logger.info(f"Opened change stream on collection {collection.name}")
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
                    raise RuntimeError(
                        f"Unable to enable change streams on collection {collection.name}"
                    ) from operation_failure
            elif operation_failure.code == 286:
                logger.opt(exception=operation_failure).error(
                    f"Unable to resume (open) change stream on collection {collection.name} from resume token. "
                    "Resetting resume token."
                )
                change_stream_options.pop("resume_after", None)
                change_stream_options.pop("start_after", None)
                change_stream = collection.watch(**change_stream_options)
            else:
                logger.opt(exception=operation_failure).error(
                    f"OperationFailure on collection.watch() in collection {collection.name}"
                )
                raise operation_failure
        except Exception as exception:
            logger.opt(exception=exception).error(
                f"Unhandled exception on collection.watch() in collection {collection.name}"
            )
            raise exception

        while change_stream.alive and keep_open:
            record: Optional[_DocumentType]
            try:
                record = change_stream.try_next()
            except OperationFailure as operation_failure:
                if operation_failure.code == 286:
                    logger.opt(exception=operation_failure).error(
                        f"Unable to resume (try_next) change stream from resume token in collection {collection.name}. "
                        "Resetting resume token."
                    )
                    change_stream_options.pop("resume_after", None)
                    change_stream_options.pop("start_after", None)
                    change_stream = collection.watch(**change_stream_options)
                    record = None
                else:
                    logger.opt(exception=operation_failure).error(
                        f"OperationFailure on try_next in collection {collection.name}"
                    )
                    raise operation_failure
            except Exception as exception:
                logger.opt(exception=exception).error(f"Unhandled exception on try_next in {collection.name}")
                raise exception
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
            if record is None and not has_seen_a_real_record and change_stream.resume_token is not None:
                # if we're in this block, we're in MongoDB specifically - DocumentDB will have a None resume
                # token here. If we take no action, the tap will remain open and idle until a message appears
                # in the change stream, then it will yield that record and close. That's not ideal because it
                # doesn't need to wait around for activity. It can just yield a "dummy" record with the resume
                # token from the change stream, exit immediately, and then pick up processing the change stream
                # from this point the next time the tap is run. So that's what we do.
                resume_token = change_stream.resume_token["_data"]
                logger.info(
                    f"Yielding 'dummy' record for collection {collection.name} with resume token {resume_token}"
                )
                dummy_record: dict = {
                    "replication_key": change_stream.resume_token["_data"],
                    "object_id": None,
                    "document": None,
                    "update_description": None,
                    "operation_type": None,
                    "cluster_time": None,
                    "namespace": None,
                    "to": None,
                }
                if should_add_metadata:
                    now: datetime = datetime.now(timezone.utc)
                    dummy_record["_sdc_extracted_at"] = now
                    dummy_record["_sdc_batched_at"] = now

                yield dummy_record
                keep_open = False
                # has_seen_a_real_record = True

            if record is None and has_seen_a_real_record:
                logger.info("Reached the end of the change stream after consuming at least one record, closing it.")
                keep_open = False
            if record is not None:
                operation_type = record["operationType"]
                if operation_type not in operation_types_allowlist:
                    logger.warning(f"Skipping record of operationType {operation_type} which is not in allowlist")
                    continue
                cluster_time: datetime = record["clusterTime"].as_datetime()
                # fullDocument key is not present on delete events - if it is missing, fall back to documentKey
                # instead. If that is missing, pass None/null to avoid raising an error.
                # document: Dict = record.get("fullDocument", record.get("documentKey", None))
                document: Optional[Dict]
                if "fullDocument" in record:
                    document = record["fullDocument"]
                elif "documentKey" in record:
                    document = record["documentKey"]
                else:
                    document = None

                object_id: Optional[ObjectId] = document["_id"] if document and "_id" in document else None
                update_description: Optional[Dict] = None
                if "updateDescription" in record:
                    update_description = record["updateDescription"]
                to_obj: Optional[Dict] = None
                if "to" in record:
                    to_obj = {
                        "database": record["to"]["db"],
                        "collection": record["to"]["coll"],
                    }
                parsed_record = {
                    "replication_key": record["_id"]["_data"],
                    "object_id": str(object_id) if object_id is not None else None,
                    "document": document,
                    "update_description": update_description,
                    "operation_type": operation_type,
                    "cluster_time": cluster_time.isoformat(),
                    "namespace": {
                        "database": record["ns"]["db"],
                        "collection": record["ns"]["coll"],
                    },
                    "to": to_obj,
                }
                if should_add_metadata:
                    parsed_record["_sdc_extracted_at"] = cluster_time
                    parsed_record["_sdc_batched_at"] = datetime.now(timezone.utc)
                    if operation_type == "delete":
                        parsed_record["_sdc_deleted_at"] = cluster_time
                yield parsed_record
                has_seen_a_real_record = True

    def get_records(self, context: Dict | None) -> Iterable[Dict]:
        """Return a generator of record-type dictionary objects."""
        bookmark: str = self.get_starting_replication_key_value(context)
        should_add_metadata: bool = self.config.get("add_record_metadata", False)
        collection: Collection = self._connector.database[self._collection_name]

        if self.replication_method == REPLICATION_INCREMENTAL:
            yield from self._get_records_incremental(bookmark, should_add_metadata, collection)

        elif self.replication_method == REPLICATION_LOG_BASED:
            yield from self._get_records_log_based(bookmark, should_add_metadata, collection)

        else:
            msg = (
                f"Unrecognized replication method {self.replication_method}. Only {REPLICATION_INCREMENTAL} and"
                f" {REPLICATION_LOG_BASED} replication methods are supported."
            )
            logger.error(msg)
            raise ValueError(msg)

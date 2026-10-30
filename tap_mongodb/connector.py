"""MongoDB/DocumentDB connector utility"""

from functools import cached_property
from logging import Logger, getLogger
from typing import Any, TypeAlias

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import OperationFailure, PyMongoError
from singer_sdk.singerlib.catalog import CatalogEntry, MetadataMapping, Schema
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

from tap_mongodb.schema import SCHEMA

MongoVersion: TypeAlias = tuple[int, int]

SYSTEM_DATABASES = frozenset({"admin", "config", "local"})


class MongoDBConnector:  # pylint: disable=too-many-instance-attributes
    """MongoDB/DocumentDB connector class"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        connection_string: str,
        options: dict[str, Any],
        databases: list[str],
        datetime_conversion: str,
        prefix: str | None = None,
        collections: list[str] | None = None,
        add_record_metadata: bool = False,
        qualify_stream_names: bool = True,
    ) -> None:
        self._connection_string = connection_string
        self._options = options
        self._databases = databases
        self._datetime_conversion: str = datetime_conversion.upper()
        self._prefix: str | None = prefix
        self._collections = collections
        self._add_record_metadata = add_record_metadata
        self._qualify_stream_names = qualify_stream_names
        self._logger: Logger = getLogger(__name__)
        self._version: MongoVersion | None = None

    @cached_property
    def mongo_client(self) -> MongoClient:
        """Provide a MongoClient instance. Client is cached and reused."""
        client: MongoClient = MongoClient(
            self._connection_string, datetime_conversion=self._datetime_conversion, **self._options
        )
        try:
            server_info: dict[str, Any] = client.server_info()
            version_array: list[int] = server_info["versionArray"]
            self._version = (version_array[0], version_array[1])
        except Exception as exception:
            self._logger.exception("Could not connect to MongoDB")
            msg = "Could not connect to MongoDB"
            raise RuntimeError(msg) from exception
        return client

    def default_database_name(self) -> str:
        """Return the only selected database, for a catalog entry that names no database."""
        if self._qualify_stream_names or len(self._databases) != 1:
            msg = (
                "A catalog entry names no database. Run discovery again, "
                "or set `database` to the one it was discovered from."
            )
            raise ValueError(msg)
        return self._databases[0]

    @property
    def version(self) -> MongoVersion | None:
        """Returns the MongoVersion that is being used."""
        return self._version

    def get_fully_qualified_name(
        self,
        collection_name: str,
        prefix: str | None = None,
        delimiter: str = "_",
    ) -> str:
        """Concatenates a fully qualified name from the parts."""
        parts = []

        if prefix:
            parts.append(prefix)

        parts.append(collection_name)

        return delimiter.join(parts).lower()

    def _get_schema(self) -> dict[str, Any]:
        """Return the stream schema, with `_sdc_*` properties only if record metadata is enabled.

        Targets that load BATCH files (e.g. target-postgres) compare the schema's properties against
        the columns of the files, so advertising metadata properties that are never emitted breaks them.
        """
        if self._add_record_metadata:
            return SCHEMA

        properties = {name: prop for name, prop in SCHEMA["properties"].items() if not name.startswith("_sdc_")}
        return {**SCHEMA, "properties": properties}

    def discover_catalog_entry(self, database_name: str, collection_name: str) -> CatalogEntry:
        """Create `CatalogEntry` object for the given collection."""
        unique_stream_id = self.get_fully_qualified_name(collection_name, prefix=self._prefix)
        if self._qualify_stream_names:
            unique_stream_id = f"{database_name}-{unique_stream_id}"
        schema = self._get_schema()

        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=collection_name,
            key_properties=["replication_key"],
            schema=Schema.from_dict(schema),
            replication_method=None,  # Must be defined by user
            metadata=MetadataMapping.get_standard_metadata(
                schema=schema,
                replication_method=REPLICATION_INCREMENTAL,  # User can override this
                key_properties=["replication_key"],
                valid_replication_keys=["replication_key"],  # Known valid replication keys
            ),
            database=database_name,
            row_count=None,
            stream_alias=None,
            replication_key="replication_key",  # Default replication key
        )

    def discover_catalog_entries(self) -> list[dict[str, Any]]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        database_names = self._databases or self._list_database_names()
        entries = [entry for name in database_names for entry in self._discover_database(self.mongo_client[name])]
        stream_ids = [entry["tap_stream_id"] for entry in entries]
        if clashes := sorted({stream_id for stream_id in stream_ids if stream_ids.count(stream_id) > 1}):
            msg = f"Collections map to the same stream name: {clashes}. Set `filter_collections` to tell them apart."
            raise ValueError(msg)
        return entries

    def _list_database_names(self) -> list[str]:
        """Return the databases the user may see, minus the system ones."""
        try:
            with self.mongo_client.list_databases(nameOnly=True, authorizedDatabases=True) as cursor:
                names = [db["name"] for db in cursor]
        except OperationFailure as exception:
            msg = "Could not list databases. Grant the user `listDatabases`, or set `databases` explicitly."
            raise RuntimeError(msg) from exception
        return [name for name in names if name not in SYSTEM_DATABASES]

    def _discover_database(self, database: Database) -> list[dict[str, Any]]:
        """Return the catalog entries for the collections in one database."""
        result: list[dict] = []

        collections = database.list_collection_names(
            authorizedCollections=True,
            nameOnly=True,
            filter={
                "$or": [
                    {
                        "name": {
                            "$regex": f"^{c}$",
                            "$options": "i",
                        }
                    }
                    for c in self._collections
                ]
            }
            if self._collections
            else None,
        )

        for collection in collections:
            try:
                database[collection].find_one()
            except PyMongoError:
                # Skip collections that are not accessible by the authenticated user
                # This is a common case when using a shared cluster
                # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                self._logger.info(
                    "Skipping collection %s.%s, user does not have permission to it.",
                    database.name,
                    collection,
                )
                continue

            self._logger.info("Discovered collection %s.%s", database.name, collection)
            catalog_entry: CatalogEntry = self.discover_catalog_entry(database.name, collection)
            result.append(catalog_entry.to_dict())

        return result

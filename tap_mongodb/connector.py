"""MongoDB/DocumentDB connector utility"""

import sys
from logging import Logger, getLogger
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError
from singer_sdk._singerlib.catalog import CatalogEntry, MetadataMapping, Schema

from tap_mongodb.schema import SCHEMA

if sys.version_info[:2] < (3, 8):
    from backports.cached_property import cached_property
else:
    from functools import cached_property


class MongoDBConnector:
    """MongoDB/DocumentDB connector class"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        connection_string: str,
        options: Dict[str, Any],
        db_name: str,
        datetime_conversion: str,
        prefix: Optional[str] = None,
    ) -> None:
        self._connection_string = connection_string
        self._options = options
        self._db_name = db_name
        self._datetime_conversion: str = datetime_conversion
        self._prefix: Optional[str] = prefix
        self._logger: Logger = getLogger(__name__)

    @cached_property
    def mongo_client(self) -> MongoClient:
        """Provide a MongoClient instance. Client is cached and reused."""
        client: MongoClient = MongoClient(
            self._connection_string, datetime_conversion=self._datetime_conversion, **self._options
        )
        try:
            client.server_info()
        except Exception as exception:
            self._logger.exception("Could not connect to MongoDB")
            raise RuntimeError("Could not connect to MongoDB") from exception
        return client

    @property
    def database(self) -> Database:
        """Provide a Database instance."""
        return self.mongo_client[self._db_name]

    def get_fully_qualified_name(
        self,
        collection_name: str,
        prefix: Optional[str] = None,
        delimiter: str = "_",
    ) -> str:
        """Concatenates a fully qualified name from the parts."""
        parts = []

        if prefix:
            parts.append(prefix)

        parts.append(collection_name)

        return delimiter.join(parts).lower()

    def discover_catalog_entry(self, collection_name: str) -> CatalogEntry:
        """Create `CatalogEntry` object for the given collection."""
        unique_stream_id = self.get_fully_qualified_name(collection_name, prefix=self._prefix)

        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=collection_name,
            key_properties=["replication_key"],
            schema=Schema.from_dict(SCHEMA),
            replication_method=None,  # Must be defined by user
            metadata=MetadataMapping.get_standard_metadata(
                schema=SCHEMA,
                replication_method=None,  # Must be defined by user
                key_properties=["replication_key"],
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def discover_catalog_entries(self) -> List[Dict[str, Any]]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: List[Dict] = []
        for collection in self.database.list_collection_names(authorizedCollections=True, nameOnly=True):
            try:
                self.database[collection].find_one()
            except PyMongoError:
                # Skip collections that are not accessible by the authenticated user
                # This is a common case when using a shared cluster
                # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                self._logger.info(
                    f"Skipping collection {self.database.name}.{collection}, user does not have permission to it."
                )
                continue

            self._logger.info(f"Discovered collection {self.database.name}.{collection}")
            catalog_entry: CatalogEntry = self.discover_catalog_entry(collection)
            result.append(catalog_entry.to_dict())

        return result

"""mongodb tap class."""

from __future__ import annotations
from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
import sys

from pathlib import Path

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from tap_mongodb.streams import CollectionStream


class TapMongoDB(Tap):
    """mongodb tap class."""

    name = "tap-mongodb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "mongodb_connection_string",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "MongoDB connection string. See "
                "https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-uri-format "
                "for specification."
            ),
        ),
        th.Property(
            "mongodb_connection_string_file",
            th.StringType,
            required=False,
            description="Path (relative or absolute) to a file containing a MongoDB connection string URI.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="The earliest record date to sync",
        ),
        th.Property(
            "database_includes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to include. If this list is empty, all databases"
                " will be included."
            ),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to exclude. If this list is empty, no databases"
                " will be excluded."
            ),
        ),
        th.Property(
            "mongodb_max_await_time_ms",
            th.IntegerType,
            required=False,
            description="Path (relative or absolute) to a file containing a MongoDB connection string URI.",
        ),
    ).to_dict()

    def get_mongo_config(self) -> str | None:
        mongodb_connection_string_file = self.config.get(
            "mongodb_connection_string_file", None
        )

        if mongodb_connection_string_file is not None:
            if Path(mongodb_connection_string_file).is_file():
                try:
                    with Path(mongodb_connection_string_file).open() as f:
                        return f.read()
                except Exception as e:
                    self.logger.critical(
                        f"The MongoDB connection string file '{mongodb_connection_string_file}' has errors: {e}"
                    )
                    sys.exit(1)

        return self.config.get("mongo", None)

    def get_mongo_client(self) -> MongoClient:
        client: MongoClient = MongoClient(self.get_mongo_config())
        try:
            client.server_info()
        except Exception as e:
            raise RuntimeError("Could not connect to MongoDB") from e
        return client

    @property
    def catalog_dict(self) -> dict:
        # Use cached catalog if available
        if hasattr(self, "_catalog_dict") and self._catalog_dict:
            return self._catalog_dict
        # Defer to passed in catalog if available
        if self.input_catalog:
            return self.input_catalog.to_dict()
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])
        catalog = Catalog()
        client: MongoClient = self.get_mongo_client()
        for db_name in client.list_database_names():
            if db_includes and db_name not in db_includes:
                continue
            if db_excludes and db_name in db_excludes:
                continue
            try:
                collections = client[db_name].list_collection_names()
            except Exception:
                # Skip databases that are not accessible by the authenticated user
                # This is a common case when using a shared cluster
                # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                # TODO: vet the list of exceptions that can be raised here to be more explicit
                self.logger.debug(
                    "Skipping database %s, authenticated user does not have permission to access",
                    db_name,
                )
                continue
            for collection in collections:
                try:
                    client[db_name][collection].find_one()
                except Exception:
                    # Skip collections that are not accessible by the authenticated user
                    # This is a common case when using a shared cluster
                    # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                    # TODO: vet the list of exceptions that can be raised here to be more explicit
                    self.logger.debug(
                        (
                            "Skipping collections %s, authenticated user does not have permission"
                            " to access"
                        ),
                        db_name,
                    )
                    continue
                self.logger.info("Discovered collection %s.%s", db_name, collection)
                stream_name = f"{db_name}_{collection}"
                entry = CatalogEntry.from_dict({"tap_stream_id": stream_name})
                entry.stream = stream_name
                schema = {
                    "type": "object",
                    "properties": {
                        "_id": {
                            "type": [
                                "string",
                                "null",
                            ],
                            "description": "The document's _id",
                        },
                        "document": {
                            "type": "object",
                            "additionalProperties": True,
                            "description": "The document from the collection",
                        },
                        "operationType": {
                            "type": [
                                "string",
                                "null",
                            ]
                        },
                        "clusterTime": {
                            "type": [
                                "integer",
                                "null",
                            ]
                        },
                        "ns": {
                            "type": [
                                "object",
                                "null",
                            ],
                            "additionalProperties": True,
                        },
                    },
                }
                entry.schema = entry.schema.from_dict(schema)
                entry.key_properties = ["_id"]
                entry.metadata = entry.metadata.get_standard_metadata(
                    schema=schema,
                    key_properties=["_id"],
                    valid_replication_keys=["_id"],
                )
                entry.database = db_name
                entry.table = collection
                catalog.add_stream(entry)
        self._catalog_dict = catalog.to_dict()
        return self._catalog_dict

    def discover_streams(self) -> list[CollectionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        client: MongoClient = self.get_mongo_client()
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])
        for entry in self.catalog.streams:
            if entry.database in db_excludes:
                continue
            if db_includes and entry.database not in db_includes:
                continue
            collection: Collection = client[entry.database][entry.table]
            stream = CollectionStream(
                tap=self,
                name=entry.tap_stream_id,
                schema=entry.schema,
                collection=collection,
                max_await_time_ms=self.config.get("mongodb_max_await_time_ms", None),
            )
            stream.apply_catalog(self.catalog)
            yield stream


if __name__ == "__main__":
    TapMongoDB.cli()

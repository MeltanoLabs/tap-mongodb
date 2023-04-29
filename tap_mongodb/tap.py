"""mongodb tap class."""

from __future__ import annotations
from typing import Any
from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
import sys

from pathlib import Path

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from tap_mongodb.streams import CollectionStream
import json
from urllib.parse import quote_plus


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
            "documentdb_credential_json_string",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "String (serialized JSON object) with keys 'username', 'password', 'engine', 'host', 'port', "
                "'dbClusterIdentifier' or 'dbName', 'ssl'. See example at "
                "https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_secret_json_structure.html#reference_secret_json_structure_docdb"
                ". The password from this JSON object will be url-encoded by the tap before opening the database "
                "connection."
            ),
        ),
        th.Property(
            "documentdb_credential_json_extra_options",
            th.StringType,
            required=False,
            description=(
                "String (serialized JSON object) containing string-string key-value pairs which will be added to the "
                "connection string options when using documentdb_credential_json_string. For example, when set to "
                'the string `{"tls":"true","tlsCAFile":"my-ca-bundle.pem"}`, the options '
                "`tls=true&tlsCAFile=my-ca-bundle.pem` will be passed to the MongoClient."
            ),
        ),
        th.Property(
            "prefix",
            th.StringType,
            required=False,
            default="",
            description="An optional prefix which will be added to each stream name.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description=(
                "Start date. This is used for incremental replication only. Log based replication does not support "
                "this setting - do not provide it unless using the incremental replication method. Defaults to "
                "epoch zero time 1970-01-01 if tap uses incremental replication method."
            ),
        ),
        th.Property(
            "database_includes",
            th.ArrayType(
                th.ObjectType(
                    th.Property("database", th.StringType, required=True),
                    th.Property("collection", th.StringType, required=True),
                ),
            ),
            required=True,
            description=(
                "A list of objects, each specifying database and collection name, to be included in tap output."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            required=False,
            default=False,
            description="When True, _sdc metadata fields will be added to records produced by this tap.",
        ),
        th.Property(
            "allow_modify_change_streams",
            th.BooleanType,
            required=False,
            default=False,
            description=(
                "In DocumentDB (unlike MongoDB), change streams must be enabled specifically (see "
                "https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html#change_streams-enabling"
                "). If attempting to open a change stream against a collection on which change streams have not been "
                "enabled, an OperationFailure error will be raised. If this property is set to True, when this error "
                "is seen, the tap will execute an admin command to enable change streams and then retry the read "
                "operation. Note: this may incur new costs in AWS DocumentDB, and it requires elevated permissions on"
                "the user - the user must have the modifyChangeStreams permission in addition to read permissions."
            ),
        ),
        th.Property(
            "operation_types",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "List of MongoDB change stream operation types to include in tap output. The default behavior is to "
                "limit to document-level operation types. See full list of operation types at"
                "https://www.mongodb.com/docs/manual/reference/change-events/#operation-types. Note that the list "
                "of allowed_values for this property includes some values not available to all MongoDB versions."
            ),
            default=[
                "create",
                "delete",
                "insert",
                "replace",
                "update",
            ],
        ),
    ).to_dict()
    config_jsonschema["properties"]["operation_types"]["items"]["enum"] = [
        "create",
        "createIndexes",
        "delete",
        "drop",
        "dropDatabase",
        "dropIndexes",
        "insert",
        "invalidate",
        "modify",
        "rename",
        "replace",
        "shardCollection",
        "update",
    ]

    def __init__(
            self,
            config: dict | PurePath | str | list[PurePath | str] | None = None,
            catalog: PurePath | str | dict | Catalog | None = None,
            state: PurePath | str | dict | None = None,
            parse_env_config: bool = False,
            validate_config: bool = True,
    ):
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self._catalog_dict = None

    def _get_mongo_connection_string(self) -> str | None:
        documentdb_credential_json_string = self.config.get(
            "documentdb_credential_json_string", None
        )
        if documentdb_credential_json_string is not None:
            self.logger.debug("Using documentdb_credential_json_string")
            documentdb_credential_json: dict[str, Any] = json.loads(
                documentdb_credential_json_string
            )
            username: str = documentdb_credential_json.get("username")
            password: str = documentdb_credential_json.get("password")
            host: str = documentdb_credential_json.get("host")
            port: int = documentdb_credential_json.get("port")
            connection_string = (
                f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}"
            )
            return connection_string

        mongodb_connection_string_file = self.config.get(
            "mongodb_connection_string_file", None
        )

        if mongodb_connection_string_file is not None:
            self.logger.debug(
                f"Using mongodb_connection_string_file: {mongodb_connection_string_file}"
            )
            if Path(mongodb_connection_string_file).exists():
                self.logger.debug("mongodb_connection_string_file exists")
                try:
                    connection_string = (
                        Path(mongodb_connection_string_file).read_text().strip()
                    )
                    return connection_string
                except Exception as e:
                    self.logger.critical(
                        f"The MongoDB connection string file '{mongodb_connection_string_file}' has errors: {e}"
                    )
                    sys.exit(1)
            else:
                self.logger.info("mongodb_connection_string_file is not file")

        self.logger.debug("Using mongodb_connection_string")
        return self.config.get("mongodb_connection_string", None)

    def _get_mongo_options(self) -> dict[str, Any]:
        documentdb_credential_json_extra_options_string = self.config.get(
            "documentdb_credential_json_extra_options", None
        )
        if documentdb_credential_json_extra_options_string is None:
            return {}
        return json.loads(documentdb_credential_json_extra_options_string)

    def get_mongo_client(self) -> MongoClient:
        client: MongoClient = MongoClient(self._get_mongo_connection_string(), **self._get_mongo_options())
        try:
            client.server_info()
        except Exception as e:
            raise RuntimeError("Could not connect to MongoDB") from e
        return client

    def _update_entry_catalog(self, catalog, db_name, collection) -> dict:
        self.logger.info("Discovered collection %s.%s", db_name, collection)

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
                    "type": [
                        "object",
                        "null",
                    ],
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
                        "string",
                        "null",
                    ],
                    "format": "date-time",
                },
                "ns": {
                    "type": [
                        "object",
                        "null",
                    ],
                    "additionalProperties": True,
                },
                "_sdc_extracted_at": {
                    "type": [
                        "string",
                        "null",
                    ],
                    "format": "date-time",
                },
                "_sdc_batched_at": {
                    "type": [
                        "string",
                        "null",
                    ],
                    "format": "date-time",
                },
            },
        }

        prefix = f"{self.config['prefix']}_" if self.config["prefix"] else ""
        stream_name = f"{prefix}{db_name}_{collection}".replace("-", "_").lower()

        entry = CatalogEntry.from_dict({"tap_stream_id": stream_name})
        entry.stream = stream_name
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

        return entry, catalog

    @property
    def catalog_dict(self) -> dict:
        # Use cached catalog if available
        if hasattr(self, "_catalog_dict") and self._catalog_dict:
            return self._catalog_dict
        # Defer to passed in catalog if available
        if self.input_catalog:
            return self.input_catalog.to_dict()

        catalog = Catalog()
        client: MongoClient = self.get_mongo_client()

        for included in self.config.get("database_includes", []):
            db_name = included["database"]
            collection = included["collection"]
            if collection == '*.*':
                self._catalog_dict = dict(streams=[])
                for col in client[db_name].list_collection_names():
                    try:
                        client[db_name][col].find_one()
                    except PyMongoError:
                        # Skip collections that are not accessible by the authenticated user
                        # This is a common case when using a shared cluster
                        # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                        self.logger.info(
                            f"Skipping collections {db_name}.{col}, authenticated user does not have permission to it."
                        )
                        continue

                    entry, catalog = self._update_entry_catalog(catalog, db_name, collection)
                    stream = catalog.to_dict()['streams'][0]
                    for k in ['tap_stream_id', 'table_name', 'stream']:
                        stream[k] = stream[k].replace('*.*', col)
                    self._catalog_dict['streams'].append(stream)
            else:
                try:
                    client[db_name][collection].find_one()
                except PyMongoError:
                    # Skip collections that are not accessible by the authenticated user
                    # This is a common case when using a shared cluster
                    # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                    self.logger.info(
                        f"Skipping collections {db_name}.{collection}, authenticated user does not have permission to it."
                    )
                    continue

                entry, catalog = self._update_entry_catalog(catalog, db_name, collection)

                self._catalog_dict = catalog.to_dict()
        return self._catalog_dict

    def discover_streams(self) -> list[CollectionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        client: MongoClient = self.get_mongo_client()
        for entry in self.catalog.streams:
            collection: Collection = client[entry.database][entry.table]
            stream = CollectionStream(
                tap=self,
                name=entry.tap_stream_id,
                schema=entry.schema,
                collection=collection,
                mongo_client=client,
            )
            stream.apply_catalog(self.catalog)
            yield stream

if __name__ == "__main__":
    TapMongoDB.cli()

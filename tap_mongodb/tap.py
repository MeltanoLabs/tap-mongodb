"""mongodb tap class."""

from __future__ import annotations

import json
import sys
from typing import Any, ClassVar, Optional
from urllib.parse import quote_plus

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_mongodb.connector import MongoDBConnector
from tap_mongodb.streams import MongoDBCollectionStream

if sys.version_info[:2] >= (3, 7):
    from backports.cached_property import cached_property
else:
    from functools import cached_property


class TapMongoDB(Tap):
    """mongodb tap class."""

    name = "tap-mongodb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="Database name from which records will be extracted.",
        ),
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
            "documentdb_credential_json_string",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "String (serialized JSON object) with keys 'username', 'password', 'engine', 'host', 'port', "
                "'dbClusterIdentifier' or 'dbName', 'ssl'. See example at "
                # pylint: disable-next=line-too-long
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
            "datetime_conversion",
            th.StringType,
            required=False,
            default="datetime",
            description=(
                "Parameter passed to MongoClient 'datetime_conversion' parameter. See documentation at "
                "https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes "
                "for details. The default value is 'datetime', which will throw a bson.errors.InvalidBson error "
                "if a document contains a date outside the range of datetime.MINYEAR (year 1) to datetime.MAXYEAR "
                "(9999). The allowed values correspond to the enumeration members here: "
                # pylint: disable-next=line-too-long
                "https://github.com/mongodb/mongo-python-driver/blob/e23eb7691e6e2905a77fc39a114d000ddf057e47/bson/codec_options.py#L192-L224"
                " (they will be uppercased by the tap)."
            ),
            allowed_values=[
                "datetime_ms",
                "datetime",
                "datetime_auto",
                "datetime_clamp",
            ],
        ),
        th.Property(
            "prefix",
            th.StringType,
            required=False,
            default="",
            description="An optional prefix which will be added to each stream name.",
        ),
        th.Property(
            "filter_collections",
            th.OneOf(
                th.StringType,
                th.ArrayType(th.StringType),
            ),
            required=True,
            default=[],
            description="Collections to discover (default: all). Useful for improving catalog discovery performance.",
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
                # pylint: disable-next=line-too-long
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
        th.Property(
            "stream_maps",
            th.ObjectType(),
            required=False,
            description="Stream maps. See https://sdk.meltano.com/en/latest/stream_maps.html for documentation.",
        ),
        th.Property(
            "stream_map_config",
            th.ObjectType(),
            required=False,
            description="Stream map config. See https://sdk.meltano.com/en/latest/stream_maps.html for documentation.",
        ),
    ).to_dict()
    config_jsonschema["properties"]["operation_types"]["items"]["enum"]: ClassVar = [
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

    def _get_mongo_connection_string(self) -> Optional[str]:
        """Get configured MongoDB connection URI."""
        documentdb_credential_json_string = self.config.get("documentdb_credential_json_string", None)
        if documentdb_credential_json_string is not None:
            self.logger.debug("Using documentdb_credential_json_string")
            documentdb_credential_json: dict[str, Any] = json.loads(documentdb_credential_json_string)
            username: str = documentdb_credential_json.get("username")
            password: str = documentdb_credential_json.get("password")
            host: str = documentdb_credential_json.get("host")
            port: int = documentdb_credential_json.get("port")
            connection_string = f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}"
            return connection_string

        self.logger.debug("Using mongodb_connection_string")
        return self.config.get("mongodb_connection_string", None)

    def _get_mongo_options(self) -> dict[str, Any]:
        """Get configured MongoDB/DocumentDB extra options"""
        documentdb_credential_json_extra_options_string = self.config.get(
            "documentdb_credential_json_extra_options", None
        )
        if documentdb_credential_json_extra_options_string is None:
            return {}
        return json.loads(documentdb_credential_json_extra_options_string)

    @cached_property
    def connector(self) -> MongoDBConnector:
        """Get MongoDBConnector instance. Instance is cached and reused."""
        return MongoDBConnector(
            self._get_mongo_connection_string(),
            self._get_mongo_options(),
            self.config.get("database"),
            self.config.get("datetime_conversion"),
            prefix=self.config.get("prefix", None),
            collections=self.config["filter_collections"],
        )

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if hasattr(self, "_catalog_dict") and self._catalog_dict:  # pylint: disable=access-member-before-definition
            return self._catalog_dict  # pylint: disable=access-member-before-definition

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result  # pylint: disable=attribute-defined-outside-init
        return self._catalog_dict

    def discover_streams(self) -> list[MongoDBCollectionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            MongoDBCollectionStream(self, catalog_entry, connector=self.connector)
            for catalog_entry in self.catalog_dict["streams"]
        ]


if __name__ == "__main__":
    TapMongoDB.cli()  # pylint: disable=no-value-for-parameter

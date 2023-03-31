"""mongodb tap class."""

from __future__ import annotations
import yaml
import sys

from pathlib import Path

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_mongodb.collection import CollectionStream


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
    ).to_dict()

    def get_mongo_config(self) -> str | None:
        mongodb_connection_string_file = self.config.get(
            "mongodb_connection_string_file", None
        )

        if mongodb_connection_string_file is not None:
            if Path(mongodb_connection_string_file).is_file():
                try:
                    with Path(mongodb_connection_string_file).open() as f:
                        return yaml.safe_load(f)
                except ValueError:
                    self.logger.critical(
                        f"The YAML mongo_file_location '{mongodb_connection_string_file}' has errors"
                    )
                    sys.exit(1)

        return self.config.get("mongo", None)

    def discover_streams(self) -> list[CollectionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return []


if __name__ == "__main__":
    TapMongoDB.cli()

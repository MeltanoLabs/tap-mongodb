"""Shared fixtures for tests that need a real MongoDB server."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from testcontainers.community.mongodb import MongoDbContainer

MONGODB_IMAGE = "mongo:{version}"
DEFAULT_MONGODB_VERSION = "8.0"


def _print_container_logs(container: MongoDbContainer) -> None:
    try:
        print(container.get_wrapped_container().logs().decode(errors="replace"))
    except Exception as exc:  # noqa: BLE001
        print(f"Couldn't read container logs: {exc!r}")


@pytest.fixture(scope="session")
def mongodb_uri() -> Iterator[str]:
    """Connection string of a MongoDB server shared by the whole test session.

    Uses `TAP_MONGODB_TEST_CONNECTION_STRING` if set; otherwise starts a throwaway container with testcontainers
    (image version from `TAP_MONGODB_TEST_VERSION`, default 8.0). Tests are skipped if neither is available, except on
    CI, where a broken container fails the run instead of passing silently.
    """
    if uri := os.environ.get("TAP_MONGODB_TEST_CONNECTION_STRING"):
        yield uri
        return

    from testcontainers.community.mongodb import MongoDbContainer

    version = os.environ.get("TAP_MONGODB_TEST_VERSION", DEFAULT_MONGODB_VERSION)
    container = MongoDbContainer(MONGODB_IMAGE.format(version=version))
    try:
        container.start()
    except Exception as exc:
        if os.environ.get("CI"):
            _print_container_logs(container)
            raise
        pytest.skip(f"Couldn't start a MongoDB container ({exc!r}); set TAP_MONGODB_TEST_CONNECTION_STRING instead")

    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture(scope="session")
def mongodb_client(mongodb_uri: str) -> Iterator[MongoClient]:
    client: MongoClient = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        pytest.skip(f"MongoDB isn't reachable at {mongodb_uri}: {exc}")
    yield client
    client.close()

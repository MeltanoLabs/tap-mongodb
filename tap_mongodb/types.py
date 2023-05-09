"""Custom type definitions for tap-mongodb."""

import re
from datetime import datetime
from typing import Optional

from bson.objectid import ObjectId
from typing_extensions import Self


class IncrementalId:
    """ID of a record emitted by tap-mongodb in incremental replication mode.

    This class is intended to be an optimal solution for the following constraints:
    * The replication_key must be a datetime, or a string
    * If the replication_key is a string, it must be alphanumerically sortable. If it is not sortable, the tap will not
      be able to resume an incremental load that is interrupted by an error.
    * The replication key should uniquely identify a record in the source MongoDB/DocumentDB database. We would like to
      avoid emitting the same record multiple times (although we understand that behavior to be compatible with the
      at-least-once delivery semantics of Meltano taps)
    """

    PATTERN = re.compile(r"^(?P<dt>\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}\+00:00)?)(\|(?P<oid>[a-f0-9]{24}))?$")

    # pylint: disable-next=used-before-assignment
    def __init__(self, datetime_part: datetime, object_id_part: Optional[str] = None) -> None:
        """Initialize the IncrementalId object.

        self._object_id is declared as an Optional[str] for backwards compatibility with earlier versions of
        tap-mongodb"""
        self._datetime: datetime = datetime_part
        self._object_id: Optional[str] = object_id_part
        self._separator: str = "|"

    @property
    def datetime(self) -> datetime:
        """Return the datetime property of the instance."""
        return self._datetime

    @property
    def object_id_str(self) -> Optional[str]:
        """Return the ObjectId hex string property of the instance, if it exists."""
        return self._object_id

    @property
    def object_id(self) -> ObjectId:
        """Return an ObjectId from the IncrementalId.

        For backwards compatibility, this method generates an ObjectId from self._datetime when self._object_id is
        absent."""
        if self._object_id is None:
            return ObjectId.from_datetime(self._datetime)
        return ObjectId(self._object_id)

    def __str__(self):
        """Return a string representation of the IncrementalId."""
        datetime_part: str = self._datetime.isoformat() if self._datetime else ""
        object_id_part: str = f"{self._separator}{self._object_id}" if self._object_id else ""
        return f"{datetime_part}{object_id_part}"

    @classmethod
    def from_string(cls, id_string: str) -> Self:
        """Create an IncrementalId instance from a string."""
        matched: re.Match = re.match(IncrementalId.PATTERN, id_string)
        if not matched:
            raise ValueError("Invalid IncrementalId string")
        datetime_part = datetime.fromisoformat(matched["dt"])
        object_id_part = matched["oid"] if matched["oid"] else None
        return IncrementalId(datetime_part, object_id_part)

    @classmethod
    def from_object_id(cls, object_id: ObjectId) -> Self:
        """Create an IncrementalId instance from a BSON ObjectId."""
        if object_id is None:
            raise ValueError("ObjectId argument cannot be None")
        datetime_part = object_id.generation_time
        object_id_part = str(object_id)
        return IncrementalId(datetime_part, object_id_part)

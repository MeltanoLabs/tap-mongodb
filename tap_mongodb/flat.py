"""Flat document layout: each declared top-level document field becomes its own column.

The user declares the fields, as properties of the stream's catalog schema (e.g. with Meltano's `schema` extra). Nothing
is inferred: values are converted to the declared types, a value of another type fails the sync, and undeclared fields
are dropped with a warning. A missing field and an explicit `null` are both null.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from singer_sdk.singerlib.json import serialize_json

from tap_mongodb.schema import SCHEMA

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

# Properties of the record envelope, which can't be document fields
ENVELOPE_PROPERTIES = frozenset(SCHEMA["properties"])

# Declared JSON Schema types the flat layout supports; any other declaration takes any value, as JSON
KINDS = frozenset({"integer", "number", "boolean", "date-time", "string", "object", "array"})

# BSON types (as named by the `$type` aggregation operator) without a JSON Schema equivalent, which a `string`
# property converts to the string RECORD mode emits for them
STRING_CONVERTIBLE_TYPES = frozenset(
    {
        "objectId",
        "decimal",
        "binData",
        "timestamp",
        "regex",
        "javascript",
        "javascriptWithScope",
        "symbol",
        "dbPointer",
        "minKey",
        "maxKey",
        "undefined",
    }
)

# BSON types each kind accepts, besides null and missing; None means any type
ALLOWED_TYPES: dict[str, frozenset[str] | None] = {
    "integer": frozenset({"int", "long"}),
    "number": frozenset({"int", "long", "double"}),
    "boolean": frozenset({"bool"}),
    "date-time": frozenset({"date"}),
    "string": frozenset({"string"}) | STRING_CONVERTIBLE_TYPES,
    "object": frozenset({"object"}),
    "array": frozenset({"array"}),
    "json": None,
}


class DeclaredTypeError(TypeError):
    """A document field's value doesn't match the type declared for it."""


@dataclass(frozen=True)
class DeclaredField:
    """A top-level document field declared in the stream's catalog schema."""

    name: str
    kind: str

    @classmethod
    def from_property(cls, name: str, schema: Mapping[str, Any]) -> DeclaredField:
        """Build a declared field from its property schema.

        Args:
            name: The property name, which is also the document field name.
            schema: The property's JSON Schema.

        Returns:
            The declared field. A property without exactly one supported non-null type takes any value, as JSON.
        """
        declared = schema.get("type")
        types = [t for t in ([declared] if isinstance(declared, str) else declared or []) if t != "null"]
        if len(types) != 1:
            return cls(name, "json")
        (kind,) = types
        if kind == "string" and schema.get("format") == "date-time":
            kind = "date-time"
        return cls(name, kind if kind in KINDS else "json")

    @property
    def allowed_types(self) -> frozenset[str] | None:
        """BSON types this field accepts, besides null and missing; None means any type."""
        return ALLOWED_TYPES[self.kind]


def declared_fields(properties: Mapping[str, Mapping[str, Any]], *, stream_name: str) -> list[DeclaredField]:
    """Get the document fields declared among a stream's schema properties.

    Args:
        properties: The stream schema's properties, possibly only the selected ones.
        stream_name: The stream name, for errors.

    Returns:
        The declared fields, in declaration order.

    Raises:
        ValueError: If no document field is declared, or a field name can't be used as a column.
    """
    fields = [
        DeclaredField.from_property(name, schema)
        for name, schema in properties.items()
        if name not in ENVELOPE_PROPERTIES and not name.startswith("_sdc_")
    ]
    if not fields:
        msg = (
            f"The flat document layout requires the schema of stream '{stream_name}' to declare the document fields "
            "to extract, as properties of the stream's catalog schema (e.g. with Meltano's `schema` extra)."
        )
        raise ValueError(msg)
    if invalid := [field.name for field in fields if "." in field.name or field.name.startswith("$")]:
        msg = (
            f"Stream '{stream_name}' declares document fields whose names contain '.' or start with '$', which the "
            f"flat document layout doesn't support yet: {', '.join(invalid)}."
        )
        raise ValueError(msg)
    return fields


def mismatch_error(field: DeclaredField, bson_type: str, *, stream_name: str, object_id: Any) -> DeclaredTypeError:
    """Build the error for a value whose BSON type doesn't match its declared type."""
    return DeclaredTypeError(
        f"Field '{field.name}' in stream '{stream_name}' is declared as '{field.kind}', but document {object_id} has "
        f"a value of BSON type '{bson_type}'. Change the field's declared type, or fix the document."
    )


def as_record_string(value: Any) -> str | None:
    """Convert a scalar to the string RECORD mode emits for it, e.g. `"1.5"` for `Decimal128("1.5")`."""
    if value is None:
        return None
    emitted = json.loads(serialize_json(value))
    return emitted if isinstance(emitted, str) else serialize_json(value)


def flatten_document(
    document: Mapping[str, Any] | None,
    fields: Iterable[DeclaredField],
    *,
    stream_name: str,
    deselected: Iterable[str] = (),
) -> tuple[dict[str, Any], set[str]]:
    """Convert a document's declared fields for a RECORD message, checking their types.

    Args:
        document: The MongoDB document, or None (e.g. a change stream's resume-token record).
        fields: The declared fields.
        stream_name: The stream name, for errors.
        deselected: Declared but deselected field names, which aren't reported as undeclared.

    Returns:
        The declared fields' values, and the names of the document's undeclared fields.

    Raises:
        DeclaredTypeError: If a value doesn't match its declared type.
    """
    fields = list(fields)
    if document is None:
        return {field.name: None for field in fields}, set()

    values = {field.name: _convert(document.get(field.name), field, document, stream_name) for field in fields}
    undeclared = set(document) - {field.name for field in fields} - set(deselected) - {"_id"}
    return values, undeclared


def _convert(value: Any, field: DeclaredField, document: Mapping[str, Any], stream_name: str) -> Any:
    if value is None:
        return None

    def mismatch() -> DeclaredTypeError:
        return mismatch_error(field, _bson_type_name(value), stream_name=stream_name, object_id=document.get("_id"))

    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if field.kind == "integer":
        if is_number and isinstance(value, int):
            return value
        raise mismatch()
    if field.kind == "number":
        if not is_number:
            raise mismatch()
        return None if isinstance(value, float) and not math.isfinite(value) else value
    if field.kind == "boolean":
        if isinstance(value, bool):
            return value
        raise mismatch()
    if field.kind == "date-time":
        if isinstance(value, datetime):
            return value
        raise mismatch()
    if field.kind == "string":
        if isinstance(value, str):
            return value
        if _bson_type_name(value) in STRING_CONVERTIBLE_TYPES:
            return as_record_string(value)
        raise mismatch()
    if field.kind == "object":
        if isinstance(value, dict):
            return value
        raise mismatch()
    if field.kind == "array":
        if isinstance(value, list):
            return value
        raise mismatch()
    return value  # json: any value


def _bson_type_name(value: Any) -> str:
    """Name a decoded value's BSON type as the `$type` aggregation operator does."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "long" if value.bit_length() > 31 else "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime):
        return "date"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, bytes):
        return "binData"
    return {
        "ObjectId": "objectId",
        "Decimal128": "decimal",
        "Binary": "binData",
        "Timestamp": "timestamp",
        "Regex": "regex",
        "Code": "javascript",
        "MinKey": "minKey",
        "MaxKey": "maxKey",
        "DBRef": "object",
        "Int64": "long",
    }.get(type(value).__name__, type(value).__name__)

"""Native Arrow extraction for Arrow BATCH files.

Documents are decoded from raw BSON batches with pymongoarrow, so most values never become Python objects. The record
envelope (`replication_key`, `object_id`, ...) is the same as in RECORD mode. With the default `object` document layout
the document is a `document` struct; with the `flat` layout (see `tap_mongodb.flat`) each declared field is a column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import bson
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pymongo import ASCENDING, DESCENDING
from pymongoarrow.api import Schema
from pymongoarrow.context import PyMongoArrowContext
from pymongoarrow.types import ObjectIdType
from singer_sdk.singerlib.json import serialize_json

from tap_mongodb.flat import DeclaredField, as_record_string, mismatch_error

if TYPE_CHECKING:
    from collections.abc import Iterator
    from logging import Logger, LoggerAdapter

    from bson.codec_options import CodecOptions
    from bson.objectid import ObjectId
    from pymongo.collection import Collection

# BSON types (as named by the `$type` aggregation operator) that pymongoarrow decodes natively
NATIVE_TYPES: dict[str, pa.DataType] = {
    "double": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
    "int": pa.int64(),
    "long": pa.int64(),
    "date": pa.timestamp("ms"),
    "objectId": ObjectIdType(),
}

# Values of these BSON types are serialized to JSON like RECORD mode does, typed with Arrow's canonical JSON type
JSON_TYPES = frozenset({"object", "array"})

# BSON types pymongoarrow silently coerces instead of raising `TypeError`: `int64` truncates doubles (1.5 becomes 1) and
# turns booleans into 1/0, and `float64` turns booleans into 1.0/0.0
_SILENTLY_COERCED: list[tuple[pa.DataType, list[str]]] = [(pa.int64(), ["double", "bool"]), (pa.float64(), ["bool"])]


def _silently_coerced(arrow_type: pa.DataType) -> list[str]:
    """BSON types pymongoarrow silently coerces to `arrow_type`."""
    return next((types for coerced, types in _SILENTLY_COERCED if arrow_type == coerced), [])


_HEX_DIGITS = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)
_NAMESPACE_TYPE = pa.struct([("database", pa.string()), ("collection", pa.string())])
_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


@dataclass
class DocumentPlan:
    """How each top-level document field is converted to Arrow."""

    native: dict[str, pa.DataType] = field(default_factory=dict)
    """Fields decoded by pymongoarrow, with their Arrow types."""

    as_string: list[str] = field(default_factory=list)
    """Scalar fields of a BSON type pymongoarrow doesn't decode, emitted as the string RECORD mode emits."""

    as_json: list[str] = field(default_factory=list)
    """Sub-documents, arrays and fields with conflicting types, emitted with Arrow's canonical JSON type."""

    @property
    def python_fields(self) -> list[str]:
        """Fields decoded in Python from the raw BSON."""
        return [*self.as_string, *self.as_json]

    @classmethod
    def from_field_types(
        cls,
        field_types: dict[str, set[str]],
        *,
        stream_name: str,
        logger: Logger | LoggerAdapter[Any],
    ) -> DocumentPlan:
        """Plan the conversion from the BSON types seen for each top-level field.

        Args:
            field_types: The `$type` names seen for each top-level field.
            stream_name: The stream name, for warnings.
            logger: The logger for conflicting-type warnings.

        Returns:
            The conversion plan.
        """
        plan = cls()
        for name in sorted(field_types, key=lambda name: (name != "_id", name)):
            seen = field_types[name] - {"null"}
            # Both 32- and 64-bit integers decode to int64
            types = {"long" if bson_type == "int" else bson_type for bson_type in seen}
            if not types:
                plan.native[name] = pa.string()  # Only nulls
            elif len(types) == 1 and (bson_type := next(iter(types))) in NATIVE_TYPES:
                plan.native[name] = NATIVE_TYPES[bson_type]
            elif len(types) == 1 and not types & JSON_TYPES:
                plan.as_string.append(name)
            else:
                if len(types) > 1:
                    logger.warning(
                        "Field '%s' in stream '%s' has values of more than one BSON type (%s), so it is emitted as "
                        "JSON in Arrow batch files.",
                        name,
                        stream_name,
                        ", ".join(sorted(seen)),
                    )
                plan.as_json.append(name)
        return plan

    @classmethod
    def from_declared(cls, fields: list[DeclaredField], field_types: dict[str, set[str]]) -> DocumentPlan:
        """Plan the conversion of declared fields, whose values already passed `check_declared_types`.

        Args:
            fields: The declared fields.
            field_types: The `$type` names seen for each declared field.

        Returns:
            The conversion plan, which also decodes `_id` for the record envelope.
        """
        plan = cls(native={"_id": ObjectIdType()})
        for declared in fields:
            seen = field_types.get(declared.name, set()) - {"null", "missing"}
            if declared.kind in {"integer", "number", "boolean", "date-time"}:
                plan.native[declared.name] = {
                    "integer": pa.int64(),
                    "number": pa.float64(),
                    "boolean": pa.bool_(),
                    "date-time": pa.timestamp("ms"),
                }[declared.kind]
            elif declared.kind == "string" and seen <= {"string"}:
                plan.native[declared.name] = pa.string()
            elif declared.kind == "string" and seen == {"objectId"}:
                plan.native[declared.name] = ObjectIdType()
            elif declared.kind == "string":
                plan.as_string.append(declared.name)
            else:
                plan.as_json.append(declared.name)
        return plan


def scan_field_types(collection: Collection, query: dict[str, Any]) -> dict[str, set[str]]:
    """Get the BSON types of each top-level field in the documents matching a query, server-side.

    Args:
        collection: The collection to scan.
        query: The `find` filter.

    Returns:
        The `$type` names seen for each top-level field.
    """
    pipeline: list[dict[str, Any]] = [
        {"$match": query},
        {"$project": {"field": {"$objectToArray": "$$ROOT"}}},
        {"$unwind": "$field"},
        {"$group": {"_id": "$field.k", "types": {"$addToSet": {"$type": "$field.v"}}}},
    ]
    return {row["_id"]: set(row["types"]) for row in collection.aggregate(pipeline, allowDiskUse=True)}


def scan_declared_types(
    collection: Collection,
    query: dict[str, Any],
    fields: list[DeclaredField],
    deselected: list[str],
) -> tuple[dict[str, set[str]], set[str]]:
    """Get the BSON types of `_id` and the declared fields, and the undeclared top-level field names, server-side.

    Args:
        collection: The collection to scan.
        query: The `find` filter.
        fields: The declared fields.
        deselected: Declared but deselected field names, which aren't reported as undeclared.

    Returns:
        The `$type` names seen for `_id` and each declared field (`missing` when absent), and the undeclared names.
    """
    names = ["_id", *(declared.name for declared in fields)]
    aliases = {name: f"t{index}" for index, name in enumerate(names)}
    pipeline: list[dict[str, Any]] = [
        {"$match": query},
        {
            "$project": {
                **{alias: {"$type": f"${name}"} for name, alias in aliases.items()},
                "undeclared": {
                    "$setDifference": [
                        {"$map": {"input": {"$objectToArray": "$$ROOT"}, "in": "$$this.k"}},
                        names + deselected,
                    ],
                },
            },
        },
        {"$unwind": {"path": "$undeclared", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": None,
                **{alias: {"$addToSet": f"${alias}"} for alias in aliases.values()},
                "undeclared": {"$addToSet": "$undeclared"},
            },
        },
    ]
    result = next(collection.aggregate(pipeline, allowDiskUse=True), None)
    if result is None:
        return {}, set()
    return {name: set(result[alias]) for name, alias in aliases.items()}, set(result["undeclared"])


def check_declared_types(
    collection: Collection,
    query: dict[str, Any],
    fields: list[DeclaredField],
    field_types: dict[str, set[str]],
    *,
    stream_name: str,
) -> None:
    """Fail if a declared field has a value of a BSON type its declared type doesn't accept.

    pymongoarrow can't be relied on for this: it silently truncates a double in an `int64` field, for example.

    Raises:
        DeclaredTypeError: Naming the field and an example document.
    """
    for declared in fields:
        allowed = declared.allowed_types
        if allowed is None:
            continue
        if disallowed := sorted(field_types.get(declared.name, set()) - allowed - {"null", "missing"}):
            example = collection.find_one(
                {"$and": [query, {declared.name: {"$type": disallowed}}]},
                {"_id": 1, declared.name: 1},
                sort=[("_id", ASCENDING)],
            )
            object_id = example["_id"] if example else "?"
            bson_type = disallowed[0] if len(disallowed) == 1 else " or ".join(disallowed)
            raise mismatch_error(declared, bson_type, stream_name=stream_name, object_id=object_id)


def object_id_hex(array: pa.Array) -> pa.Array:
    """Convert ObjectIds (12-byte binary, or pymongoarrow's ObjectId extension type) to 24-character hex strings.

    Args:
        array: The ObjectIds.

    Returns:
        A string array of hex ObjectIds, null where the input is null.
    """
    raw = _object_id_bytes(array)
    hexed = np.empty((len(raw), 24), dtype=np.uint8)
    hexed[:, 0::2] = _HEX_DIGITS[raw >> 4]
    hexed[:, 1::2] = _HEX_DIGITS[raw & 0x0F]
    mask = None if array.null_count == 0 else ~array.is_valid().to_numpy(zero_copy_only=False)
    return pa.array(hexed.view("S24").ravel(), type=pa.binary(), mask=mask).cast(pa.string())


def incremental_ids(array: pa.Array, hexes: pa.Array) -> pa.Array:
    """Build `IncrementalId` strings (`<generation time>|<hex ObjectId>`) for non-null ObjectIds.

    Args:
        array: The ObjectIds.
        hexes: The same ObjectIds as hex strings, from `object_id_hex`.

    Returns:
        A string array matching `str(IncrementalId.from_object_id(object_id))`.
    """
    raw = _object_id_bytes(array).astype(np.int64)
    seconds = raw[:, 0] << 24 | raw[:, 1] << 16 | raw[:, 2] << 8 | raw[:, 3]  # Big-endian generation time
    generated_at = pa.array(seconds, type=pa.int64()).cast(pa.timestamp("s", tz="UTC"))
    iso = pc.strftime(generated_at, format="%Y-%m-%dT%H:%M:%S")
    return pc.binary_join_element_wise(iso, pa.scalar("+00:00|"), hexes, "")


def _object_id_bytes(array: pa.Array) -> np.ndarray:
    storage = array.storage if isinstance(array, pa.ExtensionArray) else array
    return np.frombuffer(
        storage.buffers()[1],
        dtype=np.uint8,
        count=12 * len(storage),
        offset=12 * storage.offset,
    ).reshape(-1, 12)


@dataclass
class ArrowExtractor:
    """Extracts a collection's documents as Arrow tables shaped like the stream's records."""

    collection: Collection
    stream_name: str
    logger: Logger | LoggerAdapter[Any]
    add_record_metadata: bool = False
    declared: list[DeclaredField] | None = None
    """The selected declared fields with the `flat` document layout, or None for the `object` layout."""
    deselected: list[str] = field(default_factory=list)
    """Declared but deselected field names with the `flat` document layout, which aren't reported as undeclared."""

    def tables(self, start: ObjectId) -> Iterator[pa.Table]:
        """Yield the documents after `start`, in `_id` order, as Arrow tables.

        The range is bounded by the newest `_id` when the sync starts, so documents inserted during the sync can't
        bring fields or types the type scan didn't see. The next sync picks them up.

        Args:
            start: Extract documents with an `_id` greater than this.

        Yields:
            One table per raw batch returned by the server.

        Raises:
            TypeError: If `_id` isn't always an ObjectId.
            RuntimeError: If a field's type changed after the type scan, e.g. a document was updated during the sync.
        """
        newest = self.collection.find_one({"_id": {"$gt": start}}, {"_id": 1}, sort=[("_id", DESCENDING)])
        if newest is None:
            return

        query = {"_id": {"$gt": start, "$lte": newest["_id"]}}
        if self.declared is None:
            field_types = scan_field_types(self.collection, query)
            plan = DocumentPlan.from_field_types(field_types, stream_name=self.stream_name, logger=self.logger)
        else:
            field_types, undeclared = scan_declared_types(self.collection, query, self.declared, self.deselected)
            if undeclared:
                self.logger.warning(
                    "Stream '%s' uses the flat document layout, so these undeclared document fields are dropped: %s",
                    self.stream_name,
                    ", ".join(sorted(undeclared)),
                )
            check_declared_types(self.collection, query, self.declared, field_types, stream_name=self.stream_name)
            plan = DocumentPlan.from_declared(self.declared, field_types)
        if field_types.get("_id", {"objectId"}) != {"objectId"}:
            msg = f"Incremental replication of stream '{self.stream_name}' requires every `_id` to be an ObjectId."
            raise TypeError(msg)
        schema = Schema(plan.native)
        codec_options = self.collection.codec_options

        try:
            for raw_batch in self.collection.find_raw_batches(query, sort=[("_id", ASCENDING)]):
                yield self._table(raw_batch, plan, schema, codec_options)
        except TypeError as error:
            # pymongoarrow raises this for a value that doesn't match the schema, without naming the field
            msg = (
                f"A field in stream '{self.stream_name}' has a value whose type differs from the types seen when the "
                "sync started, probably because a document was updated during the sync. Run the sync again."
            )
            raise RuntimeError(msg) from error

    def _table(self, raw_batch: bytes, plan: DocumentPlan, schema: Schema, codec_options: CodecOptions) -> pa.Table:
        context = PyMongoArrowContext(schema, codec_options)
        context.process_bson_stream(raw_batch)
        native = context.finish()
        self._check_silent_coercions(native, plan)

        fields = {name: _conform(native[name].combine_chunks()) for name in plan.native}
        if plan.python_fields:
            documents = bson.decode_all(raw_batch, codec_options)
            for name in plan.as_string:
                fields[name] = pa.array([as_record_string(doc.get(name)) for doc in documents], type=pa.string())
            for name in plan.as_json:
                fields[name] = pa.array([_as_json(doc.get(name)) for doc in documents], type=pa.json_())

        object_ids = native["_id"].combine_chunks()
        hexes = object_id_hex(object_ids)
        rows = native.num_rows
        table = {
            "replication_key": incremental_ids(object_ids, hexes),
            "object_id": hexes,
        }
        if self.declared is None:
            table["document"] = pa.StructArray.from_arrays(list(fields.values()), names=list(fields))
        else:
            table.update((declared.name, fields[declared.name]) for declared in self.declared)
        table |= {
            "operation_type": pa.nulls(rows, pa.string()),
            "cluster_time": pa.nulls(rows, _TIMESTAMP_TYPE),
            "namespace": pa.array(
                [{"database": self.collection.database.name, "collection": self.collection.name}] * rows,
                type=_NAMESPACE_TYPE,
            ),
        }
        if self.add_record_metadata:
            table["_sdc_batched_at"] = pa.array([datetime.now(timezone.utc)] * rows, type=_TIMESTAMP_TYPE)
        return pa.table(table)

    def _check_silent_coercions(self, native: pa.Table, plan: DocumentPlan) -> None:
        """Fail if a numeric column got a value pymongoarrow coerces silently instead of raising `TypeError`.

        The type scan rules these values out when the sync starts, but a document updated during the sync can still
        bring one, so check the batch's documents on the server: one indexed query per batch with numeric columns.

        Raises:
            RuntimeError: Naming the field, the value's BSON type and the document.
        """
        checks = {name: types for name, arrow_type in plan.native.items() if (types := _silently_coerced(arrow_type))}
        if not checks or native.num_rows == 0:
            return

        object_ids = native["_id"].combine_chunks()
        batch_range = {"_id": {"$gte": object_ids[0].as_py(), "$lte": object_ids[-1].as_py()}}
        clauses = [_has_type(name, types) for name, types in checks.items()]
        document = self.collection.find_one({**batch_range, "$or": clauses}, sort=[("_id", ASCENDING)])
        if document is None:
            return

        for name, types in checks.items():
            value = document.get(name)
            bson_type = "bool" if isinstance(value, bool) else "double" if isinstance(value, float) else None
            if bson_type in types:
                msg = (
                    f"Field '{name}' in stream '{self.stream_name}' has a value of BSON type '{bson_type}' in document "
                    f"{document['_id']}, which differs from the types seen when the sync started, probably because "
                    "the document was updated during the sync. Run the sync again."
                )
                raise RuntimeError(msg)


def _has_type(name: str, types: list[str]) -> dict[str, Any]:
    """A filter matching documents whose top-level field `name` has one of the BSON `types`."""
    if "." in name or name.startswith("$"):
        # Field names that `find` would read as paths or operators
        field_value = {"$getField": {"field": {"$literal": name}, "input": "$$ROOT"}}
        return {"$expr": {"$in": [{"$type": field_value}, types]}}
    return {name: {"$type": types}}


def _conform(column: pa.Array) -> pa.Array:
    """Match the values RECORD mode emits for natively decoded fields."""
    if isinstance(column.type, ObjectIdType):
        return object_id_hex(column)
    if pa.types.is_floating(column.type):
        # RECORD mode replaces infinities with null
        return pc.if_else(pc.is_inf(column), pa.scalar(None, column.type), column)
    if pa.types.is_timestamp(column.type):
        return column.cast(pa.timestamp(column.type.unit, tz="UTC"))
    return column


def _as_json(value: Any) -> str | None:
    """Serialize a value the way RECORD mode does, with NaN and infinities as null."""
    return None if value is None else serialize_json(value)

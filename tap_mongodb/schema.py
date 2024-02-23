"""Schema for records emitted by tap-mongodb extractor."""

SCHEMA = {
    "properties": {
        "cluster_time": {
            "description": (
                "MongoDB/Document cluster time of a change stream message, when tap is running in log-based "
                "replication mode. When tap is running in incremental mode, this is null."
            ),
            "format": "date-time",
            "type": [
                "string",
                "null",
            ],
        },
        "document": {
            "additionalProperties": True,
            "description": (
                "The document from the collection. When running the tap in log-based replication mode, this is equal "
                "to the fullDocument field on the MongoDB Change Stream change event."
            ),
            "type": [
                "object",
                "null",
            ],
        },
        "update_description": {
            "additionalProperties": True,
            "description": (
                "When running the tap in log-based replication mode, this is equal to the updateDescription field on "
                "the MongoDB Change Stream change event. This field will always be null when the tap runs in "
                "incremental replication mode."
            ),
            "type": [
                "object",
                "null",
            ],
        },
        "namespace": {
            "description": "MongoDB namespace of the record, indicating database name and collection name.",
            "additionalProperties": False,
            "type": [
                "object",
                "null",
            ],
            "properties": {
                "database": {
                    "description": "Name of the MongoDB/DocumentDB database from which this record was extracted.",
                    "type": [
                        "string",
                        "null",
                    ],
                },
                "collection": {
                    "description": "Name of the MongoDB/DocumentDB collection from which this record was extracted.",
                    "type": [
                        "string",
                        "null",
                    ],
                },
            },
        },
        "to": {
            "description": (
                "When the tap is running in log-based replication mode, and the change event is of operation_type "
                "'rename' (meaning, that a database collection was renamed), this field indicates the new namespace."
            ),
            "additionalProperties": False,
            "type": [
                "object",
                "null",
            ],
            "properties": {
                "database": {
                    "description": "New name of the MongoDB/DocumentDB database.",
                    "type": [
                        "string",
                        "null",
                    ],
                },
                "collection": {
                    "description": "New name of the MongoDB/DocumentDB collection.",
                    "type": [
                        "string",
                        "null",
                    ],
                },
            },
        },
        "object_id": {
            "description": "ObjectId ID of the record. 24 character hex string.",
            "type": [
                "string",
                "null",
            ],
        },
        "operation_type": {
            "description": "MongoDB namespace, indicating database name and collection name.",
            "type": [
                "string",
                "null",
            ],
        },
        "replication_key": {
            "description": "Replication key which uniquely identifies one record.",
            "type": [
                "string",
                "null",
            ],
        },
        "_sdc_batched_at": {
            "format": "date-time",
            "type": [
                "string",
                "null",
            ],
        },
        "_sdc_extracted_at": {
            "description": "Timestamp representing the time that a record was extracted from the database.",
            "format": "date-time",
            "type": [
                "string",
                "null",
            ],
        },
    },
    "type": "object",
}

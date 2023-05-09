"""Schema for records emitted by tap-mongodb extractor."""

SCHEMA = {
    "properties": {
        "_id": {
            "description": "Unique identifier for one record",
            "type": [
                "string",
                "null",
            ],
        },
        "object_id": {
            "description": "ObjectId ID of the record. 24 character hex string.",
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
            "format": "date-time",
            "type": [
                "string",
                "null",
            ],
        },
        "clusterTime": {
            "format": "date-time",
            "type": [
                "string",
                "null",
            ],
        },
        "document": {
            "additionalProperties": True,
            "description": "The document from the collection",
            "type": [
                "object",
                "null",
            ],
        },
        "ns": {
            "description": "MongoDB namespace, indicating database name and collection name.",
            "additionalProperties": True,
            "type": [
                "object",
                "null",
            ],
        },
        "operationType": {
            "description": "MongoDB namespace, indicating database name and collection name.",
            "type": [
                "string",
                "null",
            ],
        },
    },
    "type": "object",
}

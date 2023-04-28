SCHEMA = {
    "properties": {
        "_id": {
            "description": "The document's _id",
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
        "ns": {"additionalProperties": True, "type": ["object", "null"]},
        "operationType": {
            "type": [
                "string",
                "null",
            ],
        },
    },
    "type": "object",
}

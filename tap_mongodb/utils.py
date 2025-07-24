"""Utility functions for tap-mongodb."""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any
from uuid import UUID

from bson import Binary, ObjectId


def sanitize_doc(doc: Any) -> Any:
    """
    Recursively sanitize a document by converting MongoDB-specific types to JSON-serializable types.
    
    This function is used when the 'sanitize_documents' config parameter is enabled to ensure
    that all MongoDB-specific data types are converted to standard JSON-serializable types
    before being sent through the Singer pipeline.
    
    Type conversions performed:
    - bson.ObjectId → str (using str() representation)
    - uuid.UUID → str (using str() representation)  
    - datetime.datetime → str (using ISO format via isoformat())
    - bson.Binary → str (base64-encoded string)
    - bytes → str (base64-encoded string)
    - dict → recursively sanitized dict
    - list → recursively sanitized list
    - All other types → unchanged
    
    Args:
        doc: The document, value, or data structure to sanitize. Can be any type.
        
    Returns:
        The sanitized document with all MongoDB-specific types converted to 
        JSON-serializable equivalents. The structure is preserved but types
        are converted as needed.
        
    Examples:
        >>> from bson import ObjectId
        >>> from datetime import datetime
        >>> doc = {"_id": ObjectId("507f1f77bcf86cd799439011"), "created": datetime.now()}
        >>> sanitized = sanitize_doc(doc)
        >>> # Returns: {"_id": "507f1f77bcf86cd799439011", "created": "2023-01-01T12:00:00"}
    """
    if isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, UUID):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()
    elif isinstance(doc, Binary):
        return base64.b64encode(doc).decode('utf-8')
    elif isinstance(doc, bytes):
        return base64.b64encode(doc).decode('utf-8')
    elif isinstance(doc, dict):
        return {key: sanitize_doc(value) for key, value in doc.items()}
    elif isinstance(doc, list):
        return [sanitize_doc(item) for item in doc]
    else:
        return doc
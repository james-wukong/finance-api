from __future__ import annotations

from pymongo import MongoClient
from pymongo import errors


class MongoConn:
    """
    Initialize the mongodb connection
    """
    @classmethod
    def initialize_mongodb_client(cls, uri: str) -> MongoClient | None:
        try:
            # Initialize new MongoDB client
            client = MongoClient(uri)
        except errors.ConnectionFailure as e:
            # Handle connection failure gracefully
            print(f"Failed to connect to MongoDB: {e}")

            return None
        else:
            return client

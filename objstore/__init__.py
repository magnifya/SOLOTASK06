"""Multi-tenant content-addressed object storage backend (stdlib only)."""

from .http_app import create_server
from .store import ContentAddressedStore, ObjectStoreError

__all__ = ["ContentAddressedStore", "create_server"]

__version__ = "0.1.0"

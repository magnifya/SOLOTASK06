"""Content-addressed object store.

Blob bytes live once under ``<root>/blobs/<sha256>`` so identical payloads are
stored exactly once regardless of how many keys point at them. Object metadata
(``key -> {sha256, size, content_type}``) lives in ``<root>/index.json`` and is
always rewritten through a temporary file plus ``os.replace`` so a crash can
never observe a half-written index.

The store is safe to use from several threads: every read that must be
consistent and every mutation runs under a single re-entrant lock.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading

__all__ = ["ContentAddressedStore", "ObjectStoreError"]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")


class ObjectStoreError(Exception):
    """Raised for missing objects, invalid digests and corrupted indexes."""


def _is_sha256(value):
    """Return True when *value* is a lowercase hex sha256 digest."""
    return isinstance(value, str) and _SHA256_RE.match(value) is not None


def _atomic_write(path, data):
    """Write *data* to *path* via a unique temp file plus os.replace."""
    tmp = "%s.tmp.%d.%d" % (path, os.getpid(), threading.get_ident())
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


class ContentAddressedStore:
    """Multi-tenant key -> content-addressed blob mapping persisted on disk."""

    def __init__(self, root):
        if not isinstance(root, (str, bytes, os.PathLike)):
            raise ObjectStoreError("invalid root: %r" % (root,))
        self.root = os.path.abspath(os.fspath(root))
        self.blobs_dir = os.path.join(self.root, BLOBS_DIRNAME)
        self.index_path = os.path.join(self.root, INDEX_NAME)
        self._lock = threading.RLock()
        os.makedirs(self.blobs_dir, exist_ok=True)
        self._index = self._load_index()

    # ------------------------------------------------------------------
    # index handling
    # ------------------------------------------------------------------
    def _load_index(self):
        if not os.path.exists(self.index_path):
            return {"version": INDEX_VERSION, "objects": {}}
        try:
            with open(self.index_path, "rb") as handle:
                raw = handle.read()
            document = json.loads(raw.decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted index: %s" % (exc,))
        if not isinstance(document, dict):
            raise ObjectStoreError("corrupted index: root is not an object")
        objects = document.get("objects")
        if not isinstance(objects, dict):
            raise ObjectStoreError("corrupted index: missing objects map")
        clean = {}
        for key, entry in objects.items():
            if not isinstance(key, str):
                raise ObjectStoreError("corrupted index: non-string key")
            clean[key] = self._validate_entry(key, entry)
        return {"version": INDEX_VERSION, "objects": clean}

    @staticmethod
    def _validate_entry(key, entry):
        if not isinstance(entry, dict):
            raise ObjectStoreError("corrupted index: entry for %r is not an object" % (key,))
        sha = entry.get("sha256")
        size = entry.get("size")
        content_type = entry.get("content_type")
        if not _is_sha256(sha):
            raise ObjectStoreError("corrupted index: bad sha256 for %r" % (key,))
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("corrupted index: bad size for %r" % (key,))
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("corrupted index: bad content_type for %r" % (key,))
        return {"sha256": sha, "size": size, "content_type": content_type}

    def _save_index(self):
        document = {"version": INDEX_VERSION, "objects": self._index["objects"]}
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"))
        _atomic_write(self.index_path, payload.encode("utf-8") + b"\n")

    # ------------------------------------------------------------------
    # validation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _check_key(key):
        if not isinstance(key, str) or not key or "\x00" in key:
            raise ObjectStoreError("invalid key: %r" % (key,))
        if any(part in ("", ".", "..") for part in key.split("/")):
            raise ObjectStoreError("invalid key: %r" % (key,))
        return key

    @staticmethod
    def _check_limit(limit):
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ObjectStoreError("invalid limit: %r" % (limit,))
        if limit < 1 or limit > MAX_LIMIT:
            raise ObjectStoreError("invalid limit: %r" % (limit,))
        return limit

    @staticmethod
    def _require_sha(sha256):
        if not _is_sha256(sha256):
            raise ObjectStoreError("invalid sha256: %r" % (sha256,))
        return sha256

    def _blob_path(self, sha256):
        return os.path.join(self.blobs_dir, sha256)

    # ------------------------------------------------------------------
    # object API
    # ------------------------------------------------------------------
    def put(self, key, payload, content_type=None):
        """Store *payload* under *key* and return its metadata entry."""
        self._check_key(key)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        data = bytes(payload)
        sha = hashlib.sha256(data).hexdigest()
        entry = {"sha256": sha, "size": len(data), "content_type": content_type}
        with self._lock:
            path = self._blob_path(sha)
            if not os.path.exists(path):
                _atomic_write(path, data)
            self._index["objects"][key] = entry
            self._save_index()
            return dict(entry)

    def get(self, key):
        """Return ``(payload, entry)`` for *key*."""
        with self._lock:
            entry = self.head(key)
            return self.blob(entry["sha256"]), entry

    def head(self, key):
        """Return a copy of the metadata entry for *key*."""
        self._check_key(key)
        with self._lock:
            entry = self._index["objects"].get(key)
            if entry is None:
                raise ObjectStoreError("not found: %s" % (key,))
            return dict(entry)

    def delete(self, key):
        """Drop *key*. Blobs stay on disk because other keys may share them."""
        self._check_key(key)
        with self._lock:
            if key not in self._index["objects"]:
                raise ObjectStoreError("not found: %s" % (key,))
            del self._index["objects"][key]
            self._save_index()

    def blob(self, sha256):
        """Return the raw bytes of a stored digest."""
        self._require_sha(sha256)
        with self._lock:
            try:
                with open(self._blob_path(sha256), "rb") as handle:
                    return handle.read()
            except FileNotFoundError:
                raise ObjectStoreError("not found: blob %s" % (sha256,))
            except OSError as exc:
                raise ObjectStoreError("blob read failed: %s" % (exc,))

    def has_blob(self, sha256):
        """Return True when the digest is present in the blob directory."""
        self._require_sha(sha256)
        with self._lock:
            return os.path.exists(self._blob_path(sha256))

    def blob_digests(self):
        """Return the sorted list of digests currently stored on disk."""
        with self._lock:
            return sorted(name for name in os.listdir(self.blobs_dir) if _is_sha256(name))

    def list_objects(self, prefix="", after=None, limit=DEFAULT_LIMIT):
        """Return one page of ``{"items": [...], "next_after": <str|null>}``."""
        if not isinstance(prefix, str):
            raise ObjectStoreError("invalid prefix: %r" % (prefix,))
        if after is not None and not isinstance(after, str):
            raise ObjectStoreError("invalid after: %r" % (after,))
        limit = self._check_limit(limit)
        with self._lock:
            objects = self._index["objects"]
            keys = sorted(key for key in objects if key.startswith(prefix))
            if after:
                keys = [key for key in keys if key > after]
            page = keys[:limit]
            items = []
            for key in page:
                entry = objects[key]
                items.append(
                    {
                        "key": key,
                        "sha256": entry["sha256"],
                        "size": entry["size"],
                        "content_type": entry["content_type"],
                    }
                )
            next_after = page[-1] if len(keys) > len(page) else None
            return {"items": items, "next_after": next_after}

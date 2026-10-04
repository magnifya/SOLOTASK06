"""Content-addressed object store (standard library only).

Blob bytes are stored once as ``<root>/blobs/<sha256>``; object metadata
(``key -> {sha256, size, content_type}``) lives in ``<root>/index.json`` and is
always rewritten through a temporary file plus ``os.replace``. Every mutation
and every consistent read runs under a single re-entrant lock.

Every blob read is verified against its digest, so on-disk corruption is never
served. A ``put`` whose target digest exists but is corrupt atomically repairs
that blob from the incoming payload, restoring every key sharing the digest.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading

__all__ = [
    "ContentAddressedStore",
    "ObjectStoreError",
    "CORRUPTED_BLOB",
    "BLOB_IO_ERROR",
]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

#: Raised when bytes on disk do not match their digest or metadata size.
CORRUPTED_BLOB = "corrupted blob"
#: Raised on an I/O failure against an existing blob file or a repair write.
BLOB_IO_ERROR = "blob io error"

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")

# Sentinel distinguishing a missing blob file from corrupt/valid bytes.
_BLOB_MISSING = object()


class ObjectStoreError(Exception):
    """Missing object, invalid key/digest/limit, or a corrupted index."""


def _is_sha256(value):
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

    # -- index ---------------------------------------------------------
    def _load_index(self):
        if not os.path.exists(self.index_path):
            return {"version": INDEX_VERSION, "objects": {}}
        try:
            with open(self.index_path, "rb") as handle:
                document = json.loads(handle.read().decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted index: %s" % (exc,))
        if not isinstance(document, dict) or not isinstance(document.get("objects"), dict):
            raise ObjectStoreError("corrupted index: missing objects map")
        objects = {}
        for key, entry in document["objects"].items():
            if not isinstance(key, str):
                raise ObjectStoreError("corrupted index: non-string key")
            objects[key] = self._validate_entry(key, entry)
        return {"version": INDEX_VERSION, "objects": objects}

    @staticmethod
    def _validate_entry(key, entry):
        if not isinstance(entry, dict):
            raise ObjectStoreError("corrupted index: entry for %r is not an object" % (key,))
        sha, size = entry.get("sha256"), entry.get("size")
        content_type = entry.get("content_type")
        if not _is_sha256(sha):
            raise ObjectStoreError("corrupted index: bad sha256 for %r" % (key,))
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("corrupted index: bad size for %r" % (key,))
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("corrupted index: bad content_type for %r" % (key,))
        return {"sha256": sha, "size": size, "content_type": content_type}

    def _save_index(self):
        payload = json.dumps(
            {"version": INDEX_VERSION, "objects": self._index["objects"]},
            sort_keys=True,
            separators=(",", ":"),
        )
        _atomic_write(self.index_path, payload.encode("utf-8") + b"\n")

    # -- validation ----------------------------------------------------
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
        if not 1 <= limit <= MAX_LIMIT:
            raise ObjectStoreError("invalid limit: %r" % (limit,))
        return limit

    @staticmethod
    def _require_sha(sha256):
        if not _is_sha256(sha256):
            raise ObjectStoreError("invalid sha256: %r" % (sha256,))
        return sha256

    # -- object API ----------------------------------------------------
    def put(self, key, payload, content_type=None):
        """Store *payload* under *key* and return its metadata entry.

        If the blob for the payload's digest is present but corrupt, it is
        atomically repaired from *payload*; missing blobs are written as usual.
        Repair failures leave both the blob and the object map untouched.
        """
        self._check_key(key)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        data = bytes(payload)
        sha = hashlib.sha256(data).hexdigest()
        entry = {"sha256": sha, "size": len(data), "content_type": content_type}
        with self._lock:
            path = os.path.join(self.blobs_dir, sha)
            needs_write = not os.path.exists(path)
            if not needs_write:
                raw = self._read_blob_raw(path)
                if raw is _BLOB_MISSING:
                    needs_write = True
                elif hashlib.sha256(raw).hexdigest() != sha:
                    # The shared blob exists but its bytes are corrupt; the
                    # incoming payload is a complete, verified replacement.
                    needs_write = True
            if needs_write:
                self._write_blob(path, data)
            self._index["objects"][key] = entry
            self._save_index()
            return dict(entry)

    def get(self, key):
        """Return ``(payload, entry)`` for *key*.

        The blob digest must match and the byte count must equal the
        metadata ``size``; otherwise :class:`ObjectStoreError` is raised with
        ``corrupted blob``.
        """
        with self._lock:
            entry = self.head(key)
            data = self.blob(entry["sha256"])
            if len(data) != entry["size"]:
                raise ObjectStoreError(CORRUPTED_BLOB)
            return data, entry

    def head(self, key):
        """Return a copy of the metadata entry for *key*."""
        self._check_key(key)
        with self._lock:
            entry = self._index["objects"].get(key)
            if entry is None:
                raise ObjectStoreError("not found: %s" % (key,))
            return dict(entry)

    def delete(self, key):
        """Drop *key*; shared blobs stay on disk for the keys still using them."""
        self._check_key(key)
        with self._lock:
            if key not in self._index["objects"]:
                raise ObjectStoreError("not found: %s" % (key,))
            del self._index["objects"][key]
            self._save_index()

    def blob(self, sha256):
        """Return the raw bytes stored under digest *sha256*.

        The bytes are re-read and re-hashed on every call: if their digest
        differs from *sha256* the caller gets ``ObjectStoreError("corrupted
        blob")`` instead of the bytes. A read failure against an existing blob
        file raises ``ObjectStoreError("blob io error")``.
        """
        self._require_sha(sha256)
        with self._lock:
            path = os.path.join(self.blobs_dir, sha256)
            raw = self._read_blob_raw(path)
            if raw is _BLOB_MISSING:
                raise ObjectStoreError("not found: blob %s" % (sha256,))
            if hashlib.sha256(raw).hexdigest() != sha256:
                raise ObjectStoreError(CORRUPTED_BLOB)
            return raw

    def _read_blob_raw(self, path):
        """Read *path*, returning its bytes, or ``_BLOB_MISSING`` if absent.

        An I/O error against a file that exists (or races away between the
        existence check and the open) is reported as ``blob io error``.
        """
        try:
            with open(path, "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return _BLOB_MISSING
        except OSError:
            raise ObjectStoreError(BLOB_IO_ERROR)

    def _write_blob(self, path, data):
        """Atomically write *data* to a new or repaired blob, verifying first.

        The destination keeps its old bytes unless the full new content has
        been staged and ``os.replace`` succeeds, so an interrupted repair never
        leaves partially written content. Callers only mutate the object map
        after this returns, so a failed repair cannot change object mappings.
        """
        try:
            _atomic_write(path, data)
        except OSError:
            raise ObjectStoreError(BLOB_IO_ERROR)

    def blob_digests(self):
        """Return the sorted digests currently present in the blob directory."""
        with self._lock:
            return sorted(name for name in os.listdir(self.blobs_dir) if _is_sha256(name))

    def list_objects(self, prefix="", after=None, limit=DEFAULT_LIMIT):
        """Return ``{"items": [...], "next_after": <str|null>}`` for one page."""
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
            items = [
                {
                    "key": key,
                    "sha256": objects[key]["sha256"],
                    "size": objects[key]["size"],
                    "content_type": objects[key]["content_type"],
                }
                for key in page
            ]
            return {"items": items, "next_after": page[-1] if len(keys) > len(page) else None}

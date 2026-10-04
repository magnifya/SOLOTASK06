"""Content-addressed object store (standard library only).

Blob bytes are stored once as ``<root>/blobs/<sha256>``; object metadata
(``key -> {sha256, size, content_type}``) lives in ``<root>/index.json`` and is
always rewritten through a temporary file plus ``os.replace``. Every mutation
and every consistent read runs under a single re-entrant lock.

Resumable chunked uploads live under ``<root>/uploads/``: each session is a
``<upload_id>.json`` declaration plus a ``<upload_id>.data`` spill file holding
the bytes received so far. Session metadata is rewritten atomically like the
index, so an interrupted append leaves either the old offset or the whole new
chunk, and an interrupted complete leaves either the old object or the fully
published new one.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import uuid

__all__ = ["ContentAddressedStore", "ObjectStoreError"]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
UPLOADS_DIRNAME = "uploads"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_UPLOAD_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")


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
        self.uploads_dir = os.path.join(self.root, UPLOADS_DIRNAME)
        self.index_path = os.path.join(self.root, INDEX_NAME)
        self._lock = threading.RLock()
        os.makedirs(self.blobs_dir, exist_ok=True)
        os.makedirs(self.uploads_dir, exist_ok=True)
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
            self._store_blob(sha, data)
            self._index["objects"][key] = entry
            self._save_index()
            return dict(entry)

    def _store_blob(self, sha, data):
        """Write the blob for *sha* when missing and repair it when corrupted.

        An existing file is hashed on every put. Intact files are left exactly
        as they are so dedup costs nothing extra; a file whose digest no longer
        matches is rewritten atomically from the complete *data*, so an
        interrupted repair leaves either the old bytes or the full new bytes.
        """
        path = os.path.join(self.blobs_dir, sha)
        if os.path.exists(path):
            try:
                with open(path, "rb") as handle:
                    stored = handle.read()
            except FileNotFoundError:
                stored = None
            except OSError:
                raise ObjectStoreError("blob io error")
            if stored is not None:
                if hashlib.sha256(stored).hexdigest() == sha:
                    return
                try:
                    _atomic_write(path, data)
                except OSError:
                    raise ObjectStoreError("blob io error")
                return
        try:
            _atomic_write(path, data)
        except OSError:
            raise ObjectStoreError("blob io error")

    def get(self, key):
        """Return ``(payload, entry)`` for *key*."""
        with self._lock:
            entry = self.head(key)
            data = self.blob(entry["sha256"])
            if len(data) != entry["size"]:
                raise ObjectStoreError("corrupted blob")
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

        The digest of the bytes actually on disk is recomputed on every read,
        so truncation, appended bytes and same-length tampering are all
        rejected instead of being handed back to the caller.
        """
        self._require_sha(sha256)
        with self._lock:
            try:
                with open(os.path.join(self.blobs_dir, sha256), "rb") as handle:
                    data = handle.read()
            except FileNotFoundError:
                raise ObjectStoreError("not found: blob %s" % (sha256,))
            except OSError:
                raise ObjectStoreError("blob io error")
            if hashlib.sha256(data).hexdigest() != sha256:
                raise ObjectStoreError("corrupted blob")
            return data

    def blob_digests(self):
        """Return the sorted digests currently present in the blob directory."""
        with self._lock:
            return sorted(name for name in os.listdir(self.blobs_dir) if _is_sha256(name))

    # -- resumable uploads ----------------------------------------------
    def _session_paths(self, upload_id):
        base = os.path.join(self.uploads_dir, upload_id)
        return base + ".json", base + ".data"

    def _load_session(self, upload_id):
        """Return the validated session record for *upload_id*.

        The data file is reconciled with the recorded offset: bytes beyond it
        (left behind by an interrupted append) are truncated away, while a
        file shorter than the recorded offset means the session is corrupted.
        """
        if not isinstance(upload_id, str) or _UPLOAD_ID_RE.match(upload_id) is None:
            raise ObjectStoreError("unknown upload session: %r" % (upload_id,))
        meta_path, data_path = self._session_paths(upload_id)
        try:
            with open(meta_path, "rb") as handle:
                meta = json.loads(handle.read().decode("utf-8"))
        except FileNotFoundError:
            raise ObjectStoreError("unknown upload session: %r" % (upload_id,))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted upload session: %s" % (exc,))
        self._validate_session(meta)
        try:
            actual = os.path.getsize(data_path)
        except FileNotFoundError:
            actual = 0
        except OSError:
            raise ObjectStoreError("upload io error")
        if actual < meta["offset"]:
            raise ObjectStoreError("corrupted upload session: data shorter than offset")
        if actual > meta["offset"]:
            try:
                with open(data_path, "r+b") as handle:
                    handle.truncate(meta["offset"])
            except OSError:
                raise ObjectStoreError("upload io error")
        return meta

    @staticmethod
    def _validate_session(meta):
        if not isinstance(meta, dict):
            raise ObjectStoreError("corrupted upload session: not an object")
        key, size, sha = meta.get("key"), meta.get("size"), meta.get("sha256")
        content_type = meta.get("content_type")
        offset, completed = meta.get("offset"), meta.get("completed")
        result = meta.get("result")
        try:
            ContentAddressedStore._check_key(key)
        except ObjectStoreError:
            raise ObjectStoreError("corrupted upload session: bad key")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("corrupted upload session: bad size")
        if not _is_sha256(sha):
            raise ObjectStoreError("corrupted upload session: bad sha256")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("corrupted upload session: bad content_type")
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= size:
            raise ObjectStoreError("corrupted upload session: bad offset")
        if not isinstance(completed, bool):
            raise ObjectStoreError("corrupted upload session: bad completed flag")
        if completed:
            if not isinstance(result, dict) or result.get("sha256") != sha \
                    or result.get("size") != size or result.get("content_type") != content_type:
                raise ObjectStoreError("corrupted upload session: bad result")

    def _save_session(self, upload_id, meta):
        meta_path, _ = self._session_paths(upload_id)
        payload = json.dumps(meta, sort_keys=True, separators=(",", ":"))
        try:
            _atomic_write(meta_path, payload.encode("utf-8") + b"\n")
        except OSError:
            raise ObjectStoreError("upload io error")

    def begin_upload(self, key, size, sha256, content_type=None):
        """Open a resumable upload session and return its unique id.

        The declaration follows the same rules as :meth:`put`: *key* and
        *content_type* are validated exactly as there, *size* must be a
        non-negative integer (bools rejected) and *sha256* a 64-digit
        lowercase hex digest. Creating a session changes no objects; the
        session and its progress survive reopening the store directory.
        """
        self._check_key(key)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("invalid size: %r" % (size,))
        self._require_sha(sha256)
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        with self._lock:
            upload_id = uuid.uuid4().hex
            meta_path, data_path = self._session_paths(upload_id)
            while os.path.exists(meta_path):
                upload_id = uuid.uuid4().hex
                meta_path, data_path = self._session_paths(upload_id)
            try:
                os.remove(data_path)  # orphan from an interrupted begin/abort
            except FileNotFoundError:
                pass
            except OSError:
                raise ObjectStoreError("upload io error")
            try:
                with open(data_path, "xb"):
                    pass
            except OSError:
                raise ObjectStoreError("upload io error")
            meta = {"key": key, "size": size, "sha256": sha256,
                    "content_type": content_type, "offset": 0,
                    "completed": False, "result": None}
            self._save_session(upload_id, meta)
            return upload_id

    def upload_status(self, upload_id):
        """Return the declaration, received ``offset`` and ``completed`` flag."""
        with self._lock:
            meta = self._load_session(upload_id)
            return {"key": meta["key"], "size": meta["size"],
                    "sha256": meta["sha256"], "content_type": meta["content_type"],
                    "offset": meta["offset"], "completed": meta["completed"]}

    def append_upload(self, upload_id, offset, payload):
        """Append *payload* at *offset* and return the received length.

        A chunk must start exactly at the current end and may not grow the
        session past the declared size. Re-sending a byte range that lies
        entirely within the received prefix succeeds without growing when
        the bytes are identical, and fails otherwise; so does any overlap
        crossing the current end, any gap past it, and an empty chunk
        anywhere but exactly at the end. A failed append changes nothing.
        """
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ObjectStoreError("invalid offset: %r" % (offset,))
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        data = bytes(payload)
        with self._lock:
            meta = self._load_session(upload_id)
            if meta["completed"]:
                raise ObjectStoreError("upload already completed: %s" % (upload_id,))
            current = meta["offset"]
            if not data:
                if offset != current:
                    raise ObjectStoreError("upload conflict: empty chunk not at end")
                return current
            end = offset + len(data)
            if end <= current:
                _, data_path = self._session_paths(upload_id)
                try:
                    with open(data_path, "rb") as handle:
                        handle.seek(offset)
                        stored = handle.read(len(data))
                except OSError:
                    raise ObjectStoreError("upload io error")
                if stored != data:
                    raise ObjectStoreError("upload conflict: bytes differ")
                return current
            if offset != current:
                raise ObjectStoreError("upload conflict: chunk does not start at end")
            if end > meta["size"]:
                raise ObjectStoreError("upload conflict: exceeds declared size")
            _, data_path = self._session_paths(upload_id)
            try:
                with open(data_path, "ab") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                raise ObjectStoreError("upload io error")
            meta["offset"] = end
            self._save_session(upload_id, meta)
            return end

    def complete_upload(self, upload_id):
        """Publish the uploaded object and return its metadata entry.

        The object is published only when the received length and the
        SHA-256 of the full content both match the declaration (zero-byte
        objects included); otherwise :class:`ObjectStoreError` is raised and
        the session keeps its progress. Once completed, further appends
        fail and repeated completions return the first result without
        touching the index, so later writes or deletes of the key win.
        """
        with self._lock:
            meta = self._load_session(upload_id)
            if meta["completed"]:
                return dict(meta["result"])
            if meta["offset"] != meta["size"]:
                raise ObjectStoreError(
                    "incomplete upload: %d of %d bytes received"
                    % (meta["offset"], meta["size"]))
            _, data_path = self._session_paths(upload_id)
            try:
                with open(data_path, "rb") as handle:
                    data = handle.read()
            except FileNotFoundError:
                data = b""
            except OSError:
                raise ObjectStoreError("upload io error")
            if hashlib.sha256(data).hexdigest() != meta["sha256"]:
                raise ObjectStoreError("hash mismatch: content does not match declaration")
            entry = {"sha256": meta["sha256"], "size": meta["size"],
                     "content_type": meta["content_type"]}
            self._store_blob(meta["sha256"], data)
            self._index["objects"][meta["key"]] = entry
            self._save_index()
            meta["completed"] = True
            meta["result"] = entry
            self._save_session(upload_id, meta)
            return dict(entry)

    def abort_upload(self, upload_id):
        """Drop the session *upload_id*; published objects are left alone."""
        with self._lock:
            self._load_session(upload_id)
            meta_path, data_path = self._session_paths(upload_id)
            for path in (meta_path, data_path):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError:
                    raise ObjectStoreError("upload io error")
            return None

    def collect_garbage(self, dry_run=True):
        """Collect unreferenced blobs, previewing by default.

        Returns ``{"digests": [...], "bytes": <int>, "dry_run": <bool>}``. The
        digests are sorted lexicographically and bytes is the sum of the actual
        file sizes; a dry run reports the candidates without touching storage,
        an actual run reports the files deleted by that call. Only regular
        files directly in the blobs directory whose names are 64 lowercase hex
        digits are eligible: subdirectories, symlinks, temporary files and any
        other names are left untouched, and symlink targets are never opened.

        References are recomputed from the current object index on every call,
        including digest sharing between keys. The whole operation runs under
        the store lock, so blobs published concurrently stay referenced and
        candidates that vanished between the scan and deletion are skipped
        rather than counted.
        """
        if not isinstance(dry_run, bool):
            raise ObjectStoreError("invalid dry_run")
        with self._lock:
            referenced = {
                entry["sha256"] for entry in self._index["objects"].values()
            }
            try:
                names = os.listdir(self.blobs_dir)
            except OSError:
                raise ObjectStoreError("gc io error")
            candidates = []
            for name in names:
                if not _is_sha256(name) or name in referenced:
                    continue
                path = os.path.join(self.blobs_dir, name)
                try:
                    info = os.lstat(path)
                except FileNotFoundError:
                    continue
                except OSError:
                    raise ObjectStoreError("gc io error")
                if not stat.S_ISREG(info.st_mode):
                    continue
                candidates.append((name, info.st_size))
            candidates.sort(key=lambda item: item[0])
            if dry_run:
                return {
                    "digests": [name for name, _ in candidates],
                    "bytes": sum(size for _, size in candidates),
                    "dry_run": True,
                }
            deleted = []
            freed = 0
            for name, size in candidates:
                path = os.path.join(self.blobs_dir, name)
                try:
                    os.remove(path)
                except FileNotFoundError:
                    continue
                except OSError:
                    raise ObjectStoreError("gc io error")
                deleted.append(name)
                freed += size
            return {"digests": deleted, "bytes": freed, "dry_run": False}

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

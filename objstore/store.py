"""Content-addressed object store (standard library only).

Blob bytes are stored once as ``<root>/blobs/<sha256>``; object metadata
(``key -> {sha256, size, content_type}``) lives in ``<root>/index.json`` and is
always rewritten through a temporary file plus ``os.replace``. Every mutation
and every consistent read runs under a single re-entrant lock.

Resumable uploads live under ``<root>/uploads/`` (created lazily): each
session is a ``<session>.json`` declaration plus confirmed offset and a
``<session>.part`` file holding the received bytes. Completing a session
publishes the object and records the result in the index's ``uploads`` map in
one atomic index rewrite, so the completion flag can never disagree with the
published object.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import uuid

__all__ = ["ContentAddressedStore", "ObjectStoreError", "SessionConflict"]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
UPLOADS_DIRNAME = "uploads"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_SESSION_META_RE = re.compile(r"\A([0-9a-f]{32})\.json\Z")
_MISSING = object()


class ObjectStoreError(Exception):
    """Missing object, invalid key/digest/limit, or a corrupted index."""


class SessionConflict(ObjectStoreError):
    """An upload operation that violates the session's current state.

    A subclass of :class:`ObjectStoreError`, so existing callers keep
    catching it; front ends (HTTP) can nevertheless distinguish these
    state conflicts from bad input and report them separately.
    """


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
        self._index, self._upload_records = self._load_index()
        self._uploads = self._load_uploads()

    # -- index ---------------------------------------------------------
    def _load_index(self):
        if not os.path.exists(self.index_path):
            return {"version": INDEX_VERSION, "objects": {}}, {}
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
        records = {}
        raw_records = document.get("uploads", {})
        if not isinstance(raw_records, dict):
            raise ObjectStoreError("corrupted index: uploads is not a map")
        for session_id, record in raw_records.items():
            if not isinstance(session_id, str) or not isinstance(record, dict):
                raise ObjectStoreError("corrupted index: bad upload record")
            key = record.get("key")
            if not isinstance(key, str):
                raise ObjectStoreError("corrupted index: bad upload record")
            records[session_id] = {
                "key": key,
                "entry": self._validate_entry(key, record.get("entry")),
            }
        return {"version": INDEX_VERSION, "objects": objects}, records

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
        document = {"version": INDEX_VERSION, "objects": self._index["objects"]}
        if self._upload_records:
            document["uploads"] = self._upload_records
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"))
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
    def _session_meta_path(self, session_id):
        return os.path.join(self.uploads_dir, session_id + ".json")

    def _session_part_path(self, session_id):
        return os.path.join(self.uploads_dir, session_id + ".part")

    def _load_uploads(self):
        uploads = {}
        try:
            names = os.listdir(self.uploads_dir)
        except FileNotFoundError:
            return uploads
        except OSError:
            raise ObjectStoreError("upload io error")
        for name in sorted(names):
            match = _SESSION_META_RE.match(name)
            if match is None:
                continue
            session_id = match.group(1)
            if session_id in self._upload_records:
                # completed before a crash could clean its files up
                self._discard_session_files(session_id)
                continue
            session = self._read_session_meta(session_id)
            self._check_part_file(session_id, session["offset"])
            uploads[session_id] = session
        return uploads

    def _read_session_meta(self, session_id):
        try:
            with open(self._session_meta_path(session_id), "rb") as handle:
                document = json.loads(handle.read().decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted upload session: %s" % (exc,))
        if not isinstance(document, dict):
            raise ObjectStoreError("corrupted upload session: not an object")
        key = document.get("key")
        size = document.get("size")
        sha = document.get("sha256")
        content_type = document.get("content_type")
        offset = document.get("offset")
        try:
            self._check_key(key)
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
        return {"key": key, "size": size, "sha256": sha,
                "content_type": content_type, "offset": offset}

    def _check_part_file(self, session_id, offset):
        """Ensure the part file holds exactly the confirmed *offset* bytes.

        A crash between the chunk write and the metadata update can leave
        unconfirmed bytes behind; they are truncated so recovery sees either
        the old progress or a whole confirmed chunk, never a partial one.
        """
        path = self._session_part_path(session_id)
        try:
            size_on_disk = os.stat(path).st_size
        except FileNotFoundError:
            if offset:
                raise ObjectStoreError("corrupted upload session: missing data")
            try:
                with open(path, "wb"):
                    pass
            except OSError:
                raise ObjectStoreError("upload io error")
            return
        except OSError:
            raise ObjectStoreError("upload io error")
        if size_on_disk < offset:
            raise ObjectStoreError("corrupted upload session: truncated data")
        if size_on_disk > offset:
            try:
                with open(path, "r+b") as handle:
                    handle.truncate(offset)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                raise ObjectStoreError("upload io error")

    def _save_session(self, session_id, session):
        payload = json.dumps(session, sort_keys=True, separators=(",", ":"))
        try:
            _atomic_write(self._session_meta_path(session_id),
                          payload.encode("utf-8") + b"\n")
        except OSError:
            raise ObjectStoreError("upload io error")

    def _write_part(self, session_id, offset, data):
        try:
            with open(self._session_part_path(session_id), "r+b") as handle:
                handle.seek(offset)
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError:
            raise ObjectStoreError("upload io error")

    def _read_part(self, session_id, offset, length):
        try:
            with open(self._session_part_path(session_id), "rb") as handle:
                handle.seek(offset)
                return handle.read(length)
        except OSError:
            raise ObjectStoreError("upload io error")

    def _remove_session_files(self, session_id):
        for path in (self._session_meta_path(session_id),
                     self._session_part_path(session_id)):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            except OSError:
                raise ObjectStoreError("upload io error")
        self._tidy_uploads_dir()

    def _discard_session_files(self, session_id):
        for path in (self._session_meta_path(session_id),
                     self._session_part_path(session_id)):
            try:
                os.remove(path)
            except OSError:
                pass
        self._tidy_uploads_dir()

    def _tidy_uploads_dir(self):
        try:
            os.rmdir(self.uploads_dir)
        except OSError:
            pass

    def _new_session_id(self):
        while True:
            session_id = uuid.uuid4().hex
            if session_id in self._uploads or session_id in self._upload_records:
                continue
            if os.path.exists(self._session_meta_path(session_id)):
                continue
            return session_id

    @staticmethod
    def _check_session_id(session_id):
        if not isinstance(session_id, str) or not session_id:
            raise ObjectStoreError("invalid upload session: %r" % (session_id,))
        return session_id

    def _require_active_session(self, session_id):
        self._check_session_id(session_id)
        if session_id in self._upload_records:
            raise SessionConflict(
                "upload session already completed: %s" % (session_id,))
        session = self._uploads.get(session_id)
        if session is None:
            raise ObjectStoreError("unknown upload session: %r" % (session_id,))
        return session

    def begin_upload(self, key, size, sha256, content_type=None):
        """Start a resumable upload session and return its unique id.

        The key and content type follow the same rules as :meth:`put`, *size*
        must be a non-negative integer (booleans excluded) and *sha256* the
        64 lowercase hex digits of the full content's digest. Creating a
        session changes no object; the session and its progress survive
        reopening the data directory.
        """
        self._check_key(key)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("invalid size: %r" % (size,))
        self._require_sha(sha256)
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        session = {"key": key, "size": size, "sha256": sha256,
                   "content_type": content_type, "offset": 0}
        with self._lock:
            session_id = self._new_session_id()
            try:
                os.makedirs(self.uploads_dir, exist_ok=True)
                with open(self._session_part_path(session_id), "wb"):
                    pass
            except OSError:
                raise ObjectStoreError("upload io error")
            try:
                self._save_session(session_id, session)
            except ObjectStoreError:
                self._discard_session_files(session_id)
                raise
            self._uploads[session_id] = session
            return session_id

    def upload_status(self, session_id):
        """Return the declaration, received ``offset`` and ``completed`` flag."""
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                entry = record["entry"]
                return {"key": record["key"], "size": entry["size"],
                        "sha256": entry["sha256"],
                        "content_type": entry["content_type"],
                        "offset": entry["size"], "completed": True}
            session = self._uploads.get(session_id)
            if session is None:
                raise ObjectStoreError("unknown upload session: %r" % (session_id,))
            return {"key": session["key"], "size": session["size"],
                    "sha256": session["sha256"],
                    "content_type": session["content_type"],
                    "offset": session["offset"], "completed": False}

    def append_upload(self, session_id, offset, payload):
        """Append one chunk to *session_id* and return the received length.

        A new chunk must start exactly at the current end and may not exceed
        the declared total size. A chunk lying fully inside the received
        range succeeds without growing when its bytes are identical to what
        is stored, and fails otherwise; chunks overlapping the end, chunks
        past the end and empty chunks anywhere but at the end all fail.
        """
        self._check_session_id(session_id)
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ObjectStoreError("invalid offset: %r" % (offset,))
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        data = bytes(payload)
        with self._lock:
            session = self._require_active_session(session_id)
            current = session["offset"]
            if offset > current:
                raise SessionConflict(
                    "append gap: offset %d beyond received %d" % (offset, current))
            if not data:
                if offset != current:
                    raise SessionConflict("empty chunk only at received end")
                return current
            end = offset + len(data)
            if offset < current:
                if end > current:
                    raise SessionConflict("append overlaps received end")
                if self._read_part(session_id, offset, len(data)) != data:
                    raise SessionConflict("append conflicts with received bytes")
                return current
            if end > session["size"]:
                raise SessionConflict("append exceeds declared size")
            self._write_part(session_id, offset, data)
            self._save_session(session_id, dict(session, offset=end))
            session["offset"] = end
            return end

    def complete_upload(self, session_id):
        """Publish the object once the received content matches the declaration.

        The total length and the SHA-256 of the full content must match the
        declared values (zero-byte objects included); only then is the object
        published and the put-style metadata returned. Repeating a completed
        call returns the first result without touching the key again, so
        later writes or deletes of the key are never overwritten.
        """
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                return dict(record["entry"])
            session = self._uploads.get(session_id)
            if session is None:
                raise ObjectStoreError("unknown upload session: %r" % (session_id,))
            if session["offset"] != session["size"]:
                raise SessionConflict(
                    "upload incomplete: %d of %d bytes received"
                    % (session["offset"], session["size"]))
            data = self._read_part(session_id, 0, session["size"])
            if len(data) != session["size"]:
                raise ObjectStoreError("upload io error")
            if hashlib.sha256(data).hexdigest() != session["sha256"]:
                raise SessionConflict("upload hash mismatch")
            entry = {"sha256": session["sha256"], "size": session["size"],
                     "content_type": session["content_type"]}
            self._store_blob(session["sha256"], data)
            previous = self._index["objects"].get(session["key"], _MISSING)
            self._index["objects"][session["key"]] = entry
            self._upload_records[session_id] = {
                "key": session["key"], "entry": dict(entry),
            }
            try:
                self._save_index()
            except OSError:
                del self._upload_records[session_id]
                if previous is _MISSING:
                    del self._index["objects"][session["key"]]
                else:
                    self._index["objects"][session["key"]] = previous
                raise ObjectStoreError("upload io error")
            del self._uploads[session_id]
            self._discard_session_files(session_id)
            return dict(entry)

    def abort_upload(self, session_id):
        """Drop *session_id* and return None; published objects are kept."""
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                del self._upload_records[session_id]
                try:
                    self._save_index()
                except OSError:
                    self._upload_records[session_id] = record
                    raise ObjectStoreError("upload io error")
                self._discard_session_files(session_id)
                return None
            session = self._uploads.get(session_id)
            if session is None:
                raise ObjectStoreError("unknown upload session: %r" % (session_id,))
            self._remove_session_files(session_id)
            del self._uploads[session_id]
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

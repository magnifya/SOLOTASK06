"""Content-addressed object store (standard library only).

Blob bytes are stored once as ``<root>/blobs/<sha256>``; object metadata
(``key -> {sha256, size, content_type}``) lives in ``<root>/index.json`` and is
always rewritten through a temporary file plus ``os.replace``. Every mutation
and every consistent read runs under a single re-entrant lock.

Objects are namespaced per tenant: every object and upload operation takes an
optional ``tenant`` keyword defaulting to ``"default"``, and the same key in
different tenants refers to independent objects. In the index the default
tenant keeps the historical flat ``objects`` map while other tenants are
stored under a ``tenants`` map, so directories written by older versions
silently belong to the default tenant. Blobs stay global: identical content
is stored once no matter which tenants reference it, and garbage collection
considers the references of every tenant.

Resumable uploads live under ``<root>/uploads/`` (created lazily): each
session is a ``<session>.json`` declaration plus confirmed offset and a
``<session>.part`` file holding the received bytes. A session belongs to the
tenant fixed at creation; operations naming another tenant behave exactly as
if the session did not exist. Completing a session publishes the object in
the session's tenant and records the result in the index's ``uploads`` map in
one atomic index rewrite, so the completion flag can never disagree with the
published object.

Per-tenant object versioning is off by default and only affects new writes:
while enabled for a tenant, every ``put`` and every completed resumable
upload additionally appends an immutable version record (``version_id``,
digest, size, content type and UTC ``created_at``) to the key's version list
in the index's ``versions`` map and makes it the current version. ``get``,
``head`` and ``list_objects`` keep returning only the current version, and
``delete`` only removes the current pointer — the recorded history stays
readable through ``list_versions``/``get_version`` until it is explicitly
removed with ``delete_version`` or reclaimed by ``purge_retention`` under the
tenant's saved retention policy (``retention`` map: ``max_versions`` and/or
``max_age_seconds``). Disabling versioning never deletes history, and indexes
written by older versions simply have no version records. Garbage collection
treats blobs referenced by any version record, any current object and any
incomplete upload session as live.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import uuid
from datetime import datetime, timezone

__all__ = ["ContentAddressedStore", "ObjectStoreError", "SessionConflict"]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
UPLOADS_DIRNAME = "uploads"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
DEFAULT_TENANT = "default"

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_SESSION_META_RE = re.compile(r"\A([0-9a-f]{32})\.json\Z")
_TENANT_RE = re.compile(r"\A[a-z0-9][a-z0-9_-]{0,63}\Z")
_VERSION_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")
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


def _is_version_id(value):
    return isinstance(value, str) and _VERSION_ID_RE.match(value) is not None


def _utc_now():
    """Return the current UTC time as an ISO 8601 string with offset."""
    return datetime.now(timezone.utc).isoformat()


def _parse_created_at(value):
    """Parse a stored ISO 8601 timestamp, returning an aware datetime or None."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


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
        (self._objects, self._upload_records, self._versioning,
         self._versions, self._retention) = self._load_index()
        self._uploads = self._load_uploads()

    # -- index ---------------------------------------------------------
    def _load_index(self):
        if not os.path.exists(self.index_path):
            return {DEFAULT_TENANT: {}}, {}, set(), {}, {}
        try:
            with open(self.index_path, "rb") as handle:
                document = json.loads(handle.read().decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted index: %s" % (exc,))
        if not isinstance(document, dict) or not isinstance(document.get("objects"), dict):
            raise ObjectStoreError("corrupted index: missing objects map")
        objects = {DEFAULT_TENANT: self._load_object_map(document["objects"])}
        raw_tenants = document.get("tenants", {})
        if not isinstance(raw_tenants, dict):
            raise ObjectStoreError("corrupted index: tenants is not a map")
        for tenant, mapping in raw_tenants.items():
            if not isinstance(tenant, str) or tenant == DEFAULT_TENANT \
                    or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad tenant")
            if not isinstance(mapping, dict):
                raise ObjectStoreError("corrupted index: bad tenant objects")
            objects[tenant] = self._load_object_map(mapping)
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
            tenant = record.get("tenant", DEFAULT_TENANT)
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad upload record")
            records[session_id] = {
                "key": key,
                "entry": self._validate_entry(key, record.get("entry")),
                "tenant": tenant,
            }
            version_id = record.get("version_id")
            if version_id is not None:
                if not _is_version_id(version_id):
                    raise ObjectStoreError("corrupted index: bad upload record")
                records[session_id]["version_id"] = version_id
        versioning = self._load_versioning(document.get("versioning", {}))
        versions = self._load_versions(document.get("versions", {}))
        retention = self._load_retention(document.get("retention", {}))
        return objects, records, versioning, versions, retention

    @staticmethod
    def _check_stored_tenant(tenant):
        if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
            raise ObjectStoreError("corrupted index: bad tenant")
        return tenant

    def _load_versioning(self, raw_versioning):
        if not isinstance(raw_versioning, dict):
            raise ObjectStoreError("corrupted index: versioning is not a map")
        versioning = set()
        for tenant, flag in raw_versioning.items():
            self._check_stored_tenant(tenant)
            if not isinstance(flag, bool):
                raise ObjectStoreError("corrupted index: bad versioning flag")
            if flag:
                versioning.add(tenant)
        return versioning

    def _load_retention(self, raw_retention):
        if not isinstance(raw_retention, dict):
            raise ObjectStoreError("corrupted index: retention is not a map")
        retention = {}
        for tenant, policy in raw_retention.items():
            self._check_stored_tenant(tenant)
            if not isinstance(policy, dict):
                raise ObjectStoreError("corrupted index: bad retention policy")
            cleaned = {}
            for field in ("max_versions", "max_age_seconds"):
                if field not in policy:
                    continue
                value = policy[field]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ObjectStoreError("corrupted index: bad retention policy")
                cleaned[field] = value
            if not cleaned:
                raise ObjectStoreError("corrupted index: empty retention policy")
            retention[tenant] = cleaned
        return retention

    def _load_versions(self, raw_versions):
        if not isinstance(raw_versions, dict):
            raise ObjectStoreError("corrupted index: versions is not a map")
        versions = {}
        for tenant, keymap in raw_versions.items():
            self._check_stored_tenant(tenant)
            if not isinstance(keymap, dict):
                raise ObjectStoreError("corrupted index: bad tenant versions")
            loaded = {}
            for key, info in keymap.items():
                if not isinstance(key, str):
                    raise ObjectStoreError("corrupted index: non-string key")
                loaded[key] = self._validate_version_info(key, info)
            if loaded:
                versions[tenant] = loaded
        return versions

    @staticmethod
    def _validate_version_info(key, info):
        if not isinstance(info, dict):
            raise ObjectStoreError(
                "corrupted index: versions for %r is not an object" % (key,))
        raw_records = info.get("versions")
        if not isinstance(raw_records, list) or not raw_records:
            raise ObjectStoreError(
                "corrupted index: bad version list for %r" % (key,))
        records = []
        for record in raw_records:
            if not isinstance(record, dict):
                raise ObjectStoreError(
                    "corrupted index: bad version record for %r" % (key,))
            version_id = record.get("version_id")
            if not _is_version_id(version_id):
                raise ObjectStoreError(
                    "corrupted index: bad version_id for %r" % (key,))
            created_at = record.get("created_at")
            if not isinstance(created_at, str) or _parse_created_at(created_at) is None:
                raise ObjectStoreError(
                    "corrupted index: bad created_at for %r" % (key,))
            entry = ContentAddressedStore._validate_entry(key, record)
            records.append({"version_id": version_id, "sha256": entry["sha256"],
                            "size": entry["size"],
                            "content_type": entry["content_type"],
                            "created_at": created_at})
        current = info.get("current")
        if current is not None:
            if not _is_version_id(current) \
                    or current not in {record["version_id"] for record in records}:
                raise ObjectStoreError(
                    "corrupted index: bad current version for %r" % (key,))
        return {"current": current, "versions": records}

    def _load_object_map(self, mapping):
        objects = {}
        for key, entry in mapping.items():
            if not isinstance(key, str):
                raise ObjectStoreError("corrupted index: non-string key")
            objects[key] = self._validate_entry(key, entry)
        return objects

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
        document = {"version": INDEX_VERSION,
                    "objects": self._objects.get(DEFAULT_TENANT, {})}
        tenants = {tenant: mapping for tenant, mapping in self._objects.items()
                   if tenant != DEFAULT_TENANT and mapping}
        if tenants:
            document["tenants"] = tenants
        if self._upload_records:
            records = {}
            for session_id, record in self._upload_records.items():
                saved = {"key": record["key"], "entry": record["entry"]}
                if record["tenant"] != DEFAULT_TENANT:
                    saved["tenant"] = record["tenant"]
                if record.get("version_id") is not None:
                    saved["version_id"] = record["version_id"]
                records[session_id] = saved
            document["uploads"] = records
        if self._versioning:
            document["versioning"] = {
                tenant: True for tenant in sorted(self._versioning)}
        if self._retention:
            document["retention"] = {
                tenant: self._retention[tenant]
                for tenant in sorted(self._retention)
            }
        saved_versions = {}
        for tenant in sorted(self._versions):
            keymap = self._versions[tenant]
            if keymap:
                saved_versions[tenant] = {
                    key: {"current": info["current"], "versions": info["versions"]}
                    for key, info in keymap.items()
                }
        if saved_versions:
            document["versions"] = saved_versions
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"))
        _atomic_write(self.index_path, payload.encode("utf-8") + b"\n")

    # -- validation ----------------------------------------------------
    @staticmethod
    def _check_tenant(tenant):
        if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
            raise ObjectStoreError("invalid tenant: %r" % (tenant,))
        return tenant

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

    @staticmethod
    def _check_precondition(expected_sha256):
        """Normalize an ``expected_sha256`` keyword argument.

        Omitted (the ``_MISSING`` sentinel) means no condition; an explicit
        ``None`` means the key must not exist; a 64-digit lowercase hex string
        means the key must exist with that exact digest. Anything else is an
        ``invalid precondition`` error rather than a failed condition.
        """
        if expected_sha256 is _MISSING:
            return _MISSING
        if expected_sha256 is None:
            return None
        if _is_sha256(expected_sha256):
            return expected_sha256
        raise ObjectStoreError("invalid precondition")

    def _precondition_met(self, tenant, key, expected):
        """Return whether *key* in *tenant* currently satisfies *expected*."""
        entry = self._objects.get(tenant, {}).get(key)
        if expected is None:
            return entry is None
        return entry is not None and entry["sha256"] == expected

    # -- object API ----------------------------------------------------
    def put(self, key, payload, content_type=None, expected_sha256=_MISSING,
            tenant=DEFAULT_TENANT):
        """Store *payload* under *key* in *tenant* and return its metadata.

        *expected_sha256* is an atomic precondition on the key's current
        digest in the same tenant: omitted leaves the old overwrite
        behaviour, an explicit ``None`` requires the key not to exist, and a
        64 lowercase hex digest requires the key to exist with exactly that
        digest. Content type and other keys are not compared. Objects of
        other tenants are never consulted or touched.

        When versioning is enabled for *tenant* the write also appends an
        immutable version record to the key's history and the returned
        metadata carries its ``version_id``; when disabled the behaviour and
        the returned metadata are exactly as before.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        expected = self._check_precondition(expected_sha256)
        data = bytes(payload)
        sha = hashlib.sha256(data).hexdigest()
        entry = {"sha256": sha, "size": len(data), "content_type": content_type}
        with self._lock:
            if expected is not _MISSING and not self._precondition_met(
                    tenant, key, expected):
                raise ObjectStoreError("precondition failed")
            self._store_blob(sha, data)
            self._objects.setdefault(tenant, {})[key] = entry
            version = self._record_write(tenant, key, entry)
            self._save_index()
            result = dict(entry)
            if version is not None:
                result["version_id"] = version["version_id"]
            return result

    def _record_write(self, tenant, key, entry):
        """Update version history for a write of *entry* to *key*.

        With versioning enabled a new immutable version is appended and made
        current; with it disabled no version is recorded and any recorded
        history simply stops being the current version (it is never deleted
        implicitly). Returns the new version record or ``None``.
        """
        if tenant not in self._versioning:
            info = self._versions.get(tenant, {}).get(key)
            if info is not None:
                info["current"] = None
            return None
        version = {"version_id": uuid.uuid4().hex, "sha256": entry["sha256"],
                   "size": entry["size"], "content_type": entry["content_type"],
                   "created_at": _utc_now()}
        keymap = self._versions.setdefault(tenant, {})
        info = keymap.get(key)
        if info is None:
            info = {"current": None, "versions": []}
            keymap[key] = info
        info["versions"].append(version)
        info["current"] = version["version_id"]
        return version

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

    def get(self, key, tenant=DEFAULT_TENANT):
        """Return ``(payload, entry)`` for *key* in *tenant*."""
        self._check_tenant(tenant)
        with self._lock:
            entry = self.head(key, tenant=tenant)
            data = self.blob(entry["sha256"])
            if len(data) != entry["size"]:
                raise ObjectStoreError("corrupted blob")
            return data, entry

    def head(self, key, tenant=DEFAULT_TENANT):
        """Return a copy of the metadata entry for *key* in *tenant*."""
        self._check_tenant(tenant)
        self._check_key(key)
        with self._lock:
            entry = self._objects.get(tenant, {}).get(key)
            if entry is None:
                raise ObjectStoreError("not found: %s" % (key,))
            return dict(entry)

    def delete(self, key, expected_sha256=_MISSING, tenant=DEFAULT_TENANT):
        """Drop *key* in *tenant*; shared blobs stay on disk for other keys.

        With *expected_sha256*, deletion also requires the key's current
        digest in this tenant to match (or an explicit ``None`` requires the
        key absent); a satisfied absent-condition delete still reports the
        key as not found. Objects other tenants keep under the same key are
        unaffected.

        With versioning enabled the delete only removes the current pointer:
        the recorded version history is kept (and stays readable through the
        version operations) but no version is current anymore, so the key
        disappears from ``get``/``head``/``list_objects`` until the next
        write.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        expected = self._check_precondition(expected_sha256)
        with self._lock:
            objects = self._objects.get(tenant, {})
            if key not in objects:
                raise ObjectStoreError("not found: %s" % (key,))
            if expected is not _MISSING and not self._precondition_met(
                    tenant, key, expected):
                raise ObjectStoreError("precondition failed")
            del objects[key]
            info = self._versions.get(tenant, {}).get(key)
            if info is not None:
                info["current"] = None
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
        expected = document.get("expected_sha256", _MISSING)
        tenant = document.get("tenant", DEFAULT_TENANT)
        try:
            self._check_key(key)
        except ObjectStoreError:
            raise ObjectStoreError("corrupted upload session: bad key")
        if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
            raise ObjectStoreError("corrupted upload session: bad tenant")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("corrupted upload session: bad size")
        if not _is_sha256(sha):
            raise ObjectStoreError("corrupted upload session: bad sha256")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("corrupted upload session: bad content_type")
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= size:
            raise ObjectStoreError("corrupted upload session: bad offset")
        # Sessions written by older versions have no condition; an explicit
        # null requires the key absent, a string requires that digest.
        if expected is not _MISSING and expected is not None and not _is_sha256(expected):
            raise ObjectStoreError("corrupted upload session: bad precondition")
        return {"key": key, "size": size, "sha256": sha,
                "content_type": content_type, "offset": offset,
                "expected_sha256": expected, "tenant": tenant}

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
        # The default tenant is implicit so older session files stay
        # byte-identical; every other tenant is recorded explicitly.
        document = {name: value for name, value in session.items()
                    if value is not _MISSING
                    and not (name == "tenant" and value == DEFAULT_TENANT)}
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"))
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

    def _require_active_session(self, session_id, tenant):
        self._check_session_id(session_id)
        record = self._upload_records.get(session_id)
        if record is not None and record["tenant"] == tenant:
            raise SessionConflict(
                "upload session already completed: %s" % (session_id,))
        session = self._uploads.get(session_id)
        if session is None or session["tenant"] != tenant:
            # Sessions of other tenants are indistinguishable from unknown
            # ones: no state or progress leaks across tenant boundaries.
            raise ObjectStoreError("unknown upload session: %r" % (session_id,))
        return session

    def begin_upload(self, key, size, sha256, content_type=None,
                     expected_sha256=_MISSING, tenant=DEFAULT_TENANT):
        """Start a resumable upload session and return its unique id.

        The key and content type follow the same rules as :meth:`put`, *size*
        must be a non-negative integer (booleans excluded) and *sha256* the
        64 lowercase hex digits of the full content's digest. Creating a
        session changes no object; the session and its progress survive
        reopening the data directory. The session belongs to *tenant* for its
        whole lifetime: every later operation must name the same tenant.

        *expected_sha256* saves the same kind of precondition as :meth:`put`;
        it is checked once, atomically with the publish, when the session
        completes, never while the session is created or appended to.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("invalid size: %r" % (size,))
        self._require_sha(sha256)
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        expected = self._check_precondition(expected_sha256)
        session = {"key": key, "size": size, "sha256": sha256,
                   "content_type": content_type, "offset": 0,
                   "expected_sha256": expected, "tenant": tenant}
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

    def upload_status(self, session_id, tenant=DEFAULT_TENANT):
        """Return the declaration, received ``offset`` and ``completed`` flag.

        Only the session's owning tenant sees it; every other tenant gets
        the same ``unknown upload session`` error as for a session that
        never existed.
        """
        self._check_tenant(tenant)
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                if record["tenant"] != tenant:
                    raise ObjectStoreError(
                        "unknown upload session: %r" % (session_id,))
                entry = record["entry"]
                return {"key": record["key"], "size": entry["size"],
                        "sha256": entry["sha256"],
                        "content_type": entry["content_type"],
                        "offset": entry["size"], "completed": True}
            session = self._uploads.get(session_id)
            if session is None or session["tenant"] != tenant:
                raise ObjectStoreError("unknown upload session: %r" % (session_id,))
            return {"key": session["key"], "size": session["size"],
                    "sha256": session["sha256"],
                    "content_type": session["content_type"],
                    "offset": session["offset"], "completed": False}

    def append_upload(self, session_id, offset, payload, tenant=DEFAULT_TENANT):
        """Append one chunk to *session_id* and return the received length.

        A new chunk must start exactly at the current end and may not exceed
        the declared total size. A chunk lying fully inside the received
        range succeeds without growing when its bytes are identical to what
        is stored, and fails otherwise; chunks overlapping the end, chunks
        past the end and empty chunks anywhere but at the end all fail.
        """
        self._check_tenant(tenant)
        self._check_session_id(session_id)
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ObjectStoreError("invalid offset: %r" % (offset,))
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ObjectStoreError("payload must be bytes")
        data = bytes(payload)
        with self._lock:
            session = self._require_active_session(session_id, tenant)
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

    def complete_upload(self, session_id, tenant=DEFAULT_TENANT):
        """Publish the object once the received content matches the declaration.

        The total length and the SHA-256 of the full content must match the
        declared values (zero-byte objects included); only then is the
        session's saved precondition checked against the owning tenant's
        object state at publish time and the object published in that
        tenant, all as one indivisible step. A failed precondition raises
        without changing the object, the session's confirmed bytes or any
        blob, so the session can be completed again (or aborted) later.
        Repeating a completed call returns the first result without touching
        the key again, so later writes or deletes of the key are never
        overwritten. When versioning is enabled for the owning tenant the
        publish also appends an immutable version record and the returned
        metadata (including later repeats) carries its ``version_id``.
        """
        self._check_tenant(tenant)
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                if record["tenant"] != tenant:
                    raise ObjectStoreError(
                        "unknown upload session: %r" % (session_id,))
                result = dict(record["entry"])
                if record.get("version_id") is not None:
                    result["version_id"] = record["version_id"]
                return result
            session = self._uploads.get(session_id)
            if session is None or session["tenant"] != tenant:
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
            expected = session.get("expected_sha256", _MISSING)
            if expected is not _MISSING and not self._precondition_met(
                    tenant, session["key"], expected):
                raise ObjectStoreError("precondition failed")
            entry = {"sha256": session["sha256"], "size": session["size"],
                     "content_type": session["content_type"]}
            self._store_blob(session["sha256"], data)
            objects = self._objects.setdefault(tenant, {})
            previous = objects.get(session["key"], _MISSING)
            objects[session["key"]] = entry
            had_keymap = tenant in self._versions
            vinfo = self._versions.get(tenant, {}).get(session["key"])
            v_snapshot = None if vinfo is None else (
                list(vinfo["versions"]), vinfo["current"])
            version = self._record_write(tenant, session["key"], entry)
            record = {"key": session["key"], "entry": dict(entry),
                      "tenant": tenant}
            if version is not None:
                record["version_id"] = version["version_id"]
            self._upload_records[session_id] = record
            try:
                self._save_index()
            except OSError:
                del self._upload_records[session_id]
                if previous is _MISSING:
                    del objects[session["key"]]
                else:
                    objects[session["key"]] = previous
                self._restore_write(tenant, session["key"], v_snapshot,
                                    had_keymap)
                raise ObjectStoreError("upload io error")
            del self._uploads[session_id]
            self._discard_session_files(session_id)
            result = dict(entry)
            if version is not None:
                result["version_id"] = version["version_id"]
            return result

    def _restore_write(self, tenant, key, snapshot, had_keymap):
        """Undo a ``_record_write`` after a failed index save."""
        if snapshot is not None:
            info = self._versions.get(tenant, {}).get(key)
            if info is not None:
                info["versions"], info["current"] = snapshot
            return
        keymap = self._versions.get(tenant)
        if keymap is not None:
            keymap.pop(key, None)
            if not keymap and not had_keymap:
                del self._versions[tenant]

    def abort_upload(self, session_id, tenant=DEFAULT_TENANT):
        """Drop *session_id* and return None; published objects are kept."""
        self._check_tenant(tenant)
        self._check_session_id(session_id)
        with self._lock:
            record = self._upload_records.get(session_id)
            if record is not None:
                if record["tenant"] != tenant:
                    raise ObjectStoreError(
                        "unknown upload session: %r" % (session_id,))
                del self._upload_records[session_id]
                try:
                    self._save_index()
                except OSError:
                    self._upload_records[session_id] = record
                    raise ObjectStoreError("upload io error")
                self._discard_session_files(session_id)
                return None
            session = self._uploads.get(session_id)
            if session is None or session["tenant"] != tenant:
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

        References are recomputed from the current object index on every
        call, across every tenant and including digest sharing between keys.
        Blobs referenced by recorded object versions or declared by
        incomplete upload sessions are live as well; a version's blob becomes
        collectable only after the version record is removed (by
        ``delete_version`` or ``purge_retention``). The whole operation runs
        under the store lock, so blobs published
        concurrently stay referenced and candidates that vanished between
        the scan and deletion are skipped rather than counted.
        """
        if not isinstance(dry_run, bool):
            raise ObjectStoreError("invalid dry_run")
        with self._lock:
            referenced = {
                entry["sha256"]
                for objects in self._objects.values()
                for entry in objects.values()
            }
            for keymap in self._versions.values():
                for info in keymap.values():
                    for record in info["versions"]:
                        referenced.add(record["sha256"])
            for session in self._uploads.values():
                referenced.add(session["sha256"])
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

    def list_objects(self, prefix="", after=None, limit=DEFAULT_LIMIT,
                     tenant=DEFAULT_TENANT):
        """Return ``{"items": [...], "next_after": <str|null>}`` for one page.

        Only *tenant*'s objects are filtered, paginated and returned, with
        the plain object keys as names; a tenant without objects yields an
        empty page.
        """
        self._check_tenant(tenant)
        if not isinstance(prefix, str):
            raise ObjectStoreError("invalid prefix: %r" % (prefix,))
        if after is not None and not isinstance(after, str):
            raise ObjectStoreError("invalid after: %r" % (after,))
        limit = self._check_limit(limit)
        with self._lock:
            objects = self._objects.get(tenant, {})
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

    # -- versioning ------------------------------------------------------
    def set_versioning(self, enabled, tenant=DEFAULT_TENANT):
        """Enable or disable object versioning for *tenant*.

        *enabled* must be a boolean; anything else raises
        ``ObjectStoreError("invalid versioning")``. The toggle only affects
        new writes: recorded history is never deleted implicitly, and while
        versioning is disabled ``put``/``delete``/uploads behave exactly as
        before. The setting survives reopening the data directory.
        """
        self._check_tenant(tenant)
        if not isinstance(enabled, bool):
            raise ObjectStoreError("invalid versioning")
        with self._lock:
            previous = tenant in self._versioning
            if enabled:
                self._versioning.add(tenant)
            else:
                self._versioning.discard(tenant)
            if previous == enabled:
                return None
            try:
                self._save_index()
            except OSError:
                if previous:
                    self._versioning.add(tenant)
                else:
                    self._versioning.discard(tenant)
                raise ObjectStoreError("version io error")
            return None

    def get_versioning(self, tenant=DEFAULT_TENANT):
        """Return whether object versioning is enabled for *tenant*."""
        self._check_tenant(tenant)
        with self._lock:
            return tenant in self._versioning

    def _require_versioning(self, tenant):
        if tenant not in self._versioning:
            raise ObjectStoreError("versioning disabled")

    def _find_version(self, tenant, key, version_id):
        """Return ``(info, record)`` for *version_id* of *key* in *tenant*.

        A malformed id raises ``invalid version``; an unknown key or an
        unknown (well-formed) id raises ``not found``.
        """
        if not _is_version_id(version_id):
            raise ObjectStoreError("invalid version")
        info = self._versions.get(tenant, {}).get(key)
        if info is None:
            raise ObjectStoreError("not found: %s" % (key,))
        for record in info["versions"]:
            if record["version_id"] == version_id:
                return info, record
        raise ObjectStoreError("not found: version %s" % (version_id,))

    def list_versions(self, key, after=None, limit=DEFAULT_LIMIT,
                      tenant=DEFAULT_TENANT):
        """Return ``{"items": [...], "next_after": <str|null>}`` for one page.

        Items are the key's version records newest first (reverse creation
        order), each a mapping with ``version_id``, ``sha256``, ``size``,
        ``created_at`` and ``current`` (true only for the version the key
        currently returns). *after* is the exclusive ``version_id`` cursor
        from a previous page and *limit* follows the usual rules. Requires
        versioning to be enabled for *tenant*; a key without recorded
        versions yields an empty page.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        if after is not None and not isinstance(after, str):
            raise ObjectStoreError("invalid after: %r" % (after,))
        limit = self._check_limit(limit)
        with self._lock:
            self._require_versioning(tenant)
            info = self._versions.get(tenant, {}).get(key)
            records = list(info["versions"]) if info is not None else []
            current = info["current"] if info is not None else None
            ordered = list(reversed(records))
            if after is not None:
                if not _is_version_id(after):
                    raise ObjectStoreError("invalid after: %r" % (after,))
                position = next(
                    (index for index, record in enumerate(ordered)
                     if record["version_id"] == after), None)
                if position is None:
                    raise ObjectStoreError("invalid after: %r" % (after,))
                ordered = ordered[position + 1:]
            page = ordered[:limit]
            items = [
                {
                    "version_id": record["version_id"],
                    "sha256": record["sha256"],
                    "size": record["size"],
                    "created_at": record["created_at"],
                    "current": record["version_id"] == current,
                }
                for record in page
            ]
            next_after = page[-1]["version_id"] if len(ordered) > len(page) else None
            return {"items": items, "next_after": next_after}

    def get_version(self, key, version_id, tenant=DEFAULT_TENANT):
        """Return ``(payload, metadata)`` for one recorded version.

        The metadata adds ``version_id``, ``created_at`` and ``current`` to
        the usual entry fields. The blob's digest and byte count are
        verified exactly like :meth:`get`, so corrupted storage raises the
        usual ``corrupted blob``/``blob io error`` errors. Requires
        versioning to be enabled for *tenant*.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        with self._lock:
            self._require_versioning(tenant)
            info, record = self._find_version(tenant, key, version_id)
            data = self.blob(record["sha256"])
            if len(data) != record["size"]:
                raise ObjectStoreError("corrupted blob")
            metadata = dict(record)
            metadata["current"] = record["version_id"] == info["current"]
            return data, metadata

    def delete_version(self, key, version_id, tenant=DEFAULT_TENANT):
        """Remove one recorded version of *key* in *tenant*.

        Deleting a historical (non-current) version leaves the key's current
        object untouched. Deleting the current version promotes the newest
        remaining version to current, and removing the last remaining
        version makes the key disappear entirely. Requires versioning to be
        enabled for *tenant*; the blob bytes stay on disk for garbage
        collection.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        with self._lock:
            self._require_versioning(tenant)
            info, record = self._find_version(tenant, key, version_id)
            keymap = self._versions[tenant]
            saved_records = list(info["versions"])
            saved_current = info["current"]
            objects = self._objects.get(tenant, {})
            saved_object = objects.get(key, _MISSING)
            info["versions"] = [item for item in info["versions"]
                                if item["version_id"] != version_id]
            was_current = saved_current == version_id
            if not info["versions"]:
                del keymap[key]
                if not keymap:
                    del self._versions[tenant]
                if was_current:
                    # The last version of a versioned key is gone, so the
                    # key disappears; a current object written while
                    # versioning was disabled is not a version and stays.
                    objects.pop(key, None)
            elif was_current:
                newest = info["versions"][-1]
                info["current"] = newest["version_id"]
                objects[key] = {"sha256": newest["sha256"],
                                "size": newest["size"],
                                "content_type": newest["content_type"]}
            try:
                self._save_index()
            except OSError:
                if tenant not in self._versions:
                    self._versions[tenant] = keymap
                keymap[key] = info
                info["versions"] = saved_records
                info["current"] = saved_current
                if saved_object is _MISSING:
                    objects.pop(key, None)
                else:
                    objects[key] = saved_object
                raise ObjectStoreError("version io error")

    # -- retention -------------------------------------------------------
    def set_retention(self, max_versions=None, max_age_seconds=None,
                      tenant=DEFAULT_TENANT):
        """Save the retention policy used by :meth:`purge_retention`.

        Both limits must be non-negative integers (booleans rejected) and at
        least one of them must be given; anything else raises
        ``ObjectStoreError("invalid retention")``. The policy is stored per
        tenant and survives reopening the data directory.
        """
        self._check_tenant(tenant)
        for value in (max_versions, max_age_seconds):
            if value is not None and (isinstance(value, bool)
                                      or not isinstance(value, int) or value < 0):
                raise ObjectStoreError("invalid retention")
        if max_versions is None and max_age_seconds is None:
            raise ObjectStoreError("invalid retention")
        policy = {}
        if max_versions is not None:
            policy["max_versions"] = max_versions
        if max_age_seconds is not None:
            policy["max_age_seconds"] = max_age_seconds
        with self._lock:
            previous = self._retention.get(tenant, _MISSING)
            self._retention[tenant] = policy
            try:
                self._save_index()
            except OSError:
                if previous is _MISSING:
                    del self._retention[tenant]
                else:
                    self._retention[tenant] = previous
                raise ObjectStoreError("version io error")

    def get_retention(self, tenant=DEFAULT_TENANT):
        """Return the tenant's saved retention policy.

        Always returns a ``{"max_versions": ..., "max_age_seconds": ...}``
        mapping, with ``None`` for limits the policy does not set.
        """
        self._check_tenant(tenant)
        with self._lock:
            policy = self._retention.get(tenant, {})
            return {"max_versions": policy.get("max_versions"),
                    "max_age_seconds": policy.get("max_age_seconds")}

    def purge_retention(self, dry_run=True, tenant=DEFAULT_TENANT):
        """Delete versions that violate the tenant's retention policy.

        The current version of every key is always kept. Of the remaining
        recorded versions, the oldest ones beyond ``max_versions`` per key
        and every version older than ``max_age_seconds`` (compared against
        the current UTC time) are removed. Returns
        ``{"versions": [...], "bytes": <int>, "dry_run": <bool>}`` where the
        versions are the removed ``{"key", "version_id"}`` pairs sorted by
        ``version_id`` and bytes is the sum of their declared sizes. A dry
        run (the default) reports the candidates without changing anything.
        Requires a saved policy (``retention not configured`` otherwise);
        works on recorded history whether or not versioning is currently
        enabled for the tenant.
        """
        self._check_tenant(tenant)
        if not isinstance(dry_run, bool):
            raise ObjectStoreError("invalid dry_run")
        with self._lock:
            policy = self._retention.get(tenant)
            if policy is None:
                raise ObjectStoreError("retention not configured")
            max_versions = policy.get("max_versions")
            max_age = policy.get("max_age_seconds")
            now = datetime.now(timezone.utc)
            keymap = self._versions.get(tenant, {})
            purged = []
            plan = {}
            for key in sorted(keymap):
                info = keymap[key]
                records = info["versions"]
                drop = set()
                if max_versions is not None and len(records) > max_versions:
                    for record in records[:len(records) - max_versions]:
                        drop.add(record["version_id"])
                if max_age is not None:
                    for record in records:
                        created = _parse_created_at(record["created_at"])
                        if created is not None and \
                                (now - created).total_seconds() > max_age:
                            drop.add(record["version_id"])
                drop.discard(info["current"])
                if not drop:
                    continue
                kept = [record for record in records
                        if record["version_id"] not in drop]
                for record in records:
                    if record["version_id"] in drop:
                        purged.append((record["version_id"], key,
                                       record["size"]))
                plan[key] = kept
            removed = sorted(
                ({"key": key, "version_id": version_id}
                 for version_id, key, _ in purged),
                key=lambda item: item["version_id"])
            total = sum(size for _, _, size in purged)
            if dry_run:
                return {"versions": removed, "bytes": total, "dry_run": True}
            snapshot = {
                key: {"current": info["current"],
                      "versions": list(info["versions"])}
                for key, info in keymap.items()
            }
            had_keymap = tenant in self._versions
            for key, kept in plan.items():
                if kept:
                    keymap[key]["versions"] = kept
                else:
                    del keymap[key]
            if not keymap and had_keymap:
                del self._versions[tenant]
            try:
                self._save_index()
            except OSError:
                if had_keymap:
                    self._versions[tenant] = snapshot
                else:
                    self._versions.pop(tenant, None)
                raise ObjectStoreError("version io error")
            return {"versions": removed, "bytes": total, "dry_run": False}

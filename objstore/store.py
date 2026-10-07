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

Object versioning is opt-in per tenant and defaults to off, which keeps the
historical behaviour exactly. While a tenant has versioning enabled, every
``put`` and every completed resumable upload appends an immutable version
(``version_id`` plus content metadata and a UTC ``created_at``) to the key's
version list and makes it the current version; reads keep returning only the
current version and ``delete`` removes only the current version, promoting
the newest remaining one. Versions live in the index's ``versions`` map, the
per-tenant enable flags in ``versioning`` and the retention policies in
``retention``; indexes written by older versions have none of these maps and
therefore no versions. Garbage collection treats blobs referenced by any
version, any current object and any incomplete upload session as live.

Resumable uploads live under ``<root>/uploads/`` (created lazily): each
session is a ``<session>.json`` declaration plus confirmed offset and a
``<session>.part`` file holding the received bytes. A session belongs to the
tenant fixed at creation; operations naming another tenant behave exactly as
if the session did not exist. Completing a session publishes the object in
the session's tenant and records the result in the index's ``uploads`` map in
one atomic index rewrite, so the completion flag can never disagree with the
published object.

Each tenant may carry a quota policy (``quota`` map in the index) limiting
the logical bytes and objects it may hold. Usage is counted by logical
reference, not physical dedup: every current object and every retained
version counts once (a current pointer duplicating its newest version is not
counted twice), keys sharing a digest count separately, and active upload
sessions reserve their declared size until they complete or abort. Writes
that would exceed a limit fail with ``ObjectStoreError("quota exceeded")``
before any data lands; tenants without a saved policy are unlimited and see
no behaviour change.

Each current object written by a new version of the store (a ``put`` or a
completed resumable upload) carries a comparable UTC ``last_modified``
timestamp in the index. Objects in indexes written by older versions have no
such field and are treated as timeless: they load without error and
lifecycle expiration always skips them. A tenant may also carry a lifecycle
policy (``lifecycle`` map in the index) with an optional ``prefix`` (a
string, empty when omitted) and a required non-negative integer
``max_age_seconds`` (booleans excluded). Running the policy previews or
deletes the current objects of that tenant whose keys start with the prefix
and whose last-modified time is older than the age, using the same current
version removal and promotion rules as ``delete``; lifecycle only drops
metadata references, leaving the blob bytes for ``collect_garbage``.

Each tenant may also carry a read access policy (``access`` map in the
index). The policy is either ``public`` -- the default, represented by the
absence of an entry, so indexes written by older versions stay fully
public -- or ``signed`` with a 16 to 256 character UTF-8 secret. While a
tenant is signed, reading an object or a version through ``get``, ``head``,
``get_version`` or ``head_version`` requires a matching ``expires`` (a
future integer Unix timestamp) and ``signature`` (the lowercase hex
HMAC-SHA256 of the tenant, HTTP method, key, optional version id and
expires, joined by newlines, keyed with the secret); :meth:`sign_request`
computes exactly that signature. The check runs before any key or version
lookup and fails with ``ObjectStoreError("access denied")``.

Each tenant may also carry a rate limit policy (``rate_limit`` map in the
index) with a required ``window_seconds`` (1 to 86400) and at least one of
``max_requests`` and ``max_bytes`` (non-negative integers, booleans
excluded). While a policy is saved, every object, blob, upload, version,
retention, quota, lifecycle and access-policy operation on that tenant is
admitted only after an atomic check against fixed UTC windows aligned to
the Unix epoch: one request per operation, plus the payload bytes of
``put`` and ``append_upload`` against ``max_bytes``. A rejected operation
raises :class:`RateLimitExceeded` (an ``ObjectStoreError`` reading "rate
limit exceeded") before any state changes and is not itself counted;
counters live in memory only, so reopening the data directory keeps the
policy but starts the current window from zero. Tenants without a saved
policy are unlimited and see no behaviour change.

Every object, upload-session, version and policy operation appends one
event to the persistent audit log ``<root>/audit.log`` before returning
(blob reads and garbage collection are recorded under the ``global``
tenant). Each event carries a per-tenant incrementing ``seq``, a UTC
``timestamp``, the ``tenant``, the ``operation``, the target
``key``/``session``/``version`` when there is one and a ``result`` of
``"ok"`` or ``"error"``; successful object events also carry ``sha256``
and ``size`` and failed events a stable ``error`` message. Payloads,
request bodies and secrets are never recorded. Events are hash-chained
per tenant (``prev_hash``/``hash``), so tampering, a sequence gap or a
mid-file format error is reported as ``ObjectStoreError("audit
corrupted")`` when the log is loaded; a trailing incomplete record left
by an interrupted write is discarded instead and the sequence continues
across restarts. A directory without a log opens as an empty log. A
failed audit write or flush raises ``ObjectStoreError("audit io
error")``: write operations roll their object, version, session and
quota changes back and read operations return no data. The audit
queries (:meth:`list_audit_events`, :meth:`verify_audit_log`) do not
record events of their own.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

__all__ = ["ContentAddressedStore", "ObjectStoreError", "RateLimitExceeded",
           "SessionConflict"]

INDEX_NAME = "index.json"
BLOBS_DIRNAME = "blobs"
UPLOADS_DIRNAME = "uploads"
AUDIT_NAME = "audit.log"
INDEX_VERSION = 1
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
DEFAULT_TENANT = "default"
AUDIT_GLOBAL_TENANT = "global"
_AUDIT_GENESIS = "0" * 64

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_VERSION_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")
_SESSION_META_RE = re.compile(r"\A([0-9a-f]{32})\.json\Z")
_TENANT_RE = re.compile(r"\A[a-z0-9][a-z0-9_-]{0,63}\Z")
_MISSING = object()


class ObjectStoreError(Exception):
    """Missing object, invalid key/digest/limit, or a corrupted index."""


class SessionConflict(ObjectStoreError):
    """An upload operation that violates the session's current state.

    A subclass of :class:`ObjectStoreError`, so existing callers keep
    catching it; front ends (HTTP) can nevertheless distinguish these
    state conflicts from bad input and report them separately.
    """


class RateLimitExceeded(ObjectStoreError):
    """A request rejected by the tenant's rate limit policy.

    A subclass of :class:`ObjectStoreError` (its message reads exactly
    "rate limit exceeded"), so existing callers keep catching it; front
    ends can distinguish it and emit a 429 with a ``Retry-After`` header
    taken from :attr:`retry_after` (seconds until the current window
    resets, always at least 1).
    """

    def __init__(self, retry_after=1):
        super().__init__("rate limit exceeded")
        self.retry_after = max(1, int(retry_after))


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
        self.audit_path = os.path.join(self.root, AUDIT_NAME)
        self._lock = threading.RLock()
        os.makedirs(self.blobs_dir, exist_ok=True)
        (self._objects, self._timestamps, self._upload_records,
         self._versioning, self._versions, self._retention, self._quota,
         self._lifecycle, self._access, self._rate_limit) = self._load_index()
        self._uploads = self._load_uploads()
        # In-memory fixed-window counters per tenant; never persisted.
        self._rate_windows = {}
        # Per-tenant audit chain heads: {tenant: {"next_seq", "head"}}.
        self._audit_state = self._load_audit()
    # -- index ---------------------------------------------------------
    def _load_index(self):
        if not os.path.exists(self.index_path):
            return {DEFAULT_TENANT: {}}, {}, {}, {}, {}, {}, {}, {}, {}, {}
        try:
            with open(self.index_path, "rb") as handle:
                document = json.loads(handle.read().decode("utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise ObjectStoreError("corrupted index: %s" % (exc,))
        if not isinstance(document, dict) or not isinstance(document.get("objects"), dict):
            raise ObjectStoreError("corrupted index: missing objects map")
        objects, timestamps = {DEFAULT_TENANT: {}}, {}
        default_objects, default_times = self._load_object_map(
            document["objects"])
        objects[DEFAULT_TENANT] = default_objects
        if default_times:
            timestamps[DEFAULT_TENANT] = default_times
        raw_tenants = document.get("tenants", {})
        if not isinstance(raw_tenants, dict):
            raise ObjectStoreError("corrupted index: tenants is not a map")
        for tenant, mapping in raw_tenants.items():
            if not isinstance(tenant, str) or tenant == DEFAULT_TENANT \
                    or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad tenant")
            if not isinstance(mapping, dict):
                raise ObjectStoreError("corrupted index: bad tenant objects")
            tenant_objects, tenant_times = self._load_object_map(mapping)
            objects[tenant] = tenant_objects
            if tenant_times:
                timestamps[tenant] = tenant_times
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
                "entry": self._validate_entry(key, record.get("entry"))[0],
                "tenant": tenant,
            }
            version_id = record.get("version_id")
            if version_id is not None:
                if not isinstance(version_id, str) \
                        or _VERSION_ID_RE.match(version_id) is None:
                    raise ObjectStoreError("corrupted index: bad upload record")
                records[session_id]["version_id"] = version_id
        versioning = {}
        raw_versioning = document.get("versioning", {})
        if not isinstance(raw_versioning, dict):
            raise ObjectStoreError("corrupted index: versioning is not a map")
        for tenant, flag in raw_versioning.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad versioning tenant")
            if not isinstance(flag, bool):
                raise ObjectStoreError("corrupted index: bad versioning flag")
            if flag:
                versioning[tenant] = True
        versions = {}
        raw_versions = document.get("versions", {})
        if not isinstance(raw_versions, dict):
            raise ObjectStoreError("corrupted index: versions is not a map")
        for tenant, keymap in raw_versions.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad versions tenant")
            if not isinstance(keymap, dict):
                raise ObjectStoreError("corrupted index: bad tenant versions")
            loaded = {}
            for key, entries in keymap.items():
                if not isinstance(key, str) or not isinstance(entries, list):
                    raise ObjectStoreError("corrupted index: bad version list")
                loaded[key] = [self._validate_version(key, entry)
                               for entry in entries]
            if loaded:
                versions[tenant] = loaded
        retention = {}
        raw_retention = document.get("retention", {})
        if not isinstance(raw_retention, dict):
            raise ObjectStoreError("corrupted index: retention is not a map")
        for tenant, policy in raw_retention.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad retention tenant")
            retention[tenant] = self._validate_policy(policy)
        quota = {}
        raw_quota = document.get("quota", {})
        if not isinstance(raw_quota, dict):
            raise ObjectStoreError("corrupted index: quota is not a map")
        for tenant, policy in raw_quota.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad quota tenant")
            quota[tenant] = self._validate_quota(policy)
        lifecycle = {}
        raw_lifecycle = document.get("lifecycle", {})
        if not isinstance(raw_lifecycle, dict):
            raise ObjectStoreError("corrupted index: lifecycle is not a map")
        for tenant, policy in raw_lifecycle.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad lifecycle tenant")
            lifecycle[tenant] = self._validate_lifecycle_policy(policy)
        access = {}
        raw_access = document.get("access", {})
        if not isinstance(raw_access, dict):
            raise ObjectStoreError("corrupted index: access is not a map")
        for tenant, policy in raw_access.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad access tenant")
            access[tenant] = self._validate_access_policy(policy)
        rate_limit = {}
        raw_rate_limit = document.get("rate_limit", {})
        if not isinstance(raw_rate_limit, dict):
            raise ObjectStoreError("corrupted index: rate_limit is not a map")
        for tenant, policy in raw_rate_limit.items():
            if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
                raise ObjectStoreError("corrupted index: bad rate limit tenant")
            rate_limit[tenant] = self._validate_rate_limit_policy(policy)
        return (objects, timestamps, records, versioning, versions,
                retention, quota, lifecycle, access, rate_limit)

    def _load_object_map(self, mapping):
        objects, timestamps = {}, {}
        for key, entry in mapping.items():
            if not isinstance(key, str):
                raise ObjectStoreError("corrupted index: non-string key")
            objects[key], last_modified = self._validate_entry(key, entry)
            if last_modified is not None:
                timestamps[key] = last_modified
        return objects, timestamps

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
        last_modified = entry.get("last_modified")
        if last_modified is not None:
            if not isinstance(last_modified, str):
                raise ObjectStoreError(
                    "corrupted index: bad last_modified for %r" % (key,))
            try:
                datetime.fromisoformat(last_modified)
            except ValueError:
                raise ObjectStoreError(
                    "corrupted index: bad last_modified for %r" % (key,))
        return ({"sha256": sha, "size": size, "content_type": content_type},
                last_modified)

    def _validate_version(self, key, entry):
        if not isinstance(entry, dict):
            raise ObjectStoreError("corrupted index: version for %r is not an object" % (key,))
        version_id = entry.get("version_id")
        if not isinstance(version_id, str) or _VERSION_ID_RE.match(version_id) is None:
            raise ObjectStoreError("corrupted index: bad version_id for %r" % (key,))
        created_at = entry.get("created_at")
        if not isinstance(created_at, str):
            raise ObjectStoreError("corrupted index: bad created_at for %r" % (key,))
        base = self._validate_entry(key, entry)[0]
        return {"version_id": version_id, "sha256": base["sha256"],
                "size": base["size"], "content_type": base["content_type"],
                "created_at": created_at}

    @staticmethod
    def _validate_policy(policy):
        if not isinstance(policy, dict):
            raise ObjectStoreError("corrupted index: bad retention policy")
        loaded = {}
        for name in ("max_versions", "max_age_seconds"):
            value = policy.get(name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ObjectStoreError("corrupted index: bad retention policy")
            loaded[name] = value
        if not loaded:
            raise ObjectStoreError("corrupted index: bad retention policy")
        return loaded

    @staticmethod
    def _validate_quota(policy):
        if not isinstance(policy, dict):
            raise ObjectStoreError("corrupted index: bad quota policy")
        loaded = {}
        for name in ("max_bytes", "max_objects"):
            value = policy.get(name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ObjectStoreError("corrupted index: bad quota policy")
            loaded[name] = value
        if not loaded:
            raise ObjectStoreError("corrupted index: bad quota policy")
        return loaded

    @staticmethod
    def _validate_lifecycle_policy(policy):
        if not isinstance(policy, dict):
            raise ObjectStoreError("corrupted index: bad lifecycle policy")
        prefix = policy.get("prefix", "")
        if not isinstance(prefix, str):
            raise ObjectStoreError("corrupted index: bad lifecycle policy")
        max_age = policy.get("max_age_seconds")
        if isinstance(max_age, bool) or not isinstance(max_age, int) or max_age < 0:
            raise ObjectStoreError("corrupted index: bad lifecycle policy")
        return {"prefix": prefix, "max_age_seconds": max_age}

    @staticmethod
    def _check_secret(secret, message):
        """Validate a signing secret: 16 to 256 UTF-8 characters."""
        if not isinstance(secret, str) or not 16 <= len(secret) <= 256:
            raise ObjectStoreError(message)
        try:
            secret.encode("utf-8")
        except UnicodeEncodeError:
            raise ObjectStoreError(message)
        return secret

    @classmethod
    def _validate_access_policy(cls, policy):
        if not isinstance(policy, dict) or policy.get("mode") != "signed":
            raise ObjectStoreError("corrupted index: bad access policy")
        secret = cls._check_secret(policy.get("secret"),
                                   "corrupted index: bad access policy")
        return {"mode": "signed", "secret": secret}

    @staticmethod
    def _validate_rate_limit_policy(policy):
        if not isinstance(policy, dict):
            raise ObjectStoreError("corrupted index: bad rate limit policy")
        window = policy.get("window_seconds")
        if isinstance(window, bool) or not isinstance(window, int) \
                or not 1 <= window <= 86400:
            raise ObjectStoreError("corrupted index: bad rate limit policy")
        loaded = {"window_seconds": window}
        for name in ("max_requests", "max_bytes"):
            value = policy.get(name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ObjectStoreError("corrupted index: bad rate limit policy")
            loaded[name] = value
        if len(loaded) == 1:
            raise ObjectStoreError("corrupted index: bad rate limit policy")
        return loaded

    @staticmethod
    def _stored_entry(entry, last_modified):
        """Return the on-disk form of *entry*, adding *last_modified* when set."""
        saved = dict(entry)
        if last_modified is not None:
            saved["last_modified"] = last_modified
        return saved

    def _persist_object_map(self, tenant, mapping):
        """Serialize one tenant's object map with its saved timestamps."""
        timestamps = self._timestamps.get(tenant, {})
        return {key: self._stored_entry(entry, timestamps.get(key))
                for key, entry in mapping.items()}

    def _save_index(self):
        document = {"version": INDEX_VERSION,
                    "objects": self._persist_object_map(
                        DEFAULT_TENANT, self._objects.get(DEFAULT_TENANT, {}))}
        tenants = {tenant: self._persist_object_map(tenant, mapping)
                   for tenant, mapping in self._objects.items()
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
                tenant: True for tenant in sorted(self._versioning)
                if self._versioning[tenant]
            }
        versions = {tenant: keymap for tenant, keymap in self._versions.items()
                    if keymap}
        if versions:
            document["versions"] = versions
        if self._retention:
            document["retention"] = self._retention
        if self._quota:
            document["quota"] = self._quota
        if self._lifecycle:
            document["lifecycle"] = self._lifecycle
        if self._access:
            document["access"] = self._access
        if self._rate_limit:
            document["rate_limit"] = self._rate_limit
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

        When *tenant* has versioning enabled the write also appends an
        immutable version to the key's version list, makes it the current
        version and includes its ``version_id`` in the returned metadata.

        When *tenant* has a quota policy, the write is rejected with
        ``ObjectStoreError("quota exceeded")`` before any data lands when
        the resulting usage would exceed a saved limit.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "put", key=key):
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
                self._rate_limit_check(tenant, len(data))
                versioning_on = self._versioning.get(tenant, False)
                delta_bytes, delta_objects = self._publish_delta(
                    tenant, key, entry, versioning_on)
                self._enforce_quota(tenant, delta_bytes, delta_objects)
                self._store_blob(sha, data)
                objects = self._objects.setdefault(tenant, {})
                timestamps = self._timestamps.setdefault(tenant, {})
                previous = objects.get(key, _MISSING)
                previous_time = timestamps.get(key, _MISSING)
                objects[key] = entry
                timestamps[key] = datetime.now(timezone.utc).isoformat()
                version_id = None
                if versioning_on:
                    version_id = self._append_version(tenant, key, entry)
                self._save_index()
                try:
                    self._audit_append(tenant, "put", "ok", key=key,
                                       sha256=sha, size=len(data))
                except ObjectStoreError:
                    # The audit record is part of the write: without it the
                    # object, version and timestamp changes are rolled back.
                    if version_id is not None:
                        versions = self._versions[tenant][key]
                        versions.pop()
                        if not versions:
                            self._drop_versions_key(tenant, key)
                    if previous is _MISSING:
                        objects.pop(key, None)
                    else:
                        objects[key] = previous
                    if previous_time is _MISSING:
                        timestamps.pop(key, None)
                    else:
                        timestamps[key] = previous_time
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise
                result = dict(entry)
                if version_id is not None:
                    result["version_id"] = version_id
                return result

    def _append_version(self, tenant, key, entry):
        """Append a new immutable version for *key* and return its id.

        Must be called under the store lock; the caller makes the matching
        current-pointer update and saves the index.
        """
        versions = self._versions.setdefault(tenant, {}).setdefault(key, [])
        existing = {version["version_id"] for version in versions}
        while True:
            version_id = uuid.uuid4().hex
            if version_id not in existing:
                break
        versions.append({
            "version_id": version_id,
            "sha256": entry["sha256"],
            "size": entry["size"],
            "content_type": entry["content_type"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        return version_id

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

    def get(self, key, tenant=DEFAULT_TENANT, expires=None, signature=None):
        """Return ``(payload, entry)`` for *key* in *tenant*.

        When *tenant* has a signed access policy, *expires* and *signature*
        must hold an unexpired signature for the ``GET`` method (see
        :meth:`sign_request`); anything else raises
        ``ObjectStoreError("access denied")`` before the key is looked up.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get", key=key):
            self._check_key(key)
            with self._lock:
                self._check_read_access(tenant, key, None, expires, signature,
                                        "GET")
                self._rate_limit_check(tenant)
                entry = self._head_unlocked(tenant, key)
                data = self._blob_unlocked(entry["sha256"])
                if len(data) != entry["size"]:
                    raise ObjectStoreError("corrupted blob")
                self._audit_append(tenant, "get", "ok", key=key,
                                   sha256=entry["sha256"], size=entry["size"])
                return data, entry

    def head(self, key, tenant=DEFAULT_TENANT, expires=None, signature=None):
        """Return a copy of the metadata entry for *key* in *tenant*.

        Signed tenants require the same ``expires``/``signature`` pair as
        :meth:`get`, but computed for the ``HEAD`` method.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "head", key=key):
            self._check_key(key)
            with self._lock:
                self._check_read_access(tenant, key, None, expires, signature,
                                        "HEAD")
                self._rate_limit_check(tenant)
                entry = self._head_unlocked(tenant, key)
                self._audit_append(tenant, "head", "ok", key=key,
                                   sha256=entry["sha256"], size=entry["size"])
                return entry

    def _peek_head(self, tenant, key):
        """Return *key*'s entry the way :meth:`head` does, uncounted.

        Used internally (e.g. by the HTTP front end's pre-write check) so
        one logical request is accounted exactly once. Raises ``access
        denied`` for signed tenants exactly like a signature-less
        :meth:`head`.
        """
        with self._lock:
            self._check_read_access(tenant, key, None, None, None, "HEAD")
            return self._head_unlocked(tenant, key)

    def _head_unlocked(self, tenant, key):
        """Return *key*'s entry; must be called under the store lock."""
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

        When *tenant* has versioning enabled and the key has versions, only
        the current version is removed: the newest remaining version becomes
        current, and the key disappears entirely once no version remains.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "delete", key=key):
            self._check_key(key)
            expected = self._check_precondition(expected_sha256)
            with self._lock:
                objects = self._objects.get(tenant, {})
                if key not in objects:
                    raise ObjectStoreError("not found: %s" % (key,))
                if expected is not _MISSING and not self._precondition_met(
                        tenant, key, expected):
                    raise ObjectStoreError("precondition failed")
                self._rate_limit_check(tenant)
                previous = dict(objects[key])
                previous_time = self._timestamps.get(tenant, {}).get(
                    key, _MISSING)
                keymap = self._versions.get(tenant, {})
                had_version_list = key in keymap
                previous_versions = list(keymap.get(key) or ())
                removed = self._remove_current_unlocked(tenant, key)
                self._save_index()
                try:
                    self._audit_append(tenant, "delete", "ok", key=key,
                                       sha256=removed["sha256"],
                                       size=removed["size"])
                except ObjectStoreError:
                    self._objects.setdefault(tenant, {})[key] = previous
                    if previous_time is _MISSING:
                        self._timestamps.get(tenant, {}).pop(key, None)
                    else:
                        self._timestamps.setdefault(
                            tenant, {})[key] = previous_time
                    if had_version_list:
                        self._versions.setdefault(
                            tenant, {})[key] = previous_versions
                    else:
                        self._versions.get(tenant, {}).pop(key, None)
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def _remove_current_unlocked(self, tenant, key):
        """Remove *key*'s current object the way :meth:`delete` does.

        Versioned tenants pop the current version and promote the newest
        remaining one; unversioned tenants drop the mapping outright. When a
        version is promoted its ``created_at`` becomes the key's
        last-modified time (that content's last write); when the mapping is
        removed the saved timestamp is dropped with it. Must be called under
        the store lock with *key* present; returns the removed current entry.
        """
        objects = self._objects[tenant]
        removed = dict(objects[key])
        versions = self._versions.get(tenant, {}).get(key)
        if self._versioning.get(tenant, False) and versions:
            versions.pop()
            if versions:
                promoted = versions[-1]
                objects[key] = {"sha256": promoted["sha256"],
                                "size": promoted["size"],
                                "content_type": promoted["content_type"]}
                self._timestamps.setdefault(tenant, {})[key] = \
                    promoted["created_at"]
            else:
                del objects[key]
                self._timestamps.get(tenant, {}).pop(key, None)
                self._drop_versions_key(tenant, key)
        else:
            del objects[key]
            self._timestamps.get(tenant, {}).pop(key, None)
        return removed

    def _drop_versions_key(self, tenant, key):
        """Remove *key*'s (empty) version list and prune empty parent maps."""
        keymap = self._versions.get(tenant)
        if keymap is None:
            return
        keymap.pop(key, None)
        if not keymap:
            del self._versions[tenant]

    def blob(self, sha256, tenant=DEFAULT_TENANT):
        """Return the raw bytes stored under digest *sha256*.

        The digest of the bytes actually on disk is recomputed on every read,
        so truncation, appended bytes and same-length tampering are all
        rejected instead of being handed back to the caller.

        Blobs stay global; *tenant* only selects the rate limit policy the
        read is accounted against (the default tenant when omitted). The
        audit event is recorded under the ``global`` tenant either way.
        """
        self._check_tenant(tenant)
        with self._audit_failures(AUDIT_GLOBAL_TENANT, "blob"):
            self._require_sha(sha256)
            with self._lock:
                self._rate_limit_check(tenant)
                data = self._blob_unlocked(sha256)
                self._audit_append(AUDIT_GLOBAL_TENANT, "blob", "ok",
                                   sha256=sha256, size=len(data))
                return data

    def _blob_unlocked(self, sha256):
        """Return the verified bytes of *sha256*; must be called under the lock."""
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
        with self._audit_failures(AUDIT_GLOBAL_TENANT, "blob_digests"):
            with self._lock:
                digests = sorted(name for name in os.listdir(self.blobs_dir)
                                 if _is_sha256(name))
                self._audit_append(AUDIT_GLOBAL_TENANT, "blob_digests", "ok")
                return digests

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

        The session reserves its declared *size* against the tenant's
        ``max_bytes`` quota for its whole lifetime: when current usage plus
        the reservations of all active sessions (this one included) would
        exceed the limit, no session is created and
        ``ObjectStoreError("quota exceeded")`` is raised.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "begin_upload", key=key):
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
                self._rate_limit_check(tenant)
                self._enforce_reservation(tenant, size)
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
                try:
                    self._audit_append(tenant, "begin_upload", "ok",
                                       key=key, session=session_id)
                except ObjectStoreError:
                    del self._uploads[session_id]
                    self._discard_session_files(session_id)
                    raise
                return session_id

    def upload_status(self, session_id, tenant=DEFAULT_TENANT):
        """Return the declaration, received ``offset`` and ``completed`` flag.

        Only the session's owning tenant sees it; every other tenant gets
        the same ``unknown upload session`` error as for a session that
        never existed.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "upload_status", session=session_id):
            self._check_session_id(session_id)
            with self._lock:
                self._rate_limit_check(tenant)
                record = self._upload_records.get(session_id)
                if record is not None:
                    if record["tenant"] != tenant:
                        raise ObjectStoreError(
                            "unknown upload session: %r" % (session_id,))
                    entry = record["entry"]
                    result = {"key": record["key"], "size": entry["size"],
                              "sha256": entry["sha256"],
                              "content_type": entry["content_type"],
                              "offset": entry["size"], "completed": True}
                else:
                    session = self._uploads.get(session_id)
                    if session is None or session["tenant"] != tenant:
                        raise ObjectStoreError(
                            "unknown upload session: %r" % (session_id,))
                    result = {"key": session["key"], "size": session["size"],
                              "sha256": session["sha256"],
                              "content_type": session["content_type"],
                              "offset": session["offset"], "completed": False}
                self._audit_append(tenant, "upload_status", "ok",
                                   session=session_id, key=result["key"])
                return result

    def append_upload(self, session_id, offset, payload, tenant=DEFAULT_TENANT):
        """Append one chunk to *session_id* and return the received length.

        A new chunk must start exactly at the current end and may not exceed
        the declared total size. A chunk lying fully inside the received
        range succeeds without growing when its bytes are identical to what
        is stored, and fails otherwise; chunks overlapping the end, chunks
        past the end and empty chunks anywhere but at the end all fail.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "append_upload", session=session_id):
            self._check_session_id(session_id)
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise ObjectStoreError("invalid offset: %r" % (offset,))
            if not isinstance(payload, (bytes, bytearray, memoryview)):
                raise ObjectStoreError("payload must be bytes")
            data = bytes(payload)
            with self._lock:
                session = self._require_active_session(session_id, tenant)
                self._rate_limit_check(tenant, len(data))
                current = session["offset"]
                if offset > current:
                    raise SessionConflict(
                        "append gap: offset %d beyond received %d" % (offset, current))
                if not data:
                    if offset != current:
                        raise SessionConflict("empty chunk only at received end")
                    self._audit_append(tenant, "append_upload", "ok",
                                       session=session_id, key=session["key"])
                    return current
                end = offset + len(data)
                if offset < current:
                    if end > current:
                        raise SessionConflict("append overlaps received end")
                    if self._read_part(session_id, offset, len(data)) != data:
                        raise SessionConflict("append conflicts with received bytes")
                    self._audit_append(tenant, "append_upload", "ok",
                                       session=session_id, key=session["key"])
                    return current
                if end > session["size"]:
                    raise SessionConflict("append exceeds declared size")
                self._write_part(session_id, offset, data)
                self._save_session(session_id, dict(session, offset=end))
                session["offset"] = end
                try:
                    self._audit_append(tenant, "append_upload", "ok",
                                       session=session_id, key=session["key"])
                except ObjectStoreError:
                    # Roll the session back to the last audited offset.
                    session["offset"] = current
                    try:
                        self._save_session(session_id, session)
                        with open(self._session_part_path(session_id),
                                  "r+b") as handle:
                            handle.truncate(current)
                            handle.flush()
                            os.fsync(handle.fileno())
                    except (OSError, ObjectStoreError):
                        pass
                    raise
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
        A quota policy on the tenant is enforced the same way: the session's
        own reservation is released first, and when the publish would exceed
        a saved limit ``ObjectStoreError("quota exceeded")`` is raised
        without changing anything. Repeating a completed call returns the
        first result without touching the key again, so later writes or
        deletes of the key are never overwritten.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "complete_upload", session=session_id):
            self._check_session_id(session_id)
            with self._lock:
                record = self._upload_records.get(session_id)
                if record is not None:
                    if record["tenant"] != tenant:
                        raise ObjectStoreError(
                            "unknown upload session: %r" % (session_id,))
                    entry = dict(record["entry"])
                    if record.get("version_id") is not None:
                        entry["version_id"] = record["version_id"]
                    self._audit_append(tenant, "complete_upload", "ok",
                                       session=session_id, key=record["key"],
                                       sha256=entry["sha256"],
                                       size=entry["size"])
                    return entry
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
                self._rate_limit_check(tenant)
                entry = {"sha256": session["sha256"], "size": session["size"],
                         "content_type": session["content_type"]}
                # The session's own reservation is lifted before the quota check:
                # its reserved bytes become the used bytes being published.
                versioning_on = self._versioning.get(tenant, False)
                delta_bytes, delta_objects = self._publish_delta(
                    tenant, session["key"], entry, versioning_on)
                self._enforce_quota(tenant, delta_bytes, delta_objects,
                                    release_session=session)
                self._store_blob(session["sha256"], data)
                objects = self._objects.setdefault(tenant, {})
                timestamps = self._timestamps.setdefault(tenant, {})
                previous = objects.get(session["key"], _MISSING)
                previous_time = timestamps.get(session["key"], _MISSING)
                published_at = datetime.now(timezone.utc).isoformat()
                objects[session["key"]] = entry
                timestamps[session["key"]] = published_at
                version_id = None
                if versioning_on:
                    version_id = self._append_version(tenant, session["key"], entry)
                record = {"key": session["key"], "entry": dict(entry),
                          "tenant": tenant}
                if version_id is not None:
                    record["version_id"] = version_id
                self._upload_records[session_id] = record

                def rollback():
                    del self._upload_records[session_id]
                    if version_id is not None:
                        self._versions[tenant][session["key"]].pop()
                        self._drop_versions_key(tenant, session["key"])
                    if previous is _MISSING:
                        del objects[session["key"]]
                    else:
                        objects[session["key"]] = previous
                    if previous_time is _MISSING:
                        timestamps.pop(session["key"], None)
                    else:
                        timestamps[session["key"]] = previous_time

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("upload io error")
                try:
                    self._audit_append(tenant, "complete_upload", "ok",
                                       session=session_id, key=session["key"],
                                       sha256=entry["sha256"],
                                       size=entry["size"])
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise
                del self._uploads[session_id]
                self._discard_session_files(session_id)
                result = dict(entry)
                if version_id is not None:
                    result["version_id"] = version_id
                return result

    def abort_upload(self, session_id, tenant=DEFAULT_TENANT):
        """Drop *session_id* and return None; published objects are kept."""
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "abort_upload", session=session_id):
            self._check_session_id(session_id)
            with self._lock:
                self._rate_limit_check(tenant)
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
                    try:
                        self._audit_append(tenant, "abort_upload", "ok",
                                           session=session_id,
                                           key=record["key"])
                    except ObjectStoreError:
                        self._upload_records[session_id] = record
                        try:
                            self._save_index()
                        except OSError:
                            pass
                        raise
                    self._discard_session_files(session_id)
                    return None
                session = self._uploads.get(session_id)
                if session is None or session["tenant"] != tenant:
                    raise ObjectStoreError("unknown upload session: %r" % (session_id,))
                self._remove_session_files(session_id)
                del self._uploads[session_id]
                try:
                    self._audit_append(tenant, "abort_upload", "ok",
                                       session=session_id, key=session["key"])
                except ObjectStoreError:
                    # The session files are already gone; only the in-memory
                    # entry can be restored.
                    self._uploads[session_id] = session
                    raise
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
        Blobs referenced by any retained object version or declared by an
        incomplete upload session are live as well. The whole operation runs
        under the store lock, so blobs published
        concurrently stay referenced and candidates that vanished between
        the scan and deletion are skipped rather than counted. The audit
        event is recorded under the ``global`` tenant.
        """
        with self._audit_failures(AUDIT_GLOBAL_TENANT, "collect_garbage"):
            if not isinstance(dry_run, bool):
                raise ObjectStoreError("invalid dry_run")
            with self._lock:
                referenced = {
                    entry["sha256"]
                    for objects in self._objects.values()
                    for entry in objects.values()
                }
                for keymap in self._versions.values():
                    for versions in keymap.values():
                        for version in versions:
                            referenced.add(version["sha256"])
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
                    result = {
                        "digests": [name for name, _ in candidates],
                        "bytes": sum(size for _, size in candidates),
                        "dry_run": True,
                    }
                    self._audit_append(AUDIT_GLOBAL_TENANT,
                                       "collect_garbage", "ok")
                    return result
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
                result = {"digests": deleted, "bytes": freed, "dry_run": False}
                self._audit_append(AUDIT_GLOBAL_TENANT, "collect_garbage", "ok")
                return result

    def list_objects(self, prefix="", after=None, limit=DEFAULT_LIMIT,
                     tenant=DEFAULT_TENANT):
        """Return ``{"items": [...], "next_after": <str|null>}`` for one page.

        Only *tenant*'s objects are filtered, paginated and returned, with
        the plain object keys as names; a tenant without objects yields an
        empty page.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "list_objects"):
            if not isinstance(prefix, str):
                raise ObjectStoreError("invalid prefix: %r" % (prefix,))
            if after is not None and not isinstance(after, str):
                raise ObjectStoreError("invalid after: %r" % (after,))
            limit = self._check_limit(limit)
            with self._lock:
                self._rate_limit_check(tenant)
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
                result = {"items": items,
                          "next_after": page[-1] if len(keys) > len(page) else None}
                self._audit_append(tenant, "list_objects", "ok")
                return result

    # -- versioning ------------------------------------------------------
    def set_versioning(self, enabled, tenant=DEFAULT_TENANT):
        """Enable or disable object versioning for *tenant*.

        *enabled* must be a boolean; anything else raises
        ``ObjectStoreError("invalid versioning")``. The flag only affects
        new writes: existing objects and retained versions are never
        touched, so disabling and re-enabling keeps the history intact.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "set_versioning"):
            if not isinstance(enabled, bool):
                raise ObjectStoreError("invalid versioning")
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._versioning.get(tenant)
                if enabled:
                    self._versioning[tenant] = True
                else:
                    self._versioning.pop(tenant, None)
                self._save_index()
                try:
                    self._audit_append(tenant, "set_versioning", "ok")
                except ObjectStoreError:
                    if previous:
                        self._versioning[tenant] = True
                    else:
                        self._versioning.pop(tenant, None)
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_versioning(self, tenant=DEFAULT_TENANT):
        """Return whether *tenant* currently has versioning enabled."""
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_versioning"):
            with self._lock:
                self._rate_limit_check(tenant)
                enabled = bool(self._versioning.get(tenant, False))
                self._audit_append(tenant, "get_versioning", "ok")
                return enabled

    def _require_versioning(self, tenant):
        if not self._versioning.get(tenant, False):
            raise ObjectStoreError("versioning disabled")

    @staticmethod
    def _check_version_id(version_id):
        if not isinstance(version_id, str) \
                or _VERSION_ID_RE.match(version_id) is None:
            raise ObjectStoreError("invalid version")
        return version_id

    def _versions_of(self, tenant, key):
        """Return the version list of *key*, raising the usual errors."""
        versions = self._versions.get(tenant, {}).get(key)
        if not versions:
            raise ObjectStoreError("not found: %s" % (key,))
        return versions

    @staticmethod
    def _version_item(version, current_id):
        return {"version_id": version["version_id"],
                "sha256": version["sha256"],
                "size": version["size"],
                "created_at": version["created_at"],
                "current": version["version_id"] == current_id}

    def list_versions(self, key, after=None, limit=DEFAULT_LIMIT,
                      tenant=DEFAULT_TENANT):
        """Return ``{"items": [...], "next_after": <str|null>}`` for one page.

        Versions of *key* in *tenant* are listed newest first (creation
        order reversed); each item holds ``version_id``, ``sha256``,
        ``size``, ``created_at`` and a ``current`` flag. *after* is the
        exclusive ``version_id`` cursor handed out as ``next_after`` and
        *limit* follows the same rules as :meth:`list_objects`. Requires
        versioning to be enabled for the tenant.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "list_versions", key=key):
            self._check_key(key)
            if after is not None and not isinstance(after, str):
                raise ObjectStoreError("invalid version")
            limit = self._check_limit(limit)
            with self._lock:
                self._rate_limit_check(tenant)
                self._require_versioning(tenant)
                versions = self._versions_of(tenant, key)
                current_id = versions[-1]["version_id"]
                ordered = list(reversed(versions))
                if after is not None:
                    position = next(
                        (index for index, version in enumerate(ordered)
                         if version["version_id"] == after), None)
                    if position is None:
                        raise ObjectStoreError("invalid version")
                    ordered = ordered[position + 1:]
                page = ordered[:limit]
                items = [self._version_item(version, current_id) for version in page]
                result = {"items": items,
                          "next_after": page[-1]["version_id"]
                          if len(ordered) > len(page) else None}
                self._audit_append(tenant, "list_versions", "ok", key=key)
                return result

    def head_version(self, key, version_id, tenant=DEFAULT_TENANT,
                     expires=None, signature=None):
        """Return the metadata of one version of *key* in *tenant*.

        The result holds ``version_id``, ``sha256``, ``size``,
        ``content_type``, ``created_at`` and a ``current`` flag. A
        malformed *version_id* raises ``invalid version`` and an unknown
        key or version raises ``not found``; the tenant must have
        versioning enabled. Signed tenants require an unexpired
        ``expires``/``signature`` pair for the ``HEAD`` method, checked
        before any key or version lookup.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "head_version", key=key,
                                  version=version_id):
            self._check_key(key)
            with self._lock:
                self._check_read_access(tenant, key, version_id, expires,
                                        signature, "HEAD")
                self._rate_limit_check(tenant)
                self._require_versioning(tenant)
                self._check_version_id(version_id)
                entry = self._head_version_unlocked(tenant, key, version_id)
                self._audit_append(tenant, "head_version", "ok", key=key,
                                   version=version_id,
                                   sha256=entry["sha256"], size=entry["size"])
                return entry

    def _head_version_unlocked(self, tenant, key, version_id):
        """Return one version's metadata; must be called under the lock."""
        versions = self._versions_of(tenant, key)
        for version in versions:
            if version["version_id"] == version_id:
                entry = self._version_item(version, versions[-1]["version_id"])
                entry["content_type"] = version["content_type"]
                return entry
        raise ObjectStoreError("not found: version %s" % (version_id,))

    def get_version(self, key, version_id, tenant=DEFAULT_TENANT,
                    expires=None, signature=None):
        """Return ``(payload, entry)`` for one version of *key*.

        The blob bytes are verified against the version's digest and size
        exactly like :meth:`get` verifies the current object. Signed
        tenants require the signature to be computed for the ``GET``
        method.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_version", key=key,
                                  version=version_id):
            self._check_key(key)
            with self._lock:
                self._check_read_access(tenant, key, version_id, expires,
                                        signature, "GET")
                self._rate_limit_check(tenant)
                self._require_versioning(tenant)
                self._check_version_id(version_id)
                entry = self._head_version_unlocked(tenant, key, version_id)
                data = self._blob_unlocked(entry["sha256"])
                if len(data) != entry["size"]:
                    raise ObjectStoreError("corrupted blob")
                self._audit_append(tenant, "get_version", "ok", key=key,
                                   version=version_id,
                                   sha256=entry["sha256"], size=entry["size"])
                return data, entry

    def delete_version(self, key, version_id, tenant=DEFAULT_TENANT):
        """Remove one version of *key* in *tenant* and return None.

        Deleting a historical version never touches the current object;
        deleting the current version promotes the newest remaining one,
        and the key disappears once no version remains. The blob bytes
        stay on disk for garbage collection.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "delete_version", key=key,
                                  version=version_id):
            self._check_key(key)
            with self._lock:
                self._rate_limit_check(tenant)
                self._require_versioning(tenant)
                self._check_version_id(version_id)
                versions = self._versions_of(tenant, key)
                index = next((i for i, version in enumerate(versions)
                              if version["version_id"] == version_id), None)
                if index is None:
                    raise ObjectStoreError("not found: version %s" % (version_id,))
                objects = self._objects.get(tenant, {})
                previous = objects.get(key, _MISSING)
                previous_time = self._timestamps.get(tenant, {}).get(
                    key, _MISSING)
                saved_versions = list(versions)
                was_current = index == len(versions) - 1
                del versions[index]
                if versions:
                    if was_current and key in objects:
                        promoted = versions[-1]
                        objects[key] = {"sha256": promoted["sha256"],
                                        "size": promoted["size"],
                                        "content_type": promoted["content_type"]}
                        self._timestamps.setdefault(tenant, {})[key] = \
                            promoted["created_at"]
                else:
                    objects.pop(key, None)
                    self._timestamps.get(tenant, {}).pop(key, None)
                    self._drop_versions_key(tenant, key)
                self._save_index()
                try:
                    self._audit_append(tenant, "delete_version", "ok",
                                       key=key, version=version_id)
                except ObjectStoreError:
                    self._versions.setdefault(tenant, {})[key] = saved_versions
                    if previous is _MISSING:
                        self._objects.get(tenant, {}).pop(key, None)
                    else:
                        self._objects.setdefault(tenant, {})[key] = previous
                    if previous_time is _MISSING:
                        self._timestamps.get(tenant, {}).pop(key, None)
                    else:
                        self._timestamps.setdefault(
                            tenant, {})[key] = previous_time
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    # -- retention -------------------------------------------------------
    def set_retention(self, max_versions=None, max_age_seconds=None,
                      tenant=DEFAULT_TENANT):
        """Save the retention policy of *tenant*.

        At least one of *max_versions* and *max_age_seconds* must be
        given, and each given value must be a non-negative integer
        (booleans excluded); anything else raises
        ``ObjectStoreError("invalid retention")``. The policy only takes
        effect through :meth:`purge_retention`.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "set_retention"):
            for value in (max_versions, max_age_seconds):
                if value is not None and (isinstance(value, bool)
                                          or not isinstance(value, int)
                                          or value < 0):
                    raise ObjectStoreError("invalid retention")
            if max_versions is None and max_age_seconds is None:
                raise ObjectStoreError("invalid retention")
            policy = {}
            if max_versions is not None:
                policy["max_versions"] = max_versions
            if max_age_seconds is not None:
                policy["max_age_seconds"] = max_age_seconds
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._retention.get(tenant, _MISSING)
                self._retention[tenant] = policy
                self._save_index()
                try:
                    self._audit_append(tenant, "set_retention", "ok")
                except ObjectStoreError:
                    if previous is _MISSING:
                        del self._retention[tenant]
                    else:
                        self._retention[tenant] = previous
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_retention(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s retention policy, or None when none is saved.

        A saved policy is returned as a dict with both ``max_versions``
        and ``max_age_seconds`` keys, unset limits reading as None.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_retention"):
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._retention.get(tenant)
                if policy is None:
                    result = None
                else:
                    result = {"max_versions": policy.get("max_versions"),
                              "max_age_seconds": policy.get("max_age_seconds")}
                self._audit_append(tenant, "get_retention", "ok")
                return result

    def purge_retention(self, dry_run=True, tenant=DEFAULT_TENANT):
        """Apply *tenant*'s saved retention policy to its object versions.

        Returns ``{"versions": [...], "bytes": <int>, "dry_run": <bool>}``
        where the versions are the entries the policy removes (or would
        remove), sorted by ``version_id``, and bytes is the sum of their
        sizes. The current version of every key is always kept; of the
        remaining versions, any beyond the newest ``max_versions`` per key
        or older than ``max_age_seconds`` (compared against the current
        UTC time) are removed. A dry run changes nothing. Without a saved
        policy the result is empty.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "purge_retention"):
            if not isinstance(dry_run, bool):
                raise ObjectStoreError("invalid dry_run")
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._retention.get(tenant)
                if policy is None:
                    result = {"versions": [], "bytes": 0, "dry_run": dry_run}
                    self._audit_append(tenant, "purge_retention", "ok")
                    return result
                max_versions = policy.get("max_versions")
                max_age = policy.get("max_age_seconds")
                now = datetime.now(timezone.utc)
                keymap = self._versions.get(tenant, {})
                plan = {}
                purged = []
                for key in sorted(keymap):
                    versions = keymap[key]
                    if not versions:
                        continue
                    remove = set()
                    if max_versions is not None:
                        keep = max(1, max_versions)
                        for version in versions[:-keep]:
                            remove.add(version["version_id"])
                    if max_age is not None:
                        cutoff = now - timedelta(seconds=max_age)
                        for version in versions[:-1]:
                            try:
                                created = datetime.fromisoformat(
                                    version["created_at"])
                            except ValueError:
                                continue
                            if created < cutoff:
                                remove.add(version["version_id"])
                    if not remove:
                        continue
                    plan[key] = remove
                    for version in versions:
                        if version["version_id"] in remove:
                            purged.append({"key": key,
                                           "version_id": version["version_id"],
                                           "sha256": version["sha256"],
                                           "size": version["size"],
                                           "created_at": version["created_at"]})
                purged.sort(key=lambda item: item["version_id"])
                result = {"versions": purged,
                          "bytes": sum(item["size"] for item in purged),
                          "dry_run": dry_run}
                if dry_run or not plan:
                    self._audit_append(tenant, "purge_retention", "ok")
                    return result
                saved = {key: list(versions) for key, versions in keymap.items()}
                for key, remove in plan.items():
                    remaining = [version for version in keymap[key]
                                 if version["version_id"] not in remove]
                    if remaining:
                        keymap[key] = remaining
                    else:
                        self._drop_versions_key(tenant, key)
                if tenant in self._versions and not self._versions[tenant]:
                    del self._versions[tenant]

                def rollback():
                    if saved:
                        self._versions[tenant] = saved
                    else:
                        self._versions.pop(tenant, None)

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("version io error")
                try:
                    self._audit_append(tenant, "purge_retention", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise
                return result

    # -- quota and usage -------------------------------------------------
    @staticmethod
    def _key_usage(entry, versions):
        """Return the logical (bytes, objects) one key contributes.

        Every retained version counts once; the current object entry counts
        only when it is not a pointer at the newest version's content, so a
        versioned key is never double-counted while an unversioned overwrite
        of a formerly versioned key still counts both the retained history
        and the diverging current object.
        """
        total_bytes = 0
        total_objects = 0
        for version in versions or ():
            total_bytes += version["size"]
            total_objects += 1
        if entry is not None and not (
                versions and versions[-1]["sha256"] == entry["sha256"]):
            total_bytes += entry["size"]
            total_objects += 1
        return total_bytes, total_objects

    def _usage_unlocked(self, tenant):
        """Return *tenant*'s current usage; must be called under the lock."""
        objects = self._objects.get(tenant, {})
        keymap = self._versions.get(tenant, {})
        used_bytes = 0
        used_objects = 0
        for key in set(objects) | set(keymap):
            delta_bytes, delta_objects = self._key_usage(
                objects.get(key), keymap.get(key) or None)
            used_bytes += delta_bytes
            used_objects += delta_objects
        reserved_bytes = 0
        reserved_sessions = 0
        for session in self._uploads.values():
            if session["tenant"] == tenant:
                reserved_bytes += session["size"]
                reserved_sessions += 1
        return {"used_bytes": used_bytes, "used_objects": used_objects,
                "reserved_bytes": reserved_bytes,
                "reserved_sessions": reserved_sessions}

    def _publish_delta(self, tenant, key, entry, versioning_on):
        """Return the (bytes, objects) *tenant* gains when *entry* is published.

        Must be called under the store lock, before the mutation happens.
        """
        objects = self._objects.get(tenant, {})
        versions = self._versions.get(tenant, {}).get(key) or None
        old_bytes, old_objects = self._key_usage(objects.get(key), versions)
        if versioning_on:
            new_versions = list(versions or ()) + [
                {"sha256": entry["sha256"], "size": entry["size"]}]
        else:
            new_versions = versions
        new_bytes, new_objects = self._key_usage(entry, new_versions)
        return new_bytes - old_bytes, new_objects - old_objects

    def _enforce_quota(self, tenant, delta_bytes, delta_objects,
                       release_session=None):
        """Raise ``quota exceeded`` when the delta would break *tenant*'s quota.

        Byte limits apply to used plus reserved space. *release_session* is
        an active upload session whose own reservation is lifted before the
        check, because completing it turns those reserved bytes into the used
        bytes being accounted here. Must be called under the store lock.
        """
        policy = self._quota.get(tenant)
        if not policy:
            return
        usage = self._usage_unlocked(tenant)
        reserved = usage["reserved_bytes"]
        if release_session is not None:
            reserved -= release_session["size"]
        max_bytes = policy.get("max_bytes")
        if max_bytes is not None and \
                usage["used_bytes"] + reserved + delta_bytes > max_bytes:
            raise ObjectStoreError("quota exceeded")
        max_objects = policy.get("max_objects")
        if max_objects is not None and \
                usage["used_objects"] + delta_objects > max_objects:
            raise ObjectStoreError("quota exceeded")

    def _enforce_reservation(self, tenant, size):
        """Raise ``quota exceeded`` when reserving *size* more bytes breaks quota."""
        policy = self._quota.get(tenant)
        if not policy:
            return
        max_bytes = policy.get("max_bytes")
        if max_bytes is None:
            return
        usage = self._usage_unlocked(tenant)
        if usage["used_bytes"] + usage["reserved_bytes"] + size > max_bytes:
            raise ObjectStoreError("quota exceeded")

    def set_quota(self, max_bytes=None, max_objects=None, tenant=DEFAULT_TENANT):
        """Save the quota policy of *tenant*.

        At least one of *max_bytes* and *max_objects* must be given, and
        each given value must be a non-negative integer (booleans excluded);
        anything else raises ``ObjectStoreError("invalid quota")``. The
        policy is persisted with the index; an I/O failure while saving
        raises ``ObjectStoreError("quota io error")`` and keeps the previous
        policy. *max_bytes* limits used plus reserved bytes, *max_objects*
        the number of logical objects (current objects plus retained
        versions).
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "set_quota"):
            for value in (max_bytes, max_objects):
                if value is not None and (isinstance(value, bool)
                                          or not isinstance(value, int)
                                          or value < 0):
                    raise ObjectStoreError("invalid quota")
            if max_bytes is None and max_objects is None:
                raise ObjectStoreError("invalid quota")
            policy = {}
            if max_bytes is not None:
                policy["max_bytes"] = max_bytes
            if max_objects is not None:
                policy["max_objects"] = max_objects
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._quota.get(tenant)
                self._quota[tenant] = policy

                def rollback():
                    if previous is None:
                        del self._quota[tenant]
                    else:
                        self._quota[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("quota io error")
                try:
                    self._audit_append(tenant, "set_quota", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_quota(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s quota policy, or None when none is saved.

        A saved policy is returned as a dict with both ``max_bytes`` and
        ``max_objects`` keys, unset limits reading as None.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_quota"):
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._quota.get(tenant)
                if policy is None:
                    result = None
                else:
                    result = {"max_bytes": policy.get("max_bytes"),
                              "max_objects": policy.get("max_objects")}
                self._audit_append(tenant, "get_quota", "ok")
                return result

    def clear_quota(self, tenant=DEFAULT_TENANT):
        """Drop *tenant*'s quota policy and return None; the tenant is unlimited.

        Clearing a tenant without a policy is a no-op. An I/O failure while
        saving raises ``ObjectStoreError("quota io error")`` and keeps the
        previous policy.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "clear_quota"):
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._quota.pop(tenant, None)

                def rollback():
                    if previous is not None:
                        self._quota[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("quota io error")
                try:
                    self._audit_append(tenant, "clear_quota", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_usage(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s current usage as four integer counters.

        ``used_bytes``/``used_objects`` count logical references: every
        current object and every retained version once (a current pointer
        duplicating its newest version is not counted twice, and keys
        sharing a digest count separately). ``reserved_bytes``/
        ``reserved_sessions`` count the declared sizes of the tenant's
        active upload sessions, released on completion or abort.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_usage"):
            with self._lock:
                self._rate_limit_check(tenant)
                usage = self._usage_unlocked(tenant)
                self._audit_append(tenant, "get_usage", "ok")
                return usage

    # -- lifecycle -------------------------------------------------------
    def set_lifecycle(self, max_age_seconds, prefix="", tenant=DEFAULT_TENANT):
        """Save *tenant*'s lifecycle expiration policy.

        *prefix* must be a string (empty when omitted) and
        *max_age_seconds* a required non-negative integer (booleans
        excluded); a missing age or any other value raises
        ``ObjectStoreError("invalid lifecycle")``. The policy only takes
        effect through :meth:`run_lifecycle` and is persisted atomically
        with the index, an I/O failure raising
        ``ObjectStoreError("lifecycle io error")`` and keeping the previous
        policy.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "set_lifecycle"):
            policy = self._check_lifecycle_values(prefix, max_age_seconds)
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._lifecycle.get(tenant)
                self._lifecycle[tenant] = policy

                def rollback():
                    if previous is None:
                        del self._lifecycle[tenant]
                    else:
                        self._lifecycle[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("lifecycle io error")
                try:
                    self._audit_append(tenant, "set_lifecycle", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    @staticmethod
    def _check_lifecycle_values(prefix, max_age_seconds):
        if not isinstance(prefix, str):
            raise ObjectStoreError("invalid lifecycle")
        if isinstance(max_age_seconds, bool) \
                or not isinstance(max_age_seconds, int) \
                or max_age_seconds < 0:
            raise ObjectStoreError("invalid lifecycle")
        return {"prefix": prefix, "max_age_seconds": max_age_seconds}

    def get_lifecycle(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s lifecycle policy, or None when none is saved."""
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_lifecycle"):
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._lifecycle.get(tenant)
                if policy is None:
                    result = None
                else:
                    result = {"prefix": policy["prefix"],
                              "max_age_seconds": policy["max_age_seconds"]}
                self._audit_append(tenant, "get_lifecycle", "ok")
                return result

    def clear_lifecycle(self, tenant=DEFAULT_TENANT):
        """Drop *tenant*'s lifecycle policy and return None.

        Clearing a tenant without a policy is a no-op. An I/O failure while
        saving raises ``ObjectStoreError("lifecycle io error")`` and keeps
        the previous policy.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "clear_lifecycle"):
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._lifecycle.pop(tenant, None)

                def rollback():
                    if previous is not None:
                        self._lifecycle[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("lifecycle io error")
                try:
                    self._audit_append(tenant, "clear_lifecycle", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    @staticmethod
    def _parse_timestamp(value):
        """Parse an ISO UTC timestamp; a naive value is assumed to be UTC."""
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    def run_lifecycle(self, dry_run=True, tenant=DEFAULT_TENANT):
        """Preview or delete *tenant*'s expired current objects.

        Returns ``{"objects": [...], "bytes": <int>, "dry_run": <bool>}``.
        The cutoff is the current UTC time minus the policy's
        ``max_age_seconds``; only the tenant's current objects whose keys
        start with the policy ``prefix`` and whose last-modified time is
        older than the cutoff are candidates, sorted by key. Objects whose
        index entry has no last-modified time (objects first written by an
        older version) are timeless and always skipped. Each result entry
        holds ``key``, ``sha256`` and ``size`` and bytes is the sum of
        their sizes. Without a saved policy the result is empty. A dry run
        (the default) changes nothing; an actual run performs every removal
        under the store lock in one atomic index rewrite, following
        :meth:`delete`'s current-version removal and old-version promotion
        rules for versioned tenants and simply dropping the mapping for
        unversioned ones. Active upload sessions are never touched, and
        only metadata references are removed: the blob bytes stay for
        :meth:`collect_garbage`. An I/O failure restores every object map,
        version list and (derived) quota counter and raises
        ``ObjectStoreError("lifecycle io error")``.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "run_lifecycle"):
            if not isinstance(dry_run, bool):
                raise ObjectStoreError("invalid dry_run")
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._lifecycle.get(tenant)
                if policy is None:
                    result = {"objects": [], "bytes": 0, "dry_run": dry_run}
                    self._audit_append(tenant, "run_lifecycle", "ok")
                    return result
                prefix = policy["prefix"]
                cutoff = datetime.now(timezone.utc) - timedelta(
                    seconds=policy["max_age_seconds"])
                objects = self._objects.get(tenant, {})
                timestamps = self._timestamps.get(tenant, {})
                candidates = []
                for key in sorted(objects):
                    if not key.startswith(prefix):
                        continue
                    last_modified = timestamps.get(key)
                    if last_modified is None:
                        continue
                    if self._parse_timestamp(last_modified) < cutoff:
                        entry = objects[key]
                        candidates.append({
                            "key": key,
                            "sha256": entry["sha256"],
                            "size": entry["size"],
                        })
                result = {"objects": candidates,
                          "bytes": sum(item["size"] for item in candidates),
                          "dry_run": dry_run}
                if dry_run or not candidates:
                    self._audit_append(tenant, "run_lifecycle", "ok")
                    return result
                saved_objects = {name: dict(mapping)
                                 for name, mapping in self._objects.items()}
                saved_timestamps = {name: dict(mapping)
                                    for name, mapping in self._timestamps.items()}
                saved_versions = {
                    name: {key: list(versions)
                           for key, versions in keymap.items()}
                    for name, keymap in self._versions.items()}

                def rollback():
                    self._objects = saved_objects
                    self._timestamps = saved_timestamps
                    self._versions = saved_versions

                try:
                    for item in candidates:
                        self._remove_current_unlocked(tenant, item["key"])
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("lifecycle io error")
                try:
                    self._audit_append(tenant, "run_lifecycle", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise
                return result

    # -- access policy and read signatures -------------------------------
    @staticmethod
    def _access_signature(secret, tenant, method, key, version_id, expires):
        """Return the HMAC-SHA256 hex digest signing one read request."""
        payload = "\n".join((tenant, method, key,
                             version_id if version_id is not None else "",
                             str(expires)))
        return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"),
                        hashlib.sha256).hexdigest()

    def get_access_policy(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s read access policy.

        The result is ``{"mode": "public"}`` when no policy is saved (the
        default, and the state every index written by an older version is
        in) or ``{"mode": "signed", "secret": <str>}`` for a signed tenant.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_access_policy"):
            with self._lock:
                self._rate_limit_check(tenant)
                policy = self._access.get(tenant)
                if policy is None:
                    result = {"mode": "public"}
                else:
                    result = {"mode": "signed", "secret": policy["secret"]}
                self._audit_append(tenant, "get_access_policy", "ok")
                return result

    def set_access_policy(self, mode, secret=None, tenant=DEFAULT_TENANT):
        """Save *tenant*'s read access policy.

        *mode* must be ``"public"`` or ``"signed"``; a signed policy
        requires *secret* to be a 16 to 256 character UTF-8 string. Any
        other input raises ``ObjectStoreError("invalid access policy")``.
        Setting ``"public"`` is equivalent to :meth:`clear_access_policy`.
        The policy is persisted atomically with the index; an I/O failure
        while saving raises ``ObjectStoreError("access policy io error")``
        and keeps the previous policy.
        """
        self._check_tenant(tenant)
        if mode == "public":
            self.clear_access_policy(tenant=tenant)
            return
        with self._audit_failures(tenant, "set_access_policy"):
            if mode != "signed":
                raise ObjectStoreError("invalid access policy")
            secret = self._check_secret(secret, "invalid access policy")
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._access.get(tenant, _MISSING)
                self._access[tenant] = {"mode": "signed", "secret": secret}

                def rollback():
                    if previous is _MISSING:
                        del self._access[tenant]
                    else:
                        self._access[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("access policy io error")
                try:
                    self._audit_append(tenant, "set_access_policy", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def clear_access_policy(self, tenant=DEFAULT_TENANT):
        """Drop *tenant*'s access policy, restoring public reads.

        Clearing a tenant without a policy is a no-op. An I/O failure
        while saving raises ``ObjectStoreError("access policy io error")``
        and keeps the previous policy.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "clear_access_policy"):
            with self._lock:
                self._rate_limit_check(tenant)
                previous = self._access.pop(tenant, None)

                def rollback():
                    if previous is not None:
                        self._access[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("access policy io error")
                try:
                    self._audit_append(tenant, "clear_access_policy", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def sign_request(self, method, key, version_id=None, expires=None,
                     tenant=DEFAULT_TENANT):
        """Sign a read request for *key* in *tenant* and return the hex digest.

        The signed payload is the tenant name, HTTP *method*, object
        *key*, *version_id* (empty when None) and *expires*, joined by
        newlines and hashed with HMAC-SHA256 keyed by the tenant's saved
        secret. *method* must be ``"GET"`` or ``"HEAD"`` and *expires* a
        future integer Unix timestamp; anything else -- or a tenant
        without a signed policy -- raises
        ``ObjectStoreError("invalid signature")``.
        """
        self._check_tenant(tenant)
        self._check_key(key)
        if method not in ("GET", "HEAD"):
            raise ObjectStoreError("invalid signature")
        if version_id is not None and not isinstance(version_id, str):
            raise ObjectStoreError("invalid signature")
        if isinstance(expires, bool) or not isinstance(expires, int) \
                or expires <= time.time():
            raise ObjectStoreError("invalid signature")
        with self._lock:
            policy = self._access.get(tenant)
            if policy is None:
                raise ObjectStoreError("invalid signature")
            return self._access_signature(policy["secret"], tenant, method,
                                          key, version_id, expires)

    def _check_read_access(self, tenant, key, version_id, expires, signature,
                           method):
        """Enforce *tenant*'s read policy; must be called under the lock.

        Public tenants (no saved policy) read freely. Signed tenants must
        present a future integer *expires* and the 64 lowercase hex digits
        of the matching signature, compared in constant time; every other
        input is ``ObjectStoreError("access denied")``, raised before any
        key or version is looked up.
        """
        policy = self._access.get(tenant)
        if policy is None:
            return
        if version_id is not None and not isinstance(version_id, str):
            raise ObjectStoreError("access denied")
        if isinstance(expires, bool) or not isinstance(expires, int) \
                or expires <= time.time():
            raise ObjectStoreError("access denied")
        if not _is_sha256(signature):
            raise ObjectStoreError("access denied")
        expected = self._access_signature(policy["secret"], tenant, method,
                                          key, version_id, expires)
        if not hmac.compare_digest(signature, expected):
            raise ObjectStoreError("access denied")

    def _access_mode(self, tenant):
        """Return ``"public"``/``"signed"`` without rate-limit accounting.

        Used internally by front ends that need the mode to prepare a
        request; the request itself is accounted by the operation it ends
        up calling.
        """
        with self._lock:
            return "signed" if self._access.get(tenant) is not None else "public"

    # -- rate limiting ---------------------------------------------------
    def _rate_limit_check(self, tenant, body_bytes=0):
        """Admit and account one request against *tenant*'s rate limit.

        Must be called under the store lock, after tenant, framing,
        signature and precondition validation and before any state change
        or response. The window is fixed and aligned to the Unix epoch:
        counters reset whenever the window changes. A permitted request is
        accounted immediately (one request plus *body_bytes* against
        ``max_bytes``); a rejected one raises :class:`RateLimitExceeded`
        without touching storage or the counters. Tenants without a saved
        policy are unlimited.
        """
        policy = self._rate_limit.get(tenant)
        if policy is None:
            return
        now = int(time.time())
        window = policy["window_seconds"]
        window_start = (now // window) * window
        state = self._rate_windows.get(tenant)
        if state is None or state["window_start"] != window_start:
            state = self._rate_windows[tenant] = {
                "window_start": window_start, "requests": 0, "bytes": 0}
        max_requests = policy.get("max_requests")
        max_bytes = policy.get("max_bytes")
        if max_requests is not None and state["requests"] + 1 > max_requests \
                or max_bytes is not None and state["bytes"] + body_bytes > max_bytes:
            raise RateLimitExceeded(window_start + window - now)
        state["requests"] += 1
        state["bytes"] += body_bytes

    def set_rate_limit(self, window_seconds, max_requests=None, max_bytes=None,
                       tenant=DEFAULT_TENANT):
        """Save *tenant*'s rate limit policy.

        *window_seconds* is required and must be an integer between 1 and
        86400 (booleans excluded); at least one of *max_requests* and
        *max_bytes* must be given as a non-negative integer (booleans
        excluded). Anything else raises ``ObjectStoreError("invalid rate
        limit")``. The policy is persisted with the index; an I/O failure
        while saving raises ``ObjectStoreError("rate limit io error")``
        and keeps the previous policy. The counters of the current window
        are kept; a window switch always resets them.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "set_rate_limit"):
            if isinstance(window_seconds, bool) \
                    or not isinstance(window_seconds, int) \
                    or not 1 <= window_seconds <= 86400:
                raise ObjectStoreError("invalid rate limit")
            for value in (max_requests, max_bytes):
                if value is not None and (isinstance(value, bool)
                                          or not isinstance(value, int)
                                          or value < 0):
                    raise ObjectStoreError("invalid rate limit")
            if max_requests is None and max_bytes is None:
                raise ObjectStoreError("invalid rate limit")
            policy = {"window_seconds": window_seconds}
            if max_requests is not None:
                policy["max_requests"] = max_requests
            if max_bytes is not None:
                policy["max_bytes"] = max_bytes
            with self._lock:
                previous = self._rate_limit.get(tenant)
                self._rate_limit[tenant] = policy

                def rollback():
                    if previous is None:
                        del self._rate_limit[tenant]
                    else:
                        self._rate_limit[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("rate limit io error")
                try:
                    self._audit_append(tenant, "set_rate_limit", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_rate_limit(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s rate limit policy, or None when none is saved.

        A saved policy is returned as a dict with ``window_seconds``,
        ``max_requests`` and ``max_bytes`` keys, unset limits reading as
        None.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_rate_limit"):
            with self._lock:
                policy = self._rate_limit.get(tenant)
                if policy is None:
                    result = None
                else:
                    result = {"window_seconds": policy["window_seconds"],
                              "max_requests": policy.get("max_requests"),
                              "max_bytes": policy.get("max_bytes")}
                self._audit_append(tenant, "get_rate_limit", "ok")
                return result

    def clear_rate_limit(self, tenant=DEFAULT_TENANT):
        """Drop *tenant*'s rate limit policy and return None.

        Clearing a tenant without a policy is a no-op. An I/O failure
        while saving raises ``ObjectStoreError("rate limit io error")``
        and keeps the previous policy.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "clear_rate_limit"):
            with self._lock:
                previous = self._rate_limit.pop(tenant, None)

                def rollback():
                    if previous is not None:
                        self._rate_limit[tenant] = previous

                try:
                    self._save_index()
                except OSError:
                    rollback()
                    raise ObjectStoreError("rate limit io error")
                try:
                    self._audit_append(tenant, "clear_rate_limit", "ok")
                except ObjectStoreError:
                    rollback()
                    try:
                        self._save_index()
                    except OSError:
                        pass
                    raise

    def get_rate_limit_usage(self, tenant=DEFAULT_TENANT):
        """Return *tenant*'s current-window counters, or None without a policy.

        The result holds ``window_start`` and ``reset_at`` (integer Unix
        seconds bounding the current fixed window) plus the ``requests``
        and ``bytes`` accounted so far in it. Counters live in memory
        only: after a restart they start at zero for the current window.
        """
        self._check_tenant(tenant)
        with self._audit_failures(tenant, "get_rate_limit_usage"):
            with self._lock:
                policy = self._rate_limit.get(tenant)
                if policy is None:
                    self._audit_append(tenant, "get_rate_limit_usage", "ok")
                    return None
                now = int(time.time())
                window = policy["window_seconds"]
                window_start = (now // window) * window
                state = self._rate_windows.get(tenant)
                requests = bytes_used = 0
                if state is not None and state["window_start"] == window_start:
                    requests = state["requests"]
                    bytes_used = state["bytes"]
                result = {"window_start": window_start,
                          "reset_at": window_start + window,
                          "requests": requests,
                          "bytes": bytes_used}
                self._audit_append(tenant, "get_rate_limit_usage", "ok")
                return result

    # -- audit log -------------------------------------------------------
    _AUDIT_REQUIRED = frozenset(
        ("seq", "timestamp", "tenant", "operation", "result",
         "prev_hash", "hash"))
    _AUDIT_OPTIONAL = frozenset(
        ("key", "session", "version", "sha256", "size", "error"))

    def _load_audit(self):
        """Read the audit log and return the per-tenant chain state.

        A missing log reads as empty. A trailing incomplete record (no
        terminating newline, left by an interrupted write) is discarded
        and the file truncated back to the last complete record, so the
        sequence continues cleanly after a crash. Any other format
        error, sequence gap or chain mismatch raises
        ``ObjectStoreError("audit corrupted")``.
        """
        events, valid_length, total_length = self._read_audit_events()
        if valid_length < total_length:
            try:
                with open(self.audit_path, "r+b") as handle:
                    handle.truncate(valid_length)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                raise ObjectStoreError("audit io error")
        self._audit_size = valid_length
        return self._audit_chain_state(events)

    def _audit_resync(self):
        """Re-read the log when it diverged from the last append.

        Must be called under the store lock. A trailing incomplete
        record (e.g. left by a failed append) is dropped and the chain
        state rebuilt, so appending always continues a valid log.
        """
        try:
            size = os.path.getsize(self.audit_path)
        except FileNotFoundError:
            size = 0
        except OSError:
            raise ObjectStoreError("audit io error")
        if size == self._audit_size:
            return
        events, valid_length, _ = self._read_audit_events()
        if valid_length != size:
            try:
                with open(self.audit_path, "r+b") as handle:
                    handle.truncate(valid_length)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                raise ObjectStoreError("audit io error")
        self._audit_state = self._audit_chain_state(events)
        self._audit_size = valid_length

    def _read_audit_events(self):
        """Return ``(events, valid_length, total_length)`` for the log.

        Every complete record is parsed, field-validated and checked
        against its own ``hash``; the per-tenant sequence and
        ``prev_hash`` linkage is verified by :meth:`_audit_chain_state`.
        """
        try:
            with open(self.audit_path, "rb") as handle:
                data = handle.read()
        except FileNotFoundError:
            return [], 0, 0
        except OSError:
            raise ObjectStoreError("audit io error")
        total_length = len(data)
        if not data:
            return [], 0, 0
        if data.endswith(b"\n"):
            valid_length = total_length
        else:
            # A final record without its terminating newline is
            # incomplete: it is dropped, never parsed.
            valid_length = data.rfind(b"\n") + 1
        events = []
        if valid_length:
            for raw in data[:valid_length - 1].split(b"\n"):
                events.append(self._parse_audit_event(raw))
        self._audit_chain_state(events)
        return events, valid_length, total_length

    @classmethod
    def _parse_audit_event(cls, raw):
        """Parse and validate one audit record or raise ``audit corrupted``."""
        try:
            event = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise ObjectStoreError("audit corrupted")
        if not isinstance(event, dict) \
                or not cls._AUDIT_REQUIRED <= set(event) \
                or set(event) - cls._AUDIT_REQUIRED - cls._AUDIT_OPTIONAL:
            raise ObjectStoreError("audit corrupted")
        seq = event["seq"]
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise ObjectStoreError("audit corrupted")
        if not isinstance(event["timestamp"], str) \
                or not isinstance(event["operation"], str) \
                or event["result"] not in ("ok", "error"):
            raise ObjectStoreError("audit corrupted")
        tenant = event["tenant"]
        if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
            raise ObjectStoreError("audit corrupted")
        if not _is_sha256(event["prev_hash"]) or not _is_sha256(event["hash"]):
            raise ObjectStoreError("audit corrupted")
        for name in ("key", "session", "version", "error"):
            if name in event and not isinstance(event[name], str):
                raise ObjectStoreError("audit corrupted")
        if "sha256" in event and not _is_sha256(event["sha256"]):
            raise ObjectStoreError("audit corrupted")
        size = event.get("size")
        if size is not None and (isinstance(size, bool)
                                 or not isinstance(size, int) or size < 0):
            raise ObjectStoreError("audit corrupted")
        payload = {name: value for name, value in event.items()
                   if name != "hash"}
        canonical = json.dumps(payload, sort_keys=True,
                               separators=(",", ":")).encode("utf-8")
        if hashlib.sha256(canonical).hexdigest() != event["hash"]:
            raise ObjectStoreError("audit corrupted")
        return event

    @staticmethod
    def _audit_chain_state(events):
        """Verify the per-tenant seq/prev_hash chains over *events*.

        Returns ``{tenant: {"next_seq", "head"}}`` for appending the next
        record of each tenant.
        """
        state = {}
        for event in events:
            tenant = event["tenant"]
            current = state.get(tenant)
            expected_seq = 1 if current is None else current["next_seq"]
            prev_hash = _AUDIT_GENESIS if current is None else current["head"]
            if event["seq"] != expected_seq or event["prev_hash"] != prev_hash:
                raise ObjectStoreError("audit corrupted")
            state[tenant] = {"next_seq": expected_seq + 1,
                             "head": event["hash"]}
        return state

    def _audit_append(self, tenant, operation, result, key=None, session=None,
                      version=None, sha256=None, size=None, error=None):
        """Append one event to *tenant*'s audit chain and return it.

        The record is written and fsynced before the caller's operation
        returns; a write or flush failure raises
        ``ObjectStoreError("audit io error")``. Target fields are only
        recorded when they hold well-formed values, so a failed call can
        never smuggle an unparseable value into the log. Payloads,
        request bodies and secrets are never recorded.
        """
        with self._lock:
            self._audit_resync()
            state = self._audit_state.get(tenant)
            seq = 1 if state is None else state["next_seq"]
            prev_hash = _AUDIT_GENESIS if state is None else state["head"]
            event = {"seq": seq,
                     "timestamp": datetime.now(timezone.utc).isoformat(),
                     "tenant": tenant,
                     "operation": operation,
                     "result": result}
            if isinstance(key, str):
                event["key"] = key
            if isinstance(session, str):
                event["session"] = session
            if isinstance(version, str):
                event["version"] = version
            if _is_sha256(sha256):
                event["sha256"] = sha256
            if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
                event["size"] = size
            if isinstance(error, str):
                event["error"] = error
            event["prev_hash"] = prev_hash
            payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
            event["hash"] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            line = json.dumps(event, sort_keys=True, separators=(",", ":"))
            record = line.encode("utf-8") + b"\n"
            try:
                with open(self.audit_path, "ab") as handle:
                    handle.write(record)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                raise ObjectStoreError("audit io error")
            self._audit_size += len(record)
            self._audit_state[tenant] = {"next_seq": seq + 1,
                                         "head": event["hash"]}
            return event

    @contextmanager
    def _audit_failures(self, tenant, operation, key=None, session=None,
                        version=None):
        """Record an ``error`` event when the wrapped operation raises.

        Success events are appended explicitly by the operation itself,
        at the point where the result is known and -- for writes -- the
        mutation can still be rolled back when the audit append fails.
        When the failure event itself cannot be written, the uniform
        ``audit io error`` replaces the original error.
        """
        try:
            yield
        except ObjectStoreError as exc:
            self._audit_append(tenant, operation, "error", key=key,
                               session=session, version=version,
                               error=str(exc))
            raise

    @staticmethod
    def _check_audit_tenant(tenant):
        if not isinstance(tenant, str) or _TENANT_RE.match(tenant) is None:
            raise ObjectStoreError("invalid audit query")
        return tenant

    def list_audit_events(self, after=None, limit=DEFAULT_LIMIT,
                          tenant=DEFAULT_TENANT):
        """Return ``{"items": [...], "next_after": <int|null>}`` for one page.

        Events of *tenant* (and only that tenant) are listed in
        ascending ``seq`` order; *after* is an exclusive non-negative
        integer ``seq`` cursor and *limit* must be between 1 and 1000.
        A tenant without events yields an empty page. Any other
        parameter shape raises ``ObjectStoreError("invalid audit
        query")``. The query records no audit event of its own.
        """
        self._check_audit_tenant(tenant)
        if after is None:
            after = 0
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise ObjectStoreError("invalid audit query")
        if isinstance(limit, bool) or not isinstance(limit, int) \
                or not 1 <= limit <= MAX_LIMIT:
            raise ObjectStoreError("invalid audit query")
        with self._lock:
            events, _, _ = self._read_audit_events()
            mine = [event for event in events
                    if event["tenant"] == tenant and event["seq"] > after]
            page = mine[:limit]
            return {"items": [dict(event) for event in page],
                    "next_after": page[-1]["seq"]
                    if len(mine) > len(page) else None}

    def verify_audit_log(self, tenant=DEFAULT_TENANT):
        """Verify *tenant*'s audit chain and return its head summary.

        The whole log is re-read and re-validated on every call; any
        format error, sequence gap or chain mismatch raises
        ``ObjectStoreError("audit corrupted")``. The result is
        ``{"ok": True, "events": <int>, "last_seq": <int|null>,
        "head": <str|null>}`` with ``last_seq``/``head`` reading None
        for a tenant without events. The check records no audit event
        of its own.
        """
        self._check_audit_tenant(tenant)
        with self._lock:
            events, _, _ = self._read_audit_events()
            mine = [event for event in events if event["tenant"] == tenant]
            if not mine:
                return {"ok": True, "events": 0,
                        "last_seq": None, "head": None}
            return {"ok": True, "events": len(mine),
                    "last_seq": mine[-1]["seq"], "head": mine[-1]["hash"]}

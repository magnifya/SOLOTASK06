"""Standard-library HTTP front end for :class:`ContentAddressedStore`.

Every failure is a single-line JSON body ``{"error": "..."}`` with status 400,
403, 404, 405, 409, 411, 412, 413 or 500 (500 for a corrupted blob or a
blob/upload I/O error, 413 for a write rejected by the tenant's quota, 403
for a read rejected by the tenant's signed access policy).

The object and upload-session endpoints select their tenant through the
optional ``X-Objstore-Tenant`` request header (default ``default``); the blob
endpoints stay global and ignore it. The versioning and retention endpoints
(``/v1/versioning``, ``/v1/objects/{key}/versions[...]``, ``/v1/retention``
and ``/v1/retention/purge``), the quota endpoints (``/v1/quota`` and
``/v1/usage``), the lifecycle endpoints (``/v1/lifecycle`` and
``/v1/lifecycle/run``) and the access-policy endpoint (``/v1/access-policy``)
are tenant-scoped the same way. While a tenant's policy is ``signed``, object
and version GET/HEAD requests must carry a future ``expires`` timestamp and a
matching ``signature`` query parameter or they fail with
``403 {"error":"access denied"}``; public tenants ignore both parameters.
"""

from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .store import (
    DEFAULT_LIMIT,
    DEFAULT_TENANT,
    ContentAddressedStore,
    ObjectStoreError,
    SessionConflict,
    _MISSING,
)

__all__ = ["ObjectStoreHandler", "ObjectStoreHTTPServer", "create_server"]

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_ETAG_RE = re.compile(r'\A"([0-9a-f]{64})"\Z')
_OBJECTS_PATH = "/v1/objects"
_BLOBS_PATH = "/v1/blobs"
_UPLOADS_PATH = "/v1/uploads"
_VERSIONING_PATH = "/v1/versioning"
_RETENTION_PATH = "/v1/retention"
_RETENTION_PURGE_PATH = "/v1/retention/purge"
_QUOTA_PATH = "/v1/quota"
_USAGE_PATH = "/v1/usage"
_LIFECYCLE_PATH = "/v1/lifecycle"
_LIFECYCLE_RUN_PATH = "/v1/lifecycle/run"
_ACCESS_POLICY_PATH = "/v1/access-policy"
_TENANT_HEADER = "X-Objstore-Tenant"
_DEFAULT_CONTENT_TYPE = "application/octet-stream"
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")
_BAD_REQUEST_PREFIXES = (
    "invalid sha256", "invalid limit", "invalid key", "invalid prefix",
    "invalid after", "invalid size", "invalid offset", "invalid json",
    "invalid precondition", "invalid tenant", "invalid version",
    "invalid retention", "invalid dry_run", "invalid quota",
    "invalid lifecycle", "invalid access policy", "versioning disabled",
    "payload", "content_type",
)
_INTERNAL_ERRORS = ("corrupted blob", "blob io error", "upload io error",
                    "version io error", "quota io error", "lifecycle io error",
                    "access policy io error")


def _json_bytes(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _status_for(message):
    if message in _INTERNAL_ERRORS:
        return 500
    if message == "precondition failed":
        return 412
    if message == "quota exceeded":
        return 413
    if message == "access denied":
        return 403
    for prefix in _BAD_REQUEST_PREFIXES:
        if message.startswith(prefix):
            return 400
    return 404


class ObjectStoreHandler(BaseHTTPRequestHandler):
    """Routes the object-store REST surface onto a ContentAddressedStore."""

    server_version = "objstore/0.1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status, body, content_type="application/json", extra_headers=(),
              head_only=False, length=None):
        self.send_response(status)
        if status != 204:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body) if length is None else length))
        for name, value in extra_headers:
            self.send_header(name, value)
        self.end_headers()
        if body and not head_only and status != 204:
            self.wfile.write(body)

    def _error(self, status, message, close=False):
        extra = [("Connection", "close")] if close else ()
        if close:
            self.close_connection = True
        self._send(status, _json_bytes({"error": message}), extra_headers=extra)

    def _read_body(self):
        raw = self.headers.get("Content-Length")
        try:
            length = int(raw)
        except (TypeError, ValueError):
            return None
        if length < 0:
            return None
        return self.rfile.read(length) if length else b""

    def _tenant(self):
        """Return the tenant selected by the ``X-Objstore-Tenant`` header.

        The header is optional and defaults to ``default``; a repeated
        header or an invalid name is an ``invalid tenant`` error that takes
        precedence over every other request problem. The connection is
        closed on such an error because an unread request body may still be
        pending and would otherwise be parsed as the next request.
        """
        values = self.headers.get_all(_TENANT_HEADER) or []
        if len(values) > 1:
            self.close_connection = True
            raise ObjectStoreError("invalid tenant: repeated header")
        tenant = values[0] if values else DEFAULT_TENANT
        try:
            ContentAddressedStore._check_tenant(tenant)
        except ObjectStoreError:
            self.close_connection = True
            raise
        return tenant

    def _precondition_headers(self):
        """Map If-Match / If-None-Match onto an ``expected_sha256`` argument.

        Returns the omitted sentinel when no condition is present, ``None``
        for ``If-None-Match: *`` (key must be absent), or the 64 hex digits
        of a single double-quoted ``If-Match`` digest. Both headers at once,
        a repeated header, or any other syntax is an ``invalid precondition``
        error.
        """
        if_match = self.headers.get_all("If-Match") or []
        if_none = self.headers.get_all("If-None-Match") or []
        if len(if_match) > 1 or len(if_none) > 1 or (if_match and if_none):
            raise ObjectStoreError("invalid precondition")
        if if_match:
            match = _ETAG_RE.match(if_match[0])
            if match is None:
                raise ObjectStoreError("invalid precondition")
            return match.group(1)
        if if_none:
            if if_none[0] != "*":
                raise ObjectStoreError("invalid precondition")
            return None
        return _MISSING

    @staticmethod
    def _etag_headers(entry):
        return [("X-Content-Sha256", entry["sha256"]),
                ("ETag", '"%s"' % entry["sha256"])]

    def do_GET(self):
        self._dispatch("GET")

    def do_HEAD(self):
        self._dispatch("HEAD")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/healthz":
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._send(200, _json_bytes({"ok": True}))
            if path == _OBJECTS_PATH:
                tenant = self._tenant()
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._list(parsed.query, tenant)
            if path == _VERSIONING_PATH:
                return self._versioning(method, self._tenant())
            if path == _RETENTION_PATH:
                return self._retention(method, self._tenant())
            if path == _RETENTION_PURGE_PATH:
                tenant = self._tenant()
                if method != "POST":
                    return self._error(405, "method not allowed")
                return self._purge_retention(tenant)
            if path == _QUOTA_PATH:
                return self._quota(method, self._tenant())
            if path == _USAGE_PATH:
                tenant = self._tenant()
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._send(200, _json_bytes(
                    self.server.store.get_usage(tenant=tenant)))
            if path == _LIFECYCLE_PATH:
                return self._lifecycle(method, self._tenant())
            if path == _LIFECYCLE_RUN_PATH:
                tenant = self._tenant()
                if method != "POST":
                    return self._error(405, "method not allowed")
                return self._run_lifecycle(tenant)
            if path == _ACCESS_POLICY_PATH:
                return self._access_policy(method, self._tenant())
            if path.startswith(_OBJECTS_PATH + "/"):
                return self._object(method, unquote(path[len(_OBJECTS_PATH) + 1:]),
                                    parsed.query)
            if path == _UPLOADS_PATH:
                tenant = self._tenant()
                if self.headers.get("Transfer-Encoding") is not None:
                    self.close_connection = True
                    return self._error(400, "transfer encoding not supported")
                if method != "POST":
                    return self._error(405, "method not allowed")
                return self._create_upload(tenant)
            if path.startswith(_UPLOADS_PATH + "/"):
                tenant = self._tenant()
                return self._upload_session(
                    method, unquote(path[len(_UPLOADS_PATH) + 1:]), parsed.query,
                    tenant)
            if path.startswith(_BLOBS_PATH + "/"):
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._blob(unquote(path[len(_BLOBS_PATH) + 1:]))
            return self._error(404, "not found")
        except SessionConflict as exc:
            return self._error(409, str(exc))
        except ObjectStoreError as exc:
            return self._error(_status_for(str(exc)), str(exc))
        except (BrokenPipeError, ConnectionResetError):
            return None

    @staticmethod
    def _split_version_tail(tail):
        """Split ``{key}/versions[/{version_id}]`` tails from plain keys.

        Returns ``(key, version_id, is_version_path)``; a plain object key
        comes back unchanged with ``is_version_path`` false.
        """
        marker = "/versions"
        if tail.endswith(marker) and len(tail) > len(marker):
            return tail[:-len(marker)], None, True
        marker += "/"
        if marker in tail:
            key, _, version_id = tail.rpartition(marker)
            if key and version_id and "/" not in version_id:
                return key, version_id, True
        return tail, None, False

    @staticmethod
    def _access_params(query):
        """Extract the ``expires``/``signature`` query values of a signed read.

        Returns ``(expires, signature)`` with *expires* as an integer.
        Anything but a single decimal ``expires`` and a single ``signature``
        value collapses to ``None``, which the store rejects as
        ``access denied`` while the tenant's policy is signed; public
        tenants ignore both.
        """
        params = parse_qs(query, keep_blank_values=True)
        expires = None
        values = params.get("expires")
        if values is not None and len(values) == 1 \
                and _DECIMAL_RE.match(values[0]) is not None:
            expires = int(values[0])
        signature = None
        values = params.get("signature")
        if values is not None and len(values) == 1:
            signature = values[0]
        return expires, signature

    def _object(self, method, tail, query=""):
        tenant = self._tenant()
        if not tail:
            return self._error(400, "invalid key")
        key, version_id, is_version_path = self._split_version_tail(tail)
        if is_version_path:
            if version_id is None:
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._list_versions(key, query, tenant)
            return self._version_item(method, key, version_id, tenant, query)
        store = self.server.store
        if method == "PUT":
            body = self._read_body()
            if body is None:
                return self._error(411, "length required")
            ContentAddressedStore._check_key(key)
            expected = self._precondition_headers()
            try:
                existing = store.head(key, tenant=tenant)
            except ObjectStoreError:
                existing = None
            entry = store.put(key, body, content_type=self.headers.get("Content-Type"),
                              expected_sha256=expected, tenant=tenant)
            repeated = existing is not None and existing["sha256"] == entry["sha256"]
            body_out = {"key": key, "sha256": entry["sha256"], "size": entry["size"]}
            if "version_id" in entry:
                body_out["version_id"] = entry["version_id"]
            return self._send(200 if repeated else 201, _json_bytes(body_out))
        if method == "GET":
            expires, signature = self._access_params(query)
            data, entry = store.get(key, tenant=tenant, expires=expires,
                                    signature=signature)
            return self._send(200, data, entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              self._etag_headers(entry))
        if method == "HEAD":
            expires, signature = self._access_params(query)
            entry = store.head(key, tenant=tenant, expires=expires,
                               signature=signature)
            return self._send(200, b"", entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              self._etag_headers(entry), head_only=True,
                              length=entry["size"])
        if method == "DELETE":
            ContentAddressedStore._check_key(key)
            expected = self._precondition_headers()
            store.delete(key, expected_sha256=expected, tenant=tenant)
            return self._send(204, b"")
        return self._error(405, "method not allowed")

    # -- resumable upload sessions -------------------------------------
    def _read_length_body(self):
        """Read a body framed strictly by Content-Length.

        Transfer-Encoding wins over every other problem (400); a missing,
        non-integer or negative Content-Length is 411; a body that ends
        before the declared length is 400. Returns ``(body, (status,
        message))`` with the error element ``None`` on success.
        """
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return None, (400, "transfer encoding not supported")
        raw = self.headers.get("Content-Length")
        if raw is None or _DECIMAL_RE.match(raw) is None:
            self.close_connection = True
            return None, (411, "length required")
        length = int(raw)
        body = self.rfile.read(length) if length else b""
        if len(body) != length:
            self.close_connection = True
            return None, (400, "truncated request body")
        return body, None

    @staticmethod
    def _json_object(body):
        """Decode *body* as a UTF-8 JSON object or raise a 400-class error."""
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            raise ObjectStoreError("invalid json: body is not utf-8")
        try:
            document = json.loads(text)
        except ValueError:
            raise ObjectStoreError("invalid json: body is not a json document")
        if not isinstance(document, dict):
            raise ObjectStoreError("invalid json: body must be a json object")
        return document

    @staticmethod
    def _offset_param(query):
        """Return the unique non-negative decimal ``offset`` query value."""
        params = parse_qs(query, keep_blank_values=True)
        values = params.get("offset")
        if values is None or len(values) != 1 or _DECIMAL_RE.match(values[0]) is None:
            raise ObjectStoreError("invalid offset")
        return int(values[0])

    def _create_upload(self, tenant):
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        document = self._json_object(body)
        allowed = {"key", "size", "sha256", "content_type"}
        unknown = set(document) - allowed
        if unknown:
            raise ObjectStoreError("invalid json: unknown field %r" % (sorted(unknown)[0],))
        missing = [name for name in ("key", "size", "sha256") if name not in document]
        if missing:
            raise ObjectStoreError("invalid json: missing field %r" % (missing[0],))
        content_type = document.get("content_type")
        # Validate every body value before touching the conditional headers:
        # request/body errors keep precedence over precondition errors.
        key, size, digest = document["key"], document["size"], document["sha256"]
        ContentAddressedStore._check_key(key)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ObjectStoreError("invalid size: %r" % (size,))
        ContentAddressedStore._require_sha(digest)
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError("content_type must be a string or None")
        expected = self._precondition_headers()
        session_id = self.server.store.begin_upload(
            key, size, digest, content_type=content_type,
            expected_sha256=expected, tenant=tenant)
        return self._send(201, _json_bytes({"session": session_id}))

    def _upload_session(self, method, tail, query, tenant):
        # Transfer-Encoding is rejected before method/session validation.
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return self._error(400, "transfer encoding not supported")
        session_id, sep, sub = tail.partition("/")
        if not session_id:
            return self._error(404, "not found")
        if not sep:
            return self._upload_session_item(method, session_id, query, tenant)
        if sub == "complete":
            if method != "POST":
                return self._error(405, "method not allowed")
            return self._complete_upload(session_id, tenant)
        return self._error(404, "not found")

    def _upload_session_item(self, method, session_id, query, tenant):
        store = self.server.store
        if method == "GET":
            return self._send(200, _json_bytes(
                store.upload_status(session_id, tenant=tenant)))
        if method == "DELETE":
            store.abort_upload(session_id, tenant=tenant)
            return self._send(204, b"")
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        offset = self._offset_param(query)
        confirmed = store.append_upload(session_id, offset, body, tenant=tenant)
        return self._send(200, _json_bytes({"offset": confirmed}))

    def _complete_upload(self, session_id, tenant):
        # No body is required, but framing headers still have to be sane and
        # any declared bytes are drained so the connection can be reused.
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return self._error(400, "transfer encoding not supported")
        raw = self.headers.get("Content-Length")
        if raw is not None:
            if _DECIMAL_RE.match(raw) is None:
                self.close_connection = True
                return self._error(400, "invalid content length")
            length = int(raw)
            if length:
                drained = self.rfile.read(length)
                if len(drained) != length:
                    self.close_connection = True
                    return self._error(400, "truncated request body")
        entry = self.server.store.complete_upload(session_id, tenant=tenant)
        return self._send(200, _json_bytes(entry))

    def _list(self, query, tenant):
        params = parse_qs(query, keep_blank_values=True)
        raw_limit = params.get("limit", [str(DEFAULT_LIMIT)])[0]
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            raise ObjectStoreError("invalid limit: %r" % (raw_limit,))
        result = self.server.store.list_objects(
            prefix=params.get("prefix", [""])[0],
            after=params.get("after", [None])[0] or None,
            limit=limit,
            tenant=tenant,
        )
        return self._send(200, _json_bytes(result))

    # -- versioning and retention ---------------------------------------
    def _list_versions(self, key, query, tenant):
        params = parse_qs(query, keep_blank_values=True)
        raw_limit = params.get("limit", [str(DEFAULT_LIMIT)])[0]
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            raise ObjectStoreError("invalid limit: %r" % (raw_limit,))
        result = self.server.store.list_versions(
            key, after=params.get("after", [None])[0] or None,
            limit=limit, tenant=tenant)
        return self._send(200, _json_bytes(result))

    def _version_item(self, method, key, version_id, tenant, query=""):
        store = self.server.store
        if method == "GET":
            expires, signature = self._access_params(query)
            data, entry = store.get_version(key, version_id, tenant=tenant,
                                            expires=expires, signature=signature)
            return self._send(200, data,
                              entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              self._etag_headers(entry))
        if method == "HEAD":
            expires, signature = self._access_params(query)
            entry = store.head_version(key, version_id, tenant=tenant,
                                       expires=expires, signature=signature)
            return self._send(200, b"",
                              entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              self._etag_headers(entry), head_only=True,
                              length=entry["size"])
        if method == "DELETE":
            store.delete_version(key, version_id, tenant=tenant)
            return self._send(204, b"")
        return self._error(405, "method not allowed")

    def _versioning(self, method, tenant):
        store = self.server.store
        if method == "GET":
            return self._send(200, _json_bytes(
                {"enabled": store.get_versioning(tenant=tenant)}))
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        document = self._json_object(body)
        enabled = document.get("enabled")
        if not isinstance(enabled, bool):
            raise ObjectStoreError("invalid versioning")
        store.set_versioning(enabled, tenant=tenant)
        return self._send(200, _json_bytes({"enabled": enabled}))

    def _retention(self, method, tenant):
        store = self.server.store
        if method == "GET":
            policy = store.get_retention(tenant=tenant) or {}
            return self._send(200, _json_bytes({
                "max_versions": policy.get("max_versions"),
                "max_age_seconds": policy.get("max_age_seconds"),
            }))
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        document = self._json_object(body)
        max_versions = document.get("max_versions")
        max_age_seconds = document.get("max_age_seconds")
        store.set_retention(max_versions=max_versions,
                            max_age_seconds=max_age_seconds, tenant=tenant)
        return self._send(200, _json_bytes({
            "max_versions": max_versions,
            "max_age_seconds": max_age_seconds,
        }))

    def _purge_retention(self, tenant):
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return self._error(400, "transfer encoding not supported")
        dry_run = True
        if self.headers.get("Content-Length") is not None:
            body, error = self._read_length_body()
            if error is not None:
                return self._error(*error)
            document = self._json_object(body)
            dry_run = document.get("dry_run", True)
        result = self.server.store.purge_retention(dry_run=dry_run, tenant=tenant)
        return self._send(200, _json_bytes(result))

    def _quota(self, method, tenant):
        store = self.server.store
        if method == "GET":
            policy = store.get_quota(tenant=tenant) or {}
            return self._send(200, _json_bytes({
                "max_bytes": policy.get("max_bytes"),
                "max_objects": policy.get("max_objects"),
            }))
        if method == "PUT":
            body, error = self._read_length_body()
            if error is not None:
                return self._error(*error)
            document = self._json_object(body)
            if set(document) - {"max_bytes", "max_objects"}:
                raise ObjectStoreError("invalid quota")
            store.set_quota(max_bytes=document.get("max_bytes"),
                            max_objects=document.get("max_objects"),
                            tenant=tenant)
            return self._send(200, _json_bytes({
                "max_bytes": document.get("max_bytes"),
                "max_objects": document.get("max_objects"),
            }))
        if method == "DELETE":
            store.clear_quota(tenant=tenant)
            return self._send(200, _json_bytes({
                "max_bytes": None, "max_objects": None}))
        return self._error(405, "method not allowed")

    def _lifecycle(self, method, tenant):
        store = self.server.store
        if method == "GET":
            return self._send(200, _json_bytes(store.get_lifecycle(tenant=tenant)))
        if method == "DELETE":
            store.clear_lifecycle(tenant=tenant)
            return self._send(200, _json_bytes(None))
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        document = self._json_object(body)
        unknown = set(document) - {"prefix", "max_age_seconds"}
        if unknown:
            raise ObjectStoreError(
                "invalid json: unknown field %r" % (sorted(unknown)[0],))
        store.set_lifecycle(
            document.get("max_age_seconds"),
            prefix=document.get("prefix", ""), tenant=tenant)
        return self._send(200, _json_bytes({
            "prefix": document.get("prefix", ""),
            "max_age_seconds": document.get("max_age_seconds"),
        }))

    def _run_lifecycle(self, tenant):
        # Framing follows the retention purge endpoint: no body is required,
        # but any declared body must be a length-framed JSON object holding at
        # most a boolean dry_run field.
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return self._error(400, "transfer encoding not supported")
        dry_run = True
        if self.headers.get("Content-Length") is not None:
            body, error = self._read_length_body()
            if error is not None:
                return self._error(*error)
            if body:
                document = self._json_object(body)
                unknown = set(document) - {"dry_run"}
                if unknown:
                    raise ObjectStoreError(
                        "invalid json: unknown field %r" % (sorted(unknown)[0],))
                dry_run = document.get("dry_run", True)
        result = self.server.store.run_lifecycle(dry_run=dry_run, tenant=tenant)
        return self._send(200, _json_bytes(result))

    def _access_policy(self, method, tenant):
        store = self.server.store
        if method == "GET":
            return self._send(200, _json_bytes(
                store.get_access_policy(tenant=tenant)))
        if method == "DELETE":
            store.clear_access_policy(tenant=tenant)
            return self._send(200, _json_bytes({"mode": "public"}))
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        # Both undecodable JSON and an invalid policy are reported as the
        # same "invalid access policy" error on this endpoint.
        try:
            document = self._json_object(body)
        except ObjectStoreError:
            raise ObjectStoreError("invalid access policy")
        if set(document) - {"mode", "secret"}:
            raise ObjectStoreError("invalid access policy")
        store.set_access_policy(document.get("mode"),
                                secret=document.get("secret"), tenant=tenant)
        return self._send(200, _json_bytes(
            store.get_access_policy(tenant=tenant)))

    def _blob(self, sha256):
        if _SHA256_RE.match(sha256) is None:
            raise ObjectStoreError("invalid sha256: %r" % (sha256,))
        return self._send(200, self.server.store.blob(sha256), _DEFAULT_CONTENT_TYPE,
                          [("X-Content-Sha256", sha256)])


class ObjectStoreHTTPServer(ThreadingHTTPServer):
    """Threading HTTP server carrying a reference to the object store."""

    daemon_threads = True
    allow_reuse_address = True


def create_server(store, host="127.0.0.1", port=8080):
    """Bind a new HTTP server for *store*; port 0 picks an ephemeral port."""
    if not isinstance(store, ContentAddressedStore):
        raise ObjectStoreError("store must be a ContentAddressedStore")
    httpd = ObjectStoreHTTPServer((host, port), ObjectStoreHandler)
    httpd.store = store
    return httpd

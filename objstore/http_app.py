"""Standard-library HTTP front end for :class:`ContentAddressedStore`.

Every failure is a single-line JSON body ``{"error": "..."}`` with status 400,
404, 405, 409, 411 or 500 (500 for a corrupted blob/upload or a backing I/O
error).
"""

from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .store import DEFAULT_LIMIT, ContentAddressedStore, ObjectStoreError

__all__ = ["ObjectStoreHandler", "ObjectStoreHTTPServer", "create_server"]

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_OBJECTS_PATH = "/v1/objects"
_BLOBS_PATH = "/v1/blobs"
_UPLOADS_PATH = "/v1/uploads"
_COMPLETE_SUFFIX = "/complete"
_DEFAULT_CONTENT_TYPE = "application/octet-stream"
_BAD_REQUEST_PREFIXES = (
    "invalid sha256", "invalid limit", "invalid key", "invalid prefix",
    "invalid after", "invalid size", "invalid offset",
    "payload", "content_type",
)
_CONFLICT_PREFIXES = (
    "append gap", "append overlaps", "append conflicts", "append exceeds",
    "empty chunk", "upload incomplete", "upload hash mismatch",
    "upload session already completed",
)
_SERVER_ERROR_PREFIXES = (
    "corrupted blob", "blob io error", "upload io error",
    "corrupted upload session",
)
_OFFSET_RE = re.compile(r"\A[0-9]+\Z")

# Sentinels for the framed-body readers; real bodies are always ``bytes``.
_TE_UNSUPPORTED = object()
_LENGTH_REQUIRED = object()
_BODY_ENDED_EARLY = object()


def _json_bytes(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _status_for(message):
    for prefix in _SERVER_ERROR_PREFIXES:
        if message.startswith(prefix):
            return 500
    for prefix in _BAD_REQUEST_PREFIXES:
        if message.startswith(prefix):
            return 400
    for prefix in _CONFLICT_PREFIXES:
        if message.startswith(prefix):
            return 409
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

    def _error(self, status, message):
        self._send(status, _json_bytes({"error": message}))

    def _read_body(self):
        raw = self.headers.get("Content-Length")
        try:
            length = int(raw)
        except (TypeError, ValueError):
            return None
        if length < 0:
            return None
        return self.rfile.read(length) if length else b""

    def _read_required_body(self):
        """Read a body that must use Content-Length framing.

        Returns the received bytes or one of the framing sentinels: a
        Transfer-Encoding header wins over everything else, a missing,
        non-integer or negative Content-Length means length required, and a
        connection that ends before the declared length means a bad request.
        """
        if self.headers.get("Transfer-Encoding") is not None:
            return _TE_UNSUPPORTED
        raw = self.headers.get("Content-Length")
        try:
            length = int(raw)
        except (TypeError, ValueError):
            return _LENGTH_REQUIRED
        if length < 0:
            return _LENGTH_REQUIRED
        data = self.rfile.read(length) if length else b""
        if len(data) != length:
            return _BODY_ENDED_EARLY
        return data

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
        path = urlparse(self.path).path
        try:
            if path == "/healthz":
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._send(200, _json_bytes({"ok": True}))
            if path == _OBJECTS_PATH:
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._list(urlparse(self.path).query)
            if path.startswith(_OBJECTS_PATH + "/"):
                return self._object(method, unquote(path[len(_OBJECTS_PATH) + 1:]))
            if path.startswith(_BLOBS_PATH + "/"):
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._blob(unquote(path[len(_BLOBS_PATH) + 1:]))
            if path == _UPLOADS_PATH:
                return self._uploads_collection(method)
            if path.startswith(_UPLOADS_PATH + "/"):
                remainder = unquote(path[len(_UPLOADS_PATH) + 1:])
                if not remainder:
                    return self._error(404, "not found")
                if remainder.endswith(_COMPLETE_SUFFIX):
                    session_id = remainder[:-len(_COMPLETE_SUFFIX)]
                    if session_id:
                        return self._upload_complete(method, session_id)
                return self._upload_session(method, remainder,
                                            urlparse(self.path).query)
            return self._error(404, "not found")
        except ObjectStoreError as exc:
            return self._error(_status_for(str(exc)), str(exc))
        except (BrokenPipeError, ConnectionResetError):
            return None

    def _object(self, method, key):
        if not key:
            return self._error(400, "invalid key")
        store = self.server.store
        if method == "PUT":
            body = self._read_body()
            if body is None:
                return self._error(411, "length required")
            try:
                existing = store.head(key)
            except ObjectStoreError:
                existing = None
            entry = store.put(key, body, content_type=self.headers.get("Content-Type"))
            repeated = existing is not None and existing["sha256"] == entry["sha256"]
            body_out = {"key": key, "sha256": entry["sha256"], "size": entry["size"]}
            return self._send(200 if repeated else 201, _json_bytes(body_out))
        if method == "GET":
            data, entry = store.get(key)
            return self._send(200, data, entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              [("X-Content-Sha256", entry["sha256"])])
        if method == "HEAD":
            entry = store.head(key)
            return self._send(200, b"", entry["content_type"] or _DEFAULT_CONTENT_TYPE,
                              [("X-Content-Sha256", entry["sha256"])], head_only=True,
                              length=entry["size"])
        if method == "DELETE":
            store.delete(key)
            return self._send(204, b"")
        return self._error(405, "method not allowed")

    def _list(self, query):
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
        )
        return self._send(200, _json_bytes(result))

    def _blob(self, sha256):
        if _SHA256_RE.match(sha256) is None:
            raise ObjectStoreError("invalid sha256: %r" % (sha256,))
        return self._send(200, self.server.store.blob(sha256), _DEFAULT_CONTENT_TYPE,
                          [("X-Content-Sha256", sha256)])

    # -- resumable uploads ---------------------------------------------
    def _uploads_collection(self, method):
        if method != "POST":
            return self._error(405, "method not allowed")
        body = self._read_required_body()
        if body is _TE_UNSUPPORTED:
            return self._error(400, "transfer-encoding not supported")
        if body is _LENGTH_REQUIRED:
            return self._error(411, "length required")
        if body is _BODY_ENDED_EARLY:
            return self._error(400, "request body ended early")
        try:
            document = json.loads(body.decode("utf-8"))
        except UnicodeDecodeError:
            return self._error(400, "invalid utf-8 body")
        except ValueError:
            return self._error(400, "invalid json body")
        if not isinstance(document, dict):
            return self._error(400, "invalid upload declaration")
        try:
            key = document["key"]
            size = document["size"]
            sha256 = document["sha256"]
        except KeyError:
            return self._error(400, "invalid upload declaration")
        session_id = self.server.store.begin_upload(
            key, size, sha256, document.get("content_type"))
        return self._send(201, _json_bytes({"session": session_id}))

    def _upload_session(self, method, session_id, query):
        if method == "GET":
            status = self.server.store.upload_status(session_id)
            return self._send(200, _json_bytes(status))
        if method == "PUT":
            body = self._read_required_body()
            if body is _TE_UNSUPPORTED:
                return self._error(400, "transfer-encoding not supported")
            if body is _LENGTH_REQUIRED:
                return self._error(411, "length required")
            if body is _BODY_ENDED_EARLY:
                return self._error(400, "request body ended early")
            offset = self._parse_offset(query)
            end = self.server.store.append_upload(session_id, offset, body)
            return self._send(200, _json_bytes({"offset": end}))
        if method == "DELETE":
            self.server.store.abort_upload(session_id)
            return self._send(204, b"")
        return self._error(405, "method not allowed")

    def _upload_complete(self, method, session_id):
        if method != "POST":
            return self._error(405, "method not allowed")
        entry = self.server.store.complete_upload(session_id)
        return self._send(200, _json_bytes(entry))

    @staticmethod
    def _parse_offset(query):
        params = parse_qs(query, keep_blank_values=True)
        values = params.get("offset")
        if not values or len(values) != 1 or _OFFSET_RE.match(values[0]) is None:
            raise ObjectStoreError("invalid offset: %r" % (query,))
        return int(values[0])


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

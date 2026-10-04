"""Standard-library HTTP front end for :class:`ContentAddressedStore`.

Every failure is a single-line JSON body ``{"error": "..."}`` with status 400,
404, 405, 409, 411 or 500 (500 for a corrupted blob or a blob/upload I/O
error).
"""

from __future__ import annotations

import json
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .store import (
    DEFAULT_LIMIT,
    ContentAddressedStore,
    ObjectStoreError,
    SessionConflict,
)

__all__ = ["ObjectStoreHandler", "ObjectStoreHTTPServer", "create_server"]

_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_OBJECTS_PATH = "/v1/objects"
_BLOBS_PATH = "/v1/blobs"
_UPLOADS_PATH = "/v1/uploads"
_DEFAULT_CONTENT_TYPE = "application/octet-stream"
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")
_BAD_REQUEST_PREFIXES = (
    "invalid sha256", "invalid limit", "invalid key", "invalid prefix",
    "invalid after", "invalid size", "invalid offset", "invalid json",
    "payload", "content_type",
)
_INTERNAL_ERRORS = ("corrupted blob", "blob io error", "upload io error")


def _json_bytes(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _status_for(message):
    if message in _INTERNAL_ERRORS:
        return 500
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
                if method != "GET":
                    return self._error(405, "method not allowed")
                return self._list(parsed.query)
            if path.startswith(_OBJECTS_PATH + "/"):
                return self._object(method, unquote(path[len(_OBJECTS_PATH) + 1:]))
            if path == _UPLOADS_PATH:
                if self.headers.get("Transfer-Encoding") is not None:
                    self.close_connection = True
                    return self._error(400, "transfer encoding not supported")
                if method != "POST":
                    return self._error(405, "method not allowed")
                return self._create_upload()
            if path.startswith(_UPLOADS_PATH + "/"):
                return self._upload_session(
                    method, unquote(path[len(_UPLOADS_PATH) + 1:]), parsed.query)
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

    def _create_upload(self):
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
        session_id = self.server.store.begin_upload(
            document["key"], document["size"], document["sha256"],
            content_type=content_type)
        return self._send(201, _json_bytes({"session": session_id}))

    def _upload_session(self, method, tail, query):
        # Transfer-Encoding is rejected before method/session validation.
        if self.headers.get("Transfer-Encoding") is not None:
            self.close_connection = True
            return self._error(400, "transfer encoding not supported")
        session_id, sep, sub = tail.partition("/")
        if not session_id:
            return self._error(404, "not found")
        if not sep:
            return self._upload_session_item(method, session_id, query)
        if sub == "complete":
            if method != "POST":
                return self._error(405, "method not allowed")
            return self._complete_upload(session_id)
        return self._error(404, "not found")

    def _upload_session_item(self, method, session_id, query):
        store = self.server.store
        if method == "GET":
            return self._send(200, _json_bytes(store.upload_status(session_id)))
        if method == "DELETE":
            store.abort_upload(session_id)
            return self._send(204, b"")
        if method != "PUT":
            return self._error(405, "method not allowed")
        body, error = self._read_length_body()
        if error is not None:
            return self._error(*error)
        offset = self._offset_param(query)
        confirmed = store.append_upload(session_id, offset, body)
        return self._send(200, _json_bytes({"offset": confirmed}))

    def _complete_upload(self, session_id):
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
        entry = self.server.store.complete_upload(session_id)
        return self._send(200, _json_bytes(entry))

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

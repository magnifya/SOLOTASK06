"""End-to-end tests for the HTTP resumable-upload session surface."""

import hashlib
import http.client
import json
import os
import shutil
import socket
import tempfile
import threading
import unittest

from objstore.http_app import create_server
from objstore.store import ContentAddressedStore


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPUploadTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-http-up-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            payload = response.read()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, payload
        finally:
            conn.close()

    def raw_request(self, raw):
        """Send *raw* (bytes, full request) over a fresh socket."""
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=15)
        try:
            sock.sendall(raw)
            chunks = []
            while True:
                part = sock.recv(65536)
                if not part:
                    break
                chunks.append(part)
        finally:
            sock.close()
        response = b"".join(chunks)
        status_line = response.split(b"\r\n", 1)[0]
        status = int(status_line.split()[1])
        head, _, body = response.partition(b"\r\n\r\n")
        return status, head, body

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def begin(self, key="k", data=b"hello world", content_type=None):
        declaration = {"key": key, "size": len(data), "sha256": sha(data)}
        if content_type is not None:
            declaration["content_type"] = content_type
        status, _, body = self.request("POST", "/v1/uploads",
                                       json.dumps(declaration).encode("utf-8"))
        self.assertEqual(status, 201, body)
        document = self.document(body)
        self.assertEqual(set(document), {"session"})
        return document["session"]


class TestSessionLifecycle(HTTPUploadTestCase):
    def test_create_returns_only_session_and_changes_nothing(self):
        data = b"hello world"
        status, headers, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "k", "size": len(data), "sha256": sha(data),
                        "content_type": "text/plain"}).encode("utf-8"))
        self.assertEqual(status, 201)
        self.assertEqual(headers["content-type"], "application/json")
        document = self.document(body)
        self.assertEqual(set(document), {"session"})
        self.assertIsInstance(document["session"], str)
        self.assertTrue(document["session"])
        # no object and no blob were created
        self.assertEqual(self.request("GET", "/v1/objects/k")[0], 404)
        self.assertEqual(self.store.blob_digests(), [])

    def test_content_type_defaults_to_null(self):
        session_id = self.begin("plain", b"abc")
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertIsNone(self.document(body)["content_type"])

    def test_get_status_matches_python_state(self):
        data = b"hello world"
        session_id = self.begin("docs/a.txt", data, content_type="text/plain")
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"key": "docs/a.txt", "size": 11, "sha256": sha(data),
                          "content_type": "text/plain", "offset": 0,
                          "completed": False})

    def test_put_chunks_and_complete(self):
        data = b"hello world"
        session_id = self.begin("k", data)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello ")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 6})
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=6" % session_id, b"world")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 11})
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"sha256": sha(data), "size": 11, "content_type": None})
        status, _, body = self.request("GET", "/v1/objects/k")
        self.assertEqual((status, body), (200, data))

    def test_complete_accepts_and_drains_a_declared_body(self):
        session_id = self.begin("k", b"ab")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        for extra_headers in ({}, {"Content-Length": "0"},
                              {"Content-Length": "5"}):
            session = session_id
            status, _, body = self.request(
                "POST", "/v1/uploads/%s/complete" % session,
                body=b"noise" if extra_headers.get("Content-Length") == "5" else b"",
                headers=extra_headers)
            self.assertEqual(status, 200, body)
            self.assertEqual(self.document(body)["sha256"], sha(b"ab"))

    def test_zero_byte_object_completes_directly(self):
        session_id = self.begin("empty", b"")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"sha256": sha(b""), "size": 0, "content_type": None})
        self.assertEqual(self.request("GET", "/v1/objects/empty")[2], b"")

    def test_delete_aborts_with_204_and_then_everything_is_404(self):
        session_id = self.begin("k", b"abc")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"abc")
        status, _, body = self.request("DELETE", "/v1/uploads/%s" % session_id)
        self.assertEqual((status, body), (204, b""))
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)
        self.assertEqual(
            self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"x")[0],
            404)
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s/complete" % session_id)[0], 404)
        self.assertEqual(
            self.request("DELETE", "/v1/uploads/%s" % session_id)[0], 404)

    def test_delete_completed_session_keeps_published_object(self):
        session_id = self.begin("k", b"ab")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        self.request("POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(
            self.request("DELETE", "/v1/uploads/%s" % session_id)[0], 204)
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], b"ab")
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)


class TestResendAndConflictSemantics(HTTPUploadTestCase):
    def setUp(self):
        super().setUp()
        self.session_id = self.begin()
        self.request("PUT", "/v1/uploads/%s?offset=0" % self.session_id, b"hello ")
        self.request("PUT", "/v1/uploads/%s?offset=6" % self.session_id, b"world")

    def status_offset(self):
        body = self.request("GET", "/v1/uploads/%s" % self.session_id)[2]
        return self.document(body)["offset"]

    def assert_unchanged(self, status, body):
        self.assertEqual(status, 409, body)
        self.assertEqual(set(self.document(body)), {"error"})
        self.assertEqual(self.status_offset(), 11)

    def test_identical_resend_does_not_grow_progress(self):
        for offset, chunk in [(0, b"hello "), (3, b"lo wo"), (6, b"world"),
                              (0, b"hello world")]:
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s?offset=%d" % (self.session_id, offset), chunk)
            self.assertEqual(status, 200, body)
            self.assertEqual(self.document(body), {"offset": 11})
        self.assertEqual(self.status_offset(), 11)

    def test_empty_chunk_only_succeeds_at_the_current_end(self):
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=11" % self.session_id, b"")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 11})
        for offset in (0, 3, 6):
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s?offset=%d" % (self.session_id, offset), b"")
            self.assert_unchanged(status, body)

    def test_gap_is_409(self):
        session = self.begin("gap", b"abc")
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=1" % session, b"bc")
        self.assertEqual(status, 409)
        self.assertEqual(self.document(
            self.request("GET", "/v1/uploads/%s" % session)[2])["offset"], 0)

    def test_overlap_past_end_is_409(self):
        # starts inside the received range but runs past the current end
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=9" % self.session_id, b"ldXX")
        self.assert_unchanged(status, body)

    def test_exceeding_declared_size_is_409(self):
        session = self.begin("big", b"abc")
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session, b"abcd")
        self.assertEqual(status, 409)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session, b"ab")
        self.assertEqual(status, 200)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=2" % session, b"cd")
        self.assertEqual(status, 409)
        self.assertEqual(self.document(
            self.request("GET", "/v1/uploads/%s" % session)[2])["offset"], 2)

    def test_conflicting_resend_is_409(self):
        for offset, chunk in [(0, b"HELLO"), (4, b"ox")]:
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s?offset=%d" % (self.session_id, offset), chunk)
            self.assert_unchanged(status, body)

    def test_append_after_complete_is_409(self):
        self.request("POST", "/v1/uploads/%s/complete" % self.session_id)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=11" % self.session_id, b"more")
        self.assertEqual(status, 409)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % self.session_id, b"hello ")
        self.assertEqual(status, 409)

    def test_complete_too_short_is_409_and_publishes_nothing(self):
        session = self.begin("short", b"abcd")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session, b"ab")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session)
        self.assertEqual(status, 409)
        self.assertEqual(self.request("GET", "/v1/objects/short")[0], 404)
        self.assertEqual(self.document(
            self.request("GET", "/v1/uploads/%s" % session)[2])["offset"], 2)
        # the session is still usable
        self.assertEqual(self.request(
            "PUT", "/v1/uploads/%s?offset=2" % session, b"cd")[0], 200)
        self.assertEqual(self.request(
            "POST", "/v1/uploads/%s/complete" % session)[0], 200)

    def test_complete_with_wrong_hash_is_409(self):
        declaration = json.dumps({"key": "wrong", "size": 3,
                                  "sha256": sha(b"xyz")}).encode("utf-8")
        status, _, body = self.request("POST", "/v1/uploads", declaration)
        self.assertEqual(status, 201)
        session = self.document(body)["session"]
        self.request("PUT", "/v1/uploads/%s?offset=0" % session, b"abc")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session)
        self.assertEqual(status, 409)
        self.assertEqual(self.request("GET", "/v1/objects/wrong")[0], 404)
        self.assertEqual(self.document(
            self.request("GET", "/v1/uploads/%s" % session)[2])["offset"], 3)

    def test_repeated_complete_returns_first_result(self):
        session_id = self.begin("k2", b"ab", content_type="text/plain")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        first = self.request("POST", "/v1/uploads/%s/complete" % session_id)[2]
        # later overwrite and delete of the key are not republished
        self.request("PUT", "/v1/objects/k2", b"newer")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(body, first)
        self.assertEqual(self.request("GET", "/v1/objects/k2")[2], b"newer")
        self.request("DELETE", "/v1/objects/k2")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual((status, body), (200, first))
        self.assertEqual(self.request("GET", "/v1/objects/k2")[0], 404)

    def test_completed_get_reports_completed_state(self):
        self.request("POST", "/v1/uploads/%s/complete" % self.session_id)
        status, _, body = self.request("GET", "/v1/uploads/%s" % self.session_id)
        self.assertEqual(status, 200)
        document = self.document(body)
        self.assertTrue(document["completed"])
        self.assertEqual(document["offset"], 11)

    def test_same_key_sessions_publish_in_completion_order(self):
        first = self.begin("ordered", b"from-first")
        second = self.begin("ordered", b"from-second!")
        self.request("PUT", "/v1/uploads/%s?offset=0" % first, b"from-first")
        self.request("PUT", "/v1/uploads/%s?offset=0" % second, b"from-second!")
        self.request("POST", "/v1/uploads/%s/complete" % first)
        self.assertEqual(self.request("GET", "/v1/objects/ordered")[2], b"from-first")
        self.request("POST", "/v1/uploads/%s/complete" % second)
        self.assertEqual(self.request("GET", "/v1/objects/ordered")[2],
                         b"from-second!")


class TestFramingAndBadInput(HTTPUploadTestCase):
    def test_create_requires_content_length(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("POST", "/v1/uploads")
            conn.endheaders()
            response = conn.getresponse()
            status, body = response.status, response.read()
        finally:
            conn.close()
        self.assertEqual(status, 411)
        self.assertEqual(self.document(body), {"error": "length required"})

    def test_put_requires_content_length(self):
        session_id = self.begin("k", b"a")
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("PUT", "/v1/uploads/%s?offset=0" % session_id)
            conn.endheaders()
            response = conn.getresponse()
            status, body = response.status, response.read()
        finally:
            conn.close()
        self.assertEqual(status, 411)
        self.assertEqual(self.document(body), {"error": "length required"})

    def test_non_integer_or_negative_content_length_is_411(self):
        raw_headers = (
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            b"Content-Length: %s\r\n\r\n")
        for value in (b"abc", b"-1", b"+3", b"1_0", b" 3 "):
            status, _, body = self.raw_request(raw_headers % value)
            self.assertEqual(status, 411, value)
            self.assertEqual(json.loads(body), {"error": "length required"})

    def test_transfer_encoding_takes_priority_400(self):
        targets = [
            b"POST /v1/uploads HTTP/1.1",
            b"GET /v1/uploads HTTP/1.1",
            b"POST /v1/uploads/%s/complete HTTP/1.1" % (b"a" * 32),
            b"GET /v1/uploads/%s HTTP/1.1" % (b"a" * 32),
            b"DELETE /v1/uploads/%s HTTP/1.1" % (b"a" * 32),
        ]
        for target in targets:
            raw = (target + b"\r\nHost: x\r\nConnection: close\r\n"
                   b"Transfer-Encoding: chunked\r\n\r\n"
                   b"0\r\n\r\n")
            status, _, body = self.raw_request(raw)
            self.assertEqual(status, 400, target)
            self.assertEqual(json.loads(body),
                             {"error": "transfer encoding not supported"})

    def test_premature_body_end_is_400(self):
        raw = (b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\n"
               b"Content-Length: 10\r\n\r\nabc")
        # signal EOF on the request body so the server's read returns early
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=15)
        try:
            sock.sendall(raw)
            sock.shutdown(socket.SHUT_WR)
            response = b""
            while True:
                part = sock.recv(65536)
                if not part:
                    break
                response += part
        finally:
            sock.close()
        self.assertTrue(response.startswith(b"HTTP/1.1 400"))
        self.assertIn(b'"truncated request body"', response)

    def test_bad_utf8_json_and_fields_are_400(self):
        cases = [
            b"\xff\xfe not utf8",
            b"{not json",
            b"[1,2,3]",
            b'{"size":1,"sha256":"' + b"a" * 64 + b'"}',           # missing key
            b'{"key":"k","sha256":"' + b"a" * 64 + b'"}',          # missing size
            b'{"key":"k","size":1}',                               # missing sha256
            b'{"key":"k","size":1,"sha256":"' + b"a" * 64
            + b'","extra":1}',                                     # unknown field
            b'{"key":"a/../b","size":1,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":-1,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":true,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":"3","sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":1,"sha256":"ZZ"}',
            b'{"key":"k","size":1,"sha256":"' + b"A" * 64 + b'"}',
            b'{"key":7,"size":1,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":1,"sha256":"' + b"a" * 64
            + b'","content_type":7}',
        ]
        for case in cases:
            status, _, body = self.request(
                "POST", "/v1/uploads", case, {"Content-Length": str(len(case))})
            self.assertEqual(status, 400, case)
            self.assertEqual(set(self.document(body)), {"error"})
        # no sessions were persisted by the rejected requests
        self.assertEqual(os.listdir(self.root), ["blobs"])

    def test_bad_offset_query_is_400(self):
        session_id = self.begin("k", b"abc")
        for query in ["", "?offset", "?offset=", "?offset=abc", "?offset=-1",
                      "?offset=1.5", "?offset=true",
                      "?offset=0&offset=1", "?offset=%2B1"]:
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s%s" % (session_id, query), b"")
            self.assertEqual(status, 400, query)
            self.assertEqual(set(self.document(body)), {"error"})
        # progress untouched
        self.assertEqual(self.document(
            self.request("GET", "/v1/uploads/%s" % session_id)[2])["offset"], 0)


class TestRoutingErrors(HTTPUploadTestCase):
    def test_unknown_and_cancelled_sessions_are_404(self):
        for method, path, body in [
            ("GET", "/v1/uploads/%s" % ("f" * 32), None),
            ("PUT", "/v1/uploads/%s?offset=0" % ("f" * 32), b"x"),
            ("DELETE", "/v1/uploads/%s" % ("f" * 32), None),
            ("POST", "/v1/uploads/%s/complete" % ("f" * 32), b""),
        ]:
            status, _, response = self.request(method, path, body)
            self.assertEqual(status, 404, (method, path, response))
            self.assertEqual(set(self.document(response)), {"error"})
        self.assertEqual(self.request("GET", "/v1/uploads/")[0], 404)
        session_id = self.begin("gone", b"x")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"x")
        self.request("DELETE", "/v1/uploads/%s" % session_id)
        self.assertEqual(
            self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)

    def test_known_paths_with_unopened_methods_are_405(self):
        session_id = self.begin("m", b"a")
        for method, path, body in [
            ("GET", "/v1/uploads", None),
            ("PUT", "/v1/uploads", b""),
            ("DELETE", "/v1/uploads", None),
            ("POST", "/v1/uploads/%s" % session_id, b""),
            ("HEAD", "/v1/uploads/%s" % session_id, None),
            ("GET", "/v1/uploads/%s/complete" % session_id, None),
            ("PUT", "/v1/uploads/%s/complete" % session_id, b""),
            ("DELETE", "/v1/uploads/%s/complete" % session_id, None),
        ]:
            status, _, response = self.request(method, path, body)
            self.assertEqual(status, 405, (method, path))
            if method != "HEAD":
                self.assertEqual(set(self.document(response)), {"error"})

    def test_unknown_sub_resource_is_404(self):
        session_id = self.begin("sub", b"a")
        status, _, body = self.request(
            "GET", "/v1/uploads/%s/whatever" % session_id)
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found"})


class TestIOErrors(HTTPUploadTestCase):
    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "root bypasses directory permissions")
    def test_upload_io_failure_is_500(self):
        session_id = self.begin("io", b"abcdef")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"abc")
        os.chmod(os.path.join(self.root, "uploads"), 0)
        try:
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s?offset=3" % session_id, b"def")
        finally:
            os.chmod(os.path.join(self.root, "uploads"), 0o755)
        self.assertEqual(status, 500)
        self.assertEqual(self.document(body), {"error": "upload io error"})


class TestPersistenceAndCrossEntry(HTTPUploadTestCase):
    def reload_store(self):
        new_store = ContentAddressedStore(self.root)
        self.server.store = new_store
        return new_store

    def test_session_continues_after_reopen(self):
        data = b"hello world"
        session_id = self.begin("k", data)
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello ")
        self.reload_store()
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body)["offset"], 6)
        self.assertEqual(self.request(
            "PUT", "/v1/uploads/%s?offset=6" % session_id, b"world")[0], 200)
        self.assertEqual(self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)[0], 200)
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], data)

    def test_python_created_session_is_driven_over_http(self):
        data = b"python-begin"
        session_id = self.store.begin_upload("py", len(data), sha(data),
                                             content_type="text/plain")
        self.reload_store()
        self.assertEqual(self.request(
            "GET", "/v1/uploads/%s" % session_id)[0], 200)
        self.assertEqual(self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, data)[0], 200)
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body)["sha256"], sha(data))

    def test_http_created_session_is_driven_from_python(self):
        data = b"http-begin-bytes"
        session_id = self.begin("hp", data)
        self.assertEqual(self.store.append_upload(session_id, 0, data), len(data))
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(data))
        status, _, body = self.request("GET", "/v1/objects/hp")
        self.assertEqual((status, body), (200, data))

    def test_completed_session_record_survives_reopen(self):
        session_id = self.begin("persist", b"ab")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        first = self.request("POST", "/v1/uploads/%s/complete" % session_id)[2]
        reopened = self.reload_store()
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertTrue(self.document(body)["completed"])
        self.assertEqual(reopened.complete_upload(session_id),
                         self.document(first))


if __name__ == "__main__":
    unittest.main()

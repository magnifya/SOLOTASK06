"""End-to-end tests for the HTTP resumable-upload session surface."""

import hashlib
import http.client
import json
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
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-up-")
        cls.store = ContentAddressedStore(cls.root)
        cls.server = create_server(cls.store, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      kwargs={"poll_interval": 0.05}, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=10)
        shutil.rmtree(cls.root, ignore_errors=True)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            payload = response.read()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, payload
        finally:
            conn.close()

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def raw_request(self, raw):
        """Send one raw HTTP/1.1 request and return ``(status, body)``."""
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as sock:
            sock.sendall(raw)
            sock.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
        data = b"".join(chunks)
        head, _, body = data.partition(b"\r\n\r\n")
        status = int(head.split(b"\r\n", 1)[0].split()[1])
        return status, body

    def begin(self, key, data, content_type=None):
        declaration = {"key": key, "size": len(data), "sha256": sha(data)}
        if content_type is not None:
            declaration["content_type"] = content_type
        status, _, body = self.request("POST", "/v1/uploads",
                                       json.dumps(declaration).encode("utf-8"))
        self.assertEqual(status, 201)
        document = self.document(body)
        self.assertEqual(sorted(document), ["session"])
        session_id = document["session"]
        self.assertIsInstance(session_id, str)
        self.assertTrue(session_id)
        return session_id


class TestUploadSessionLifecycle(HTTPUploadTestCase):
    def test_create_status_append_complete_fetch(self):
        data = b"hello world"
        session_id = self.begin("up/life.txt", data, content_type="text/plain")
        status, headers, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(self.document(body),
                         {"key": "up/life.txt", "size": 11, "sha256": sha(data),
                          "content_type": "text/plain", "offset": 0,
                          "completed": False})

        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello ")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 6})
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=6" % session_id, b"world")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 11})
        self.assertEqual(
            self.document(self.request("GET", "/v1/uploads/%s" % session_id)[2])
            ["offset"], 11)

        status, _, body = self.request("POST",
                                       "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"sha256": sha(data), "size": 11,
                          "content_type": "text/plain"})
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        document = self.document(body)
        self.assertEqual(document["completed"], True)
        self.assertEqual(document["offset"], 11)
        status, headers, body = self.request("GET", "/v1/objects/up/life.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, data)
        self.assertEqual(headers["x-content-sha256"], sha(data))

    def test_create_does_not_touch_objects(self):
        self.request("POST", "/v1/uploads",
                     json.dumps({"key": "up/ghost.bin", "size": 3,
                                 "sha256": sha(b"abc")}).encode("utf-8"))
        status, _, body = self.request("GET", "/v1/objects/up/ghost.bin")
        self.assertEqual(status, 404)
        self.assertIn("error", self.document(body))

    def test_content_type_defaults_to_null(self):
        session_id = self.begin("up/null-ct.bin", b"abc")
        document = self.document(
            self.request("GET", "/v1/uploads/%s" % session_id)[2])
        self.assertIsNone(document["content_type"])
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, b"abc")
        self.assertEqual(status, 200)
        status, _, body = self.request("POST",
                                       "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertIsNone(self.document(body)["content_type"])
        self.assertIsNone(self.store.head("up/null-ct.bin")["content_type"])

    def test_zero_byte_object_completes_directly(self):
        session_id = self.begin("up/empty", b"")
        status, _, body = self.request("POST",
                                       "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"sha256": sha(b""), "size": 0, "content_type": None})
        self.assertEqual(self.request("GET", "/v1/objects/up/empty")[2], b"")

    def test_extra_declaration_fields_are_ignored(self):
        body = json.dumps({"key": "up/extra", "size": 1, "sha256": sha(b"x"),
                           "unexpected": [1, 2]}).encode("utf-8")
        status, _, response = self.request("POST", "/v1/uploads", body)
        self.assertEqual(status, 201)
        self.assertIn("session", self.document(response))

    def test_complete_then_delete_session_keeps_object(self):
        session_id = self.begin("up/kept", b"ab")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        self.request("POST", "/v1/uploads/%s/complete" % session_id)
        status, _, body = self.request("DELETE", "/v1/uploads/%s" % session_id)
        self.assertEqual((status, body), (204, b""))
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)
        self.assertEqual(self.request("GET", "/v1/objects/up/kept")[2], b"ab")


class TestUploadAppendConflicts(HTTPUploadTestCase):
    def setUp(self):
        self.session_id = self.begin("up/conflict", b"hello world")

    def put(self, offset, data):
        return self.request(
            "PUT", "/v1/uploads/%s?offset=%d" % (self.session_id, offset), data)

    def status_offset(self):
        return self.document(
            self.request("GET", "/v1/uploads/%s" % self.session_id)[2])["offset"]

    def test_identical_resend_inside_range_does_not_grow(self):
        self.assertEqual(self.put(0, b"hello ")[0], 200)
        self.assertEqual(self.put(6, b"world")[0], 200)
        for offset, chunk in [(0, b"hello"), (3, b"lo wo"), (6, b"world")]:
            status, _, body = self.put(offset, chunk)
            self.assertEqual(status, 200)
            self.assertEqual(self.document(body), {"offset": 11})
        self.assertEqual(self.status_offset(), 11)

    def test_conflicting_resend_is_409(self):
        self.put(0, b"hello ")
        status, _, body = self.put(0, b"HELL")
        self.assertEqual(status, 409)
        self.assertIn("error", self.document(body))
        status, _, _ = self.put(4, b"ox")
        self.assertEqual(status, 409)
        self.assertEqual(self.status_offset(), 6)

    def test_gap_is_409(self):
        status, _, _ = self.put(2, b"llo")
        self.assertEqual(status, 409)
        self.assertEqual(self.status_offset(), 0)

    def test_overlap_past_end_is_409(self):
        self.put(0, b"hello ")
        status, _, _ = self.put(4, b"o wor")
        self.assertEqual(status, 409)
        self.assertEqual(self.status_offset(), 6)

    def test_exceeding_declared_size_is_409(self):
        session_id = self.begin("up/too-big", b"abc")
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, b"abcd")
        self.assertEqual(status, 409)
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=2" % session_id, b"cd")
        self.assertEqual(status, 409)
        self.assertEqual(
            self.document(self.request("GET", "/v1/uploads/%s" % session_id)[2])
            ["offset"], 2)

    def test_empty_chunk_only_at_current_end(self):
        self.assertEqual(self.put(0, b"")[0], 200)
        self.put(0, b"hello ")
        self.assertEqual(self.put(0, b"")[0], 409)
        self.assertEqual(self.put(3, b"")[0], 409)
        status, _, body = self.put(6, b"")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 6})

    def test_append_after_complete_is_409(self):
        self.put(0, b"hello world")
        self.request("POST", "/v1/uploads/%s/complete" % self.session_id)
        self.assertEqual(self.put(11, b"x")[0], 409)
        self.assertEqual(self.put(0, b"hello")[0], 409)


class TestUploadCompleteConflicts(HTTPUploadTestCase):
    def test_incomplete_completion_is_409_and_publishes_nothing(self):
        session_id = self.begin("up/short", b"hello world")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 409)
        self.assertIn("error", self.document(body))
        self.assertEqual(self.request("GET", "/v1/objects/up/short")[0], 404)
        document = self.document(
            self.request("GET", "/v1/uploads/%s" % session_id)[2])
        self.assertEqual(document["offset"], 5)
        self.assertFalse(document["completed"])

    def test_hash_mismatch_is_409(self):
        session_id = self.begin_key_decl("up/wrong-hash", 3, sha(b"xyz"))
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"abc")
        status, _, _ = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 409)
        self.assertEqual(self.request("GET", "/v1/objects/up/wrong-hash")[0], 404)
        self.assertEqual(
            self.document(self.request("GET", "/v1/uploads/%s" % session_id)[2])
            ["offset"], 3)

    def begin_key_decl(self, key, size, digest, content_type=None):
        declaration = {"key": key, "size": size, "sha256": digest}
        if content_type is not None:
            declaration["content_type"] = content_type
        status, _, body = self.request("POST", "/v1/uploads",
                                       json.dumps(declaration).encode("utf-8"))
        assert status == 201
        return self.document(body)["session"]

    def test_repeated_complete_returns_first_result(self):
        session_id = self.begin("up/repeat", b"ab", content_type="text/plain")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        first_status, _, first = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(first_status, 200)
        # later put and delete of the key must not be overwritten
        self.request("PUT", "/v1/objects/up/repeat", b"newer")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(body, first)
        self.assertEqual(self.request("GET", "/v1/objects/up/repeat")[2], b"newer")
        self.request("DELETE", "/v1/objects/up/repeat")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(body, first)
        self.assertEqual(self.request("GET", "/v1/objects/up/repeat")[0], 404)

    def test_same_key_sessions_publish_in_completion_order(self):
        first = self.begin("up/ordered", b"from-first")
        second = self.begin("up/ordered", b"from-second!")
        self.request("PUT", "/v1/uploads/%s?offset=0" % first, b"from-first")
        self.request("PUT", "/v1/uploads/%s?offset=0" % second, b"from-second!")
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s/complete" % first)[0], 200)
        self.assertEqual(self.request("GET", "/v1/objects/up/ordered")[2],
                         b"from-first")
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s/complete" % second)[0], 200)
        self.assertEqual(self.request("GET", "/v1/objects/up/ordered")[2],
                         b"from-second!")

    def test_abort_removes_session(self):
        session_id = self.begin("up/aborted", b"hello")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello")
        status, _, body = self.request("DELETE", "/v1/uploads/%s" % session_id)
        self.assertEqual((status, body), (204, b""))
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s/complete" % session_id)[0], 404)


class TestUploadBadRequests(HTTPUploadTestCase):
    def test_bad_declaration_bodies_are_400(self):
        bodies = [
            b"not json",
            b"{",
            b"[]",
            b"null",
            b'"a string"',
            json.dumps({"size": 1, "sha256": sha(b"x")}).encode(),  # no key
            json.dumps({"key": "up/x", "sha256": sha(b"x")}).encode(),  # no size
            json.dumps({"key": "up/x", "size": 1}).encode(),  # no sha256
            json.dumps({"key": "", "size": 1, "sha256": sha(b"x")}).encode(),
            json.dumps({"key": "/bad", "size": 1, "sha256": sha(b"x")}).encode(),
            json.dumps({"key": "up/x", "size": -1, "sha256": sha(b"x")}).encode(),
            json.dumps({"key": "up/x", "size": "1", "sha256": sha(b"x")}).encode(),
            json.dumps({"key": "up/x", "size": True, "sha256": sha(b"x")}).encode(),
            json.dumps({"key": "up/x", "size": 1, "sha256": "Z" * 64}).encode(),
            json.dumps({"key": "up/x", "size": 1, "sha256": "abc"}).encode(),
            json.dumps({"key": "up/x", "size": 1,
                        "sha256": sha(b"x"), "content_type": 7}).encode(),
            b"\xff\xfe\x00invalid utf-8",
        ]
        before = set(self.store._uploads)
        for body in bodies:
            status, _, response = self.request("POST", "/v1/uploads", body)
            self.assertEqual(status, 400, body)
            self.assertIn("error", self.document(response))
        # nothing was created by the rejected attempts
        self.assertEqual(set(self.store._uploads), before)

    def test_offset_must_be_one_non_negative_decimal_integer(self):
        session_id = self.begin("up/offset", b"hello world")
        for query in ["", "offset=", "offset=-1", "offset=1.5",
                      "offset=abc", "offset=0x1", "offset=%2B0",
                      "offset=0&offset=1"]:
            status, _, body = self.request(
                "PUT", "/v1/uploads/%s?%s" % (session_id, query), b"x")
            self.assertEqual(status, 400, query)
            self.assertIn("error", self.document(body))
        # leading zeros are still a non-negative decimal integer
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=00" % session_id, b"hello ")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 6})

    def test_framing_errors_on_create(self):
        # Transfer-Encoding wins and is a 400
        status, body = self.raw_request(
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
        self.assertEqual(status, 400)
        self.assertIn("error", json.loads(body))
        # missing Content-Length is 411
        for request_bytes in [
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: abc\r\n\r\n",
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: -1\r\n\r\n",
        ]:
            status, body = self.raw_request(request_bytes)
            self.assertEqual(status, 411)
            self.assertIn("error", json.loads(body))
        # a body ending before the declared length is 400
        status, body = self.raw_request(
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 5\r\n\r\nab")
        self.assertEqual(status, 400)
        self.assertIn("error", json.loads(body))
        # Transfer-Encoding takes priority over a missing length
        status, body = self.raw_request(
            b"POST /v1/uploads HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n")
        self.assertEqual(status, 400)

    def test_framing_errors_on_append(self):
        session_id = self.begin("up/frame", b"abc")
        status, body = self.raw_request(
            b"PUT /v1/uploads/%s?offset=0 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
            % session_id.encode())
        self.assertEqual(status, 400)
        self.assertIn("error", json.loads(body))
        status, body = self.raw_request(
            b"PUT /v1/uploads/%s?offset=0 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
            % session_id.encode())
        self.assertEqual(status, 411)
        self.assertIn("error", json.loads(body))
        status, body = self.raw_request(
            b"PUT /v1/uploads/%s?offset=0 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            b"Content-Length: 3\r\n\r\nab" % session_id.encode())
        self.assertEqual(status, 400)
        self.assertIn("error", json.loads(body))
        # failed framing changed no progress
        self.assertEqual(
            self.document(self.request("GET", "/v1/uploads/%s" % session_id)[2])
            ["offset"], 0)

    def test_unknown_and_cancelled_sessions_are_404(self):
        missing = "a" * 32
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % missing)[0], 404)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % missing, b"x")
        self.assertEqual(status, 404)
        self.assertIn("error", self.document(body))
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s/complete" % missing)[0], 404)
        self.assertEqual(self.request("DELETE", "/v1/uploads/%s" % missing)[0], 404)
        session_id = self.begin("up/gone", b"x")
        self.assertEqual(self.request("DELETE", "/v1/uploads/%s" % session_id)[0], 204)
        self.assertEqual(self.request("GET", "/v1/uploads/%s" % session_id)[0], 404)

    def test_known_paths_reject_other_methods_with_405(self):
        session_id = self.begin("up/methods", b"x")
        self.assertEqual(self.request("GET", "/v1/uploads")[0], 405)
        self.assertEqual(self.request("PUT", "/v1/uploads", b"")[0], 405)
        self.assertEqual(self.request("DELETE", "/v1/uploads")[0], 405)
        self.assertEqual(self.request("HEAD", "/v1/uploads")[0], 405)
        self.assertEqual(
            self.request("POST", "/v1/uploads/%s" % session_id, b"")[0], 405)
        self.assertEqual(
            self.request("HEAD", "/v1/uploads/%s" % session_id)[0], 405)
        self.assertEqual(
            self.request("GET", "/v1/uploads/%s/complete" % session_id)[0], 405)
        self.assertEqual(
            self.request("DELETE", "/v1/uploads/%s/complete" % session_id)[0], 405)
        self.assertEqual(
            self.request("PUT", "/v1/uploads/%s/complete" % session_id, b"")[0], 405)

    def test_errors_are_json_objects_with_error_string(self):
        for status, _, body in [
            self.request("GET", "/v1/uploads/%s" % ("f" * 32)),
            self.request("POST", "/v1/uploads", b"not json"),
            self.request("GET", "/v1/uploads"),
        ]:
            document = json.loads(body)
            self.assertEqual(sorted(document), ["error"])
            self.assertIsInstance(document["error"], str)


class TestUploadEntryInterop(HTTPUploadTestCase):
    def test_python_session_continues_over_http(self):
        session_id = self.store.begin_upload("up/from-python", 11,
                                             sha(b"hello world"),
                                             content_type="text/plain")
        self.store.append_upload(session_id, 0, b"hello ")
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=6" % session_id, b"world")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"offset": 11})
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body)["sha256"], sha(b"hello world"))
        self.assertEqual(self.store.get("up/from-python")[0], b"hello world")

    def test_http_session_continues_in_python(self):
        session_id = self.begin("up/from-http", b"hello world")
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"hello ")
        self.assertEqual(self.store.append_upload(session_id, 6, b"world"), 11)
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(b"hello world"))
        status, _, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertTrue(self.document(body)["completed"])
        # repeating completion through HTTP still returns the first result
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), entry)


class TestUploadPersistenceHTTP(unittest.TestCase):
    """Sessions keep working after the data directory is reopened."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-http-up-persist-")
        self.store = ContentAddressedStore(self.root)
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
        shutil.rmtree(self.root, ignore_errors=True)

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            return response.status, response.read()
        finally:
            conn.close()

    def test_session_resumes_after_reopen(self):
        data = b"hello world"
        declaration = json.dumps({"key": "up/resume", "size": len(data),
                                  "sha256": sha(data)}).encode("utf-8")
        status, body = self.request("POST", "/v1/uploads", declaration)
        self.assertEqual(status, 201)
        session_id = json.loads(body)["session"]
        self.assertEqual(
            self.request("PUT", "/v1/uploads/%s?offset=0" % session_id,
                         b"hello ")[0], 200)
        # simulate a server restart against the same data directory
        self.server.store = ContentAddressedStore(self.root)
        status, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["offset"], 6)
        status, body = self.request(
            "PUT", "/v1/uploads/%s?offset=6" % session_id, b"world")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"offset": 11})
        status, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["sha256"], sha(data))
        # a second restart still remembers the completion
        self.server.store = ContentAddressedStore(self.root)
        status, body = self.request("GET", "/v1/uploads/%s" % session_id)
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["completed"])


if __name__ == "__main__":
    unittest.main()

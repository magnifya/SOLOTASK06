"""End-to-end tests for the stdlib HTTP front end on an ephemeral port."""

import hashlib
import http.client
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.http_app import create_server
from objstore.store import ContentAddressedStore


def sha256_of(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-")
        cls.store = ContentAddressedStore(cls.root)
        cls.server = create_server(cls.store, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(
            target=cls.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
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
            head = {name.lower(): value for name, value in response.getheaders()}
            return response.status, head, payload
        finally:
            conn.close()

    def json_body(self, payload):
        return json.loads(payload.decode("utf-8"))


class TestHealthz(HTTPTestCase):
    def test_healthz(self):
        status, headers, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(self.json_body(body), {"ok": True})
        self.assertEqual(headers["content-type"], "application/json")


class TestObjectLifecycle(HTTPTestCase):
    def test_put_get_head_delete(self):
        status, _, body = self.request("PUT", "/v1/objects/life/a.txt", b"hello",
                                       {"Content-Type": "text/plain"})
        self.assertEqual(status, 201)
        self.assertEqual(self.json_body(body),
                         {"key": "life/a.txt", "sha256": sha256_of(b"hello"), "size": 5})

        status, headers, body = self.request("GET", "/v1/objects/life/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"hello")
        self.assertEqual(headers["x-content-sha256"], sha256_of(b"hello"))
        self.assertEqual(headers["content-type"], "text/plain")
        self.assertEqual(headers["content-length"], "5")

        status, headers, body = self.request("HEAD", "/v1/objects/life/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(headers["content-length"], "5")
        self.assertEqual(headers["x-content-sha256"], sha256_of(b"hello"))

        status, _, body = self.request("DELETE", "/v1/objects/life/a.txt")
        self.assertEqual(status, 204)
        self.assertEqual(body, b"")
        self.assertEqual(self.request("GET", "/v1/objects/life/a.txt")[0], 404)

    def test_default_content_type(self):
        self.request("PUT", "/v1/objects/plain.bin", b"raw")
        status, headers, body = self.request("GET", "/v1/objects/plain.bin")
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "application/octet-stream")
        self.assertEqual(body, b"raw")

    def test_repeat_put_of_same_bytes_is_idempotent(self):
        first = self.request("PUT", "/v1/objects/idem.txt", b"same")
        self.assertEqual(first[0], 201)
        second = self.request("PUT", "/v1/objects/idem.txt", b"same")
        self.assertEqual(second[0], 200)
        self.assertEqual(self.json_body(first[2]), self.json_body(second[2]))

    def test_overwrite_with_new_bytes_returns_201(self):
        self.request("PUT", "/v1/objects/over.txt", b"one")
        status, _, body = self.request("PUT", "/v1/objects/over.txt", b"two")
        self.assertEqual(status, 201)
        self.assertEqual(self.json_body(body)["sha256"], sha256_of(b"two"))
        self.assertEqual(self.request("GET", "/v1/objects/over.txt")[2], b"two")

    def test_delete_missing_key_is_404(self):
        status, _, body = self.request("DELETE", "/v1/objects/ghost.txt")
        self.assertEqual(status, 404)
        self.assertEqual(self.json_body(body), {"error": "not found: ghost.txt"})


class TestDedup(HTTPTestCase):
    def test_two_keys_one_blob(self):
        first = self.request("PUT", "/v1/objects/dedup/a.txt", b"shared-bytes")
        second = self.request("PUT", "/v1/objects/dedup/b.txt", b"shared-bytes")
        self.assertEqual(first[0], 201)
        self.assertEqual(second[0], 201)
        sha = sha256_of(b"shared-bytes")
        self.assertEqual(self.json_body(first[2])["sha256"], sha)
        self.assertEqual(self.json_body(second[2])["sha256"], sha)
        self.assertEqual(self.store.blob_digests(), [sha])

        status, headers, body = self.request("GET", "/v1/blobs/%s" % sha)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"shared-bytes")
        self.assertEqual(headers["x-content-sha256"], sha)


class TestListingAPI(HTTPTestCase):
    def setUp(self):
        for key in ["list/b.txt", "list/a.txt", "list/sub/c.txt"]:
            self.request("PUT", "/v1/objects/%s" % key, key.encode("utf-8"))

    def test_full_listing_is_sorted(self):
        status, _, body = self.request("GET", "/v1/objects?prefix=list/&limit=100")
        self.assertEqual(status, 200)
        document = self.json_body(body)
        self.assertEqual(sorted(document), ["items", "next_after"])
        self.assertEqual([item["key"] for item in document["items"]],
                         ["list/a.txt", "list/b.txt", "list/sub/c.txt"])
        self.assertIsNone(document["next_after"])

    def test_pagination(self):
        status, _, body = self.request("GET", "/v1/objects?prefix=list/&limit=2")
        self.assertEqual(status, 200)
        document = self.json_body(body)
        self.assertEqual(len(document["items"]), 2)
        self.assertEqual(document["next_after"], "list/b.txt")
        status, _, body = self.request(
            "GET", "/v1/objects?prefix=list/&after=list/b.txt&limit=2")
        self.assertEqual(status, 200)
        document = self.json_body(body)
        self.assertEqual([item["key"] for item in document["items"]], ["list/sub/c.txt"])
        self.assertIsNone(document["next_after"])

    def test_bad_limit_is_400(self):
        for bad in ["0", "-1", "abc", "100000"]:
            status, _, body = self.request("GET", "/v1/objects?limit=%s" % bad)
            self.assertEqual(status, 400)
            self.assertIn("error", self.json_body(body))


class TestErrors(HTTPTestCase):
    def test_unknown_key_is_404(self):
        status, _, body = self.request("GET", "/v1/objects/missing.txt")
        self.assertEqual(status, 404)
        self.assertEqual(self.json_body(body), {"error": "not found: missing.txt"})

    def test_unknown_head_is_404(self):
        status, _, body = self.request("HEAD", "/v1/objects/missing.txt")
        self.assertEqual(status, 404)

    def test_malformed_sha_is_400(self):
        for bad in ["zz", "ABC", "0" * 63]:
            status, _, body = self.request("GET", "/v1/blobs/%s" % bad)
            self.assertEqual(status, 400)
            self.assertEqual(self.json_body(body)["error"].split(":")[0], "invalid sha256")

    def test_unknown_sha_is_404(self):
        status, _, body = self.request("GET", "/v1/blobs/%s" % ("a" * 64))
        self.assertEqual(status, 404)
        self.assertIn("error", self.json_body(body))

    def test_empty_key_is_400(self):
        status, _, body = self.request("PUT", "/v1/objects/", b"x")
        self.assertEqual(status, 400)
        self.assertEqual(self.json_body(body), {"error": "invalid key"})

    def test_unknown_route_is_404(self):
        status, _, body = self.request("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(self.json_body(body), {"error": "not found"})

    def test_bad_method_is_405(self):
        status, _, body = self.request("DELETE", "/healthz")
        self.assertEqual(status, 405)
        self.assertIn("error", self.json_body(body))


if __name__ == "__main__":
    unittest.main()

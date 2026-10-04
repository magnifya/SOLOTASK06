"""End-to-end tests for the stdlib HTTP front end on an ephemeral port."""

import hashlib
import http.client
import json
import shutil
import tempfile
import threading
import unittest

from objstore.http_app import create_server
from objstore.store import ContentAddressedStore


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-")
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


class TestHealthz(HTTPTestCase):
    def test_healthz(self):
        status, headers, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"ok": True})
        self.assertEqual(headers["content-type"], "application/json")

    def test_healthz_rejects_other_methods(self):
        status, _, body = self.request("DELETE", "/healthz")
        self.assertEqual(status, 405)
        self.assertIn("error", self.document(body))


class TestObjectLifecycle(HTTPTestCase):
    def test_put_get_head_delete(self):
        status, _, body = self.request("PUT", "/v1/objects/life/a.txt", b"hello",
                                       {"Content-Type": "text/plain"})
        self.assertEqual(status, 201)
        self.assertEqual(self.document(body),
                         {"key": "life/a.txt", "sha256": sha(b"hello"), "size": 5})

        status, headers, body = self.request("GET", "/v1/objects/life/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"hello")
        self.assertEqual(headers["x-content-sha256"], sha(b"hello"))
        self.assertEqual(headers["content-type"], "text/plain")
        self.assertEqual(headers["content-length"], "5")

        status, headers, body = self.request("HEAD", "/v1/objects/life/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(headers["content-length"], "5")
        self.assertEqual(headers["x-content-sha256"], sha(b"hello"))

        status, _, body = self.request("DELETE", "/v1/objects/life/a.txt")
        self.assertEqual((status, body), (204, b""))
        self.assertEqual(self.request("GET", "/v1/objects/life/a.txt")[0], 404)

    def test_default_content_type(self):
        self.request("PUT", "/v1/objects/plain.bin", b"raw")
        status, headers, body = self.request("GET", "/v1/objects/plain.bin")
        self.assertEqual((status, body), (200, b"raw"))
        self.assertEqual(headers["content-type"], "application/octet-stream")

    def test_repeat_put_is_idempotent(self):
        first = self.request("PUT", "/v1/objects/idem.txt", b"same")
        second = self.request("PUT", "/v1/objects/idem.txt", b"same")
        self.assertEqual((first[0], second[0]), (201, 200))
        self.assertEqual(self.document(first[2]), self.document(second[2]))

    def test_new_bytes_for_same_key_is_created(self):
        self.request("PUT", "/v1/objects/over.txt", b"one")
        status, _, body = self.request("PUT", "/v1/objects/over.txt", b"two")
        self.assertEqual(status, 201)
        self.assertEqual(self.document(body)["sha256"], sha(b"two"))
        self.assertEqual(self.request("GET", "/v1/objects/over.txt")[2], b"two")

    def test_delete_missing_key_is_404(self):
        status, _, body = self.request("DELETE", "/v1/objects/ghost.txt")
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: ghost.txt"})


class TestDedup(HTTPTestCase):
    def test_two_keys_share_one_blob(self):
        first = self.request("PUT", "/v1/objects/dedup/a.txt", b"shared-bytes")
        second = self.request("PUT", "/v1/objects/dedup/b.txt", b"shared-bytes")
        self.assertEqual((first[0], second[0]), (201, 201))
        self.assertEqual(self.document(first[2])["sha256"], sha(b"shared-bytes"))
        self.assertEqual(self.document(second[2])["sha256"], sha(b"shared-bytes"))
        self.assertEqual(self.store.blob_digests(), [sha(b"shared-bytes")])

        status, headers, body = self.request("GET", "/v1/blobs/%s" % sha(b"shared-bytes"))
        self.assertEqual((status, body), (200, b"shared-bytes"))
        self.assertEqual(headers["x-content-sha256"], sha(b"shared-bytes"))


class TestListingAPI(HTTPTestCase):
    def setUp(self):
        for key in ["list/b.txt", "list/a.txt", "list/sub/c.txt"]:
            self.request("PUT", "/v1/objects/%s" % key, key.encode("utf-8"))

    def test_sorted_listing(self):
        status, _, body = self.request("GET", "/v1/objects?prefix=list/&limit=100")
        self.assertEqual(status, 200)
        document = self.document(body)
        self.assertEqual(sorted(document), ["items", "next_after"])
        self.assertEqual([item["key"] for item in document["items"]],
                         ["list/a.txt", "list/b.txt", "list/sub/c.txt"])
        self.assertIsNone(document["next_after"])

    def test_pagination(self):
        status, _, body = self.request("GET", "/v1/objects?prefix=list/&limit=2")
        self.assertEqual(status, 200)
        document = self.document(body)
        self.assertEqual([item["key"] for item in document["items"]], ["list/a.txt", "list/b.txt"])
        self.assertEqual(document["next_after"], "list/b.txt")
        status, _, body = self.request(
            "GET", "/v1/objects?prefix=list/&after=list/b.txt&limit=2")
        self.assertEqual(status, 200)
        document = self.document(body)
        self.assertEqual([item["key"] for item in document["items"]], ["list/sub/c.txt"])
        self.assertIsNone(document["next_after"])

    def test_bad_limit_is_400(self):
        for bad in ["0", "-1", "abc", "100000"]:
            status, _, body = self.request("GET", "/v1/objects?limit=%s" % bad)
            self.assertEqual(status, 400)
            self.assertTrue(self.document(body)["error"].startswith("invalid limit"))


class TestErrors(HTTPTestCase):
    def test_unknown_key_is_404(self):
        status, _, body = self.request("GET", "/v1/objects/missing.txt")
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: missing.txt"})

    def test_unknown_head_is_404(self):
        self.assertEqual(self.request("HEAD", "/v1/objects/missing.txt")[0], 404)

    def test_malformed_digest_is_400(self):
        for bad in ["zz", "ABC", "0" * 63]:
            status, _, body = self.request("GET", "/v1/blobs/%s" % bad)
            self.assertEqual(status, 400)
            self.assertTrue(self.document(body)["error"].startswith("invalid sha256"))

    def test_unknown_digest_is_404(self):
        status, _, body = self.request("GET", "/v1/blobs/%s" % ("a" * 64))
        self.assertEqual(status, 404)
        self.assertIn("error", self.document(body))

    def test_empty_key_is_400(self):
        status, _, body = self.request("PUT", "/v1/objects/", b"x")
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid key"})

    def test_unknown_route_is_404(self):
        status, _, body = self.request("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found"})

    def test_missing_content_length_is_411(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("PUT", "/v1/objects/nolen.txt")
            conn.endheaders()
            response = conn.getresponse()
            status, body = response.status, response.read()
        finally:
            conn.close()
        self.assertEqual(status, 411)
        self.assertEqual(self.document(body), {"error": "length required"})


if __name__ == "__main__":
    unittest.main()

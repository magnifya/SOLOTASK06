"""End-to-end tests for conditional writes and ETag over HTTP."""

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


class HTTPPreconditionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-precond-")
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

    def raw_request(self, method, path, header_pairs, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest(method, path)
            for name, value in header_pairs:
                conn.putheader(name, value)
            conn.endheaders(body)
            response = conn.getresponse()
            payload = response.read()
            return response.status, {k.lower(): v for k, v in response.getheaders()}, payload
        finally:
            conn.close()

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def put_object(self, key, data, headers=None):
        return self.request("PUT", "/v1/objects/%s" % key, data, headers)


class TestObjectPreconditions(HTTPPreconditionTestCase):
    def test_if_match_allows_update_with_current_digest(self):
        self.put_object("cond/a.txt", b"v1")
        status, _, _ = self.put_object("cond/a.txt", b"v2",
                                       {"If-Match": '"%s"' % sha(b"v1")})
        self.assertEqual(status, 201)
        self.assertEqual(self.request("GET", "/v1/objects/cond/a.txt")[2], b"v2")

    def test_if_match_with_stale_digest_is_412(self):
        self.put_object("cond/b.txt", b"v1")
        status, _, body = self.put_object("cond/b.txt", b"v2",
                                          {"If-Match": '"%s"' % sha(b"other")})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/cond/b.txt")[2], b"v1")

    def test_if_match_on_missing_key_is_412(self):
        status, _, body = self.put_object("cond/missing.txt", b"v1",
                                          {"If-Match": '"%s"' % sha(b"v1")})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/cond/missing.txt")[0], 404)

    def test_if_none_match_star_creates_only_when_absent(self):
        status, _, _ = self.put_object("cond/c.txt", b"v1",
                                       {"If-None-Match": "*"})
        self.assertEqual(status, 201)
        status, _, body = self.put_object("cond/c.txt", b"v2",
                                          {"If-None-Match": "*"})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/cond/c.txt")[2], b"v1")

    def test_both_headers_is_400(self):
        status, _, body = self.put_object(
            "cond/d.txt", b"v1",
            {"If-Match": '"%s"' % sha(b"v1"), "If-None-Match": "*"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})

    def test_malformed_precondition_headers_are_400(self):
        bad_if_match = ["*", sha(b"v1"), '"%s"' % sha(b"v1").upper(),
                        '"%s", "%s"' % (sha(b"a"), sha(b"b")), '"%s' % sha(b"v1"),
                        'W/"%s"' % sha(b"v1")]
        for value in bad_if_match:
            status, _, body = self.put_object("cond/e.txt", b"v1",
                                              {"If-Match": value})
            self.assertEqual(status, 400, value)
            self.assertEqual(self.document(body), {"error": "invalid precondition"})
        for value in ['"%s"' % sha(b"v1"), "", "**"]:
            status, _, body = self.put_object("cond/e.txt", b"v1",
                                              {"If-None-Match": value})
            self.assertEqual(status, 400, value)
            self.assertEqual(self.document(body), {"error": "invalid precondition"})
        self.assertEqual(self.request("GET", "/v1/objects/cond/e.txt")[0], 404)

    def test_repeated_precondition_header_is_400(self):
        pairs = [("If-Match", '"%s"' % sha(b"a")), ("If-Match", '"%s"' % sha(b"b")),
                 ("Content-Length", "2")]
        status, _, body = self.raw_request("PUT", "/v1/objects/cond/f.txt", pairs, b"v1")
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})
        pairs = [("If-None-Match", "*"), ("If-None-Match", "*"),
                 ("Content-Length", "2")]
        status, _, body = self.raw_request("PUT", "/v1/objects/cond/f.txt", pairs, b"v1")
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})

    def test_delete_honours_preconditions(self):
        self.put_object("cond/g.txt", b"v1")
        status, _, body = self.request("DELETE", "/v1/objects/cond/g.txt",
                                       headers={"If-Match": '"%s"' % sha(b"other")})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        status, _, _ = self.request("DELETE", "/v1/objects/cond/g.txt",
                                    headers={"If-None-Match": "*"})
        self.assertEqual(status, 412)
        status, _, _ = self.request("DELETE", "/v1/objects/cond/g.txt",
                                    headers={"If-Match": '"%s"' % sha(b"v1")})
        self.assertEqual(status, 204)
        self.assertEqual(self.request("GET", "/v1/objects/cond/g.txt")[0], 404)

    def test_delete_if_none_match_on_missing_key_is_still_404(self):
        status, _, body = self.request("DELETE", "/v1/objects/cond/ghost.txt",
                                       headers={"If-None-Match": "*"})
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: cond/ghost.txt"})

    def test_delete_with_malformed_precondition_is_400(self):
        self.put_object("cond/h.txt", b"v1")
        status, _, body = self.request("DELETE", "/v1/objects/cond/h.txt",
                                       headers={"If-Match": "not-a-digest"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})
        self.assertEqual(self.request("GET", "/v1/objects/cond/h.txt")[0], 200)


class TestETag(HTTPPreconditionTestCase):
    def test_get_and_head_carry_etag(self):
        self.put_object("etag/a.txt", b"payload")
        status, headers, body = self.request("GET", "/v1/objects/etag/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(headers["etag"], '"%s"' % sha(b"payload"))
        self.assertEqual(body, b"payload")
        status, headers, body = self.request("HEAD", "/v1/objects/etag/a.txt")
        self.assertEqual(status, 200)
        self.assertEqual(headers["etag"], '"%s"' % sha(b"payload"))
        self.assertEqual(body, b"")

    def test_etag_tracks_the_current_digest(self):
        self.put_object("etag/b.txt", b"one")
        first = self.request("GET", "/v1/objects/etag/b.txt")[1]["etag"]
        self.put_object("etag/b.txt", b"two")
        second = self.request("GET", "/v1/objects/etag/b.txt")[1]["etag"]
        self.assertEqual(first, '"%s"' % sha(b"one"))
        self.assertEqual(second, '"%s"' % sha(b"two"))

    def test_other_successful_responses_have_no_etag(self):
        status, headers, _ = self.put_object("etag/c.txt", b"data")
        self.assertEqual(status, 201)
        self.assertNotIn("etag", headers)
        status, headers, _ = self.request("DELETE", "/v1/objects/etag/c.txt")
        self.assertEqual(status, 204)
        self.assertNotIn("etag", headers)
        self.put_object("etag/d.txt", b"data")
        status, headers, _ = self.request("GET", "/v1/blobs/%s" % sha(b"data"))
        self.assertEqual(status, 200)
        self.assertNotIn("etag", headers)
        status, headers, _ = self.request("GET", "/v1/objects?prefix=etag/")
        self.assertEqual(status, 200)
        self.assertNotIn("etag", headers)


class TestUploadPreconditions(HTTPPreconditionTestCase):
    def create_session(self, key, data, headers=None):
        document = {"key": key, "size": len(data), "sha256": sha(data)}
        status, _, body = self.request("POST", "/v1/uploads",
                                       json.dumps(document).encode("utf-8"),
                                       headers)
        return status, self.document(body)

    def upload_and_complete(self, session, data):
        status, _, _ = self.request("PUT", "/v1/uploads/%s?offset=0" % session, data)
        self.assertEqual(status, 200)
        return self.request("POST", "/v1/uploads/%s/complete" % session)

    def test_if_none_match_star_guards_completion(self):
        status, body = self.create_session("up/a.txt", b"content",
                                           {"If-None-Match": "*"})
        self.assertEqual(status, 201)
        self.put_object("up/a.txt", b"racing-write")
        status, _, payload = self.upload_and_complete(body["session"], b"content")
        self.assertEqual(status, 412)
        self.assertEqual(self.document(payload), {"error": "precondition failed"})
        # the racing object is untouched and the session can still complete
        self.assertEqual(self.request("GET", "/v1/objects/up/a.txt")[2],
                         b"racing-write")
        self.request("DELETE", "/v1/objects/up/a.txt")
        status, _, payload = self.request(
            "POST", "/v1/uploads/%s/complete" % body["session"])
        self.assertEqual(status, 200)
        self.assertEqual(self.document(payload)["sha256"], sha(b"content"))

    def test_if_match_guards_completion(self):
        self.put_object("up/b.txt", b"v1")
        status, body = self.create_session("up/b.txt", b"content",
                                           {"If-Match": '"%s"' % sha(b"v1")})
        self.assertEqual(status, 201)
        status, _, payload = self.upload_and_complete(body["session"], b"content")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(payload)["sha256"], sha(b"content"))

    def test_bad_precondition_headers_on_session_create_are_400(self):
        document = json.dumps({"key": "up/c.txt", "size": 1,
                               "sha256": sha(b"x")}).encode("utf-8")
        for headers in [{"If-Match": "*"},
                        {"If-None-Match": '"%s"' % sha(b"x")},
                        {"If-Match": '"%s"' % sha(b"x"), "If-None-Match": "*"}]:
            status, _, body = self.request("POST", "/v1/uploads", document, headers)
            self.assertEqual(status, 400, headers)
            self.assertEqual(self.document(body), {"error": "invalid precondition"})

    def test_body_errors_still_precede_precondition_evaluation(self):
        # malformed JSON body keeps its 400 even with valid headers
        status, _, body = self.request("POST", "/v1/uploads", b"{not json",
                                       {"If-None-Match": "*"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))
        # unknown session completion keeps its 404
        status, _, body = self.request("POST", "/v1/uploads/%s/complete" % ("0" * 32))
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()

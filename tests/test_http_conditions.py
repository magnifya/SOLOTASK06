"""End-to-end tests for conditional writes and ETags over HTTP."""

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


class HTTPConditionTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-http-cond-")
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
            return response.status, {k.lower(): v for k, v in
                                     response.getheaders()}, payload
        finally:
            conn.close()

    def raw_request(self, raw):
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
        status = int(response.split(b"\r\n", 1)[0].split()[1])
        head, _, body = response.partition(b"\r\n\r\n")
        return status, head, body

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def put(self, key, data, headers=None):
        return self.request("PUT", "/v1/objects/%s" % key, data, headers)

    def etag(self, digest):
        return '"%s"' % digest


class TestETagHeader(HTTPConditionTestCase):
    def test_get_and_head_carry_the_quoted_digest(self):
        digest = sha(b"etagged")
        self.put("e.txt", b"etagged", {"Content-Type": "text/plain"})
        status, headers, body = self.request("GET", "/v1/objects/e.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"etagged")
        self.assertEqual(headers["etag"], self.etag(digest))
        self.assertEqual(headers["x-content-sha256"], digest)
        status, headers, body = self.request("HEAD", "/v1/objects/e.txt")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(headers["etag"], self.etag(digest))
        self.assertEqual(headers["x-content-sha256"], digest)

    def test_other_success_responses_are_unchanged(self):
        digest = sha(b"plain")
        status, headers, _ = self.put("p.bin", b"plain")
        self.assertEqual(status, 201)
        self.assertNotIn("etag", headers)
        status, headers, _ = self.request("GET", "/v1/objects?limit=10")
        self.assertEqual(status, 200)
        self.assertNotIn("etag", headers)
        status, headers, _ = self.request("GET", "/v1/blobs/%s" % digest)
        self.assertEqual(status, 200)
        self.assertNotIn("etag", headers)
        self.assertIn("x-content-sha256", headers)


class TestConditionalPut(HTTPConditionTestCase):
    def test_if_match_matching_digest_updates(self):
        old = sha(b"v1")
        self.put("k", b"v1")
        status, _, body = self.put("k", b"v2", {"If-Match": self.etag(old)})
        self.assertEqual(status, 201)
        self.assertEqual(self.document(body)["sha256"], sha(b"v2"))

    def test_if_match_stale_digest_is_412_and_changes_nothing(self):
        old = sha(b"v1")
        self.put("k", b"v1")
        status, _, body = self.put("k", b"v2", {"If-Match": self.etag(old)})
        self.assertEqual(status, 201)
        status, headers, body = self.put("k", b"v3", {"If-Match": self.etag(old)})
        self.assertEqual(status, 412)
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], b"v2")

    def test_if_match_on_missing_key_is_412(self):
        status, _, body = self.put("ghost", b"x",
                                   {"If-Match": self.etag(sha(b"x"))})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/ghost")[0], 404)

    def test_if_none_match_star_creates_only_when_absent(self):
        status, _, _ = self.put("k", b"v1", {"If-None-Match": "*"})
        self.assertEqual(status, 201)
        status, _, body = self.put("k", b"v2", {"If-None-Match": "*"})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], b"v1")

    def test_other_keys_do_not_satisfy_an_absence_condition(self):
        self.put("other", b"present")
        status, _, _ = self.put("k", b"v", {"If-None-Match": "*"})
        self.assertEqual(status, 201)


class TestConditionalDelete(HTTPConditionTestCase):
    def test_matching_if_match_deletes(self):
        self.put("k", b"v")
        status, _, body = self.request(
            "DELETE", "/v1/objects/k", None,
            {"If-Match": self.etag(sha(b"v"))})
        self.assertEqual((status, body), (204, b""))
        self.assertEqual(self.request("GET", "/v1/objects/k")[0], 404)

    def test_stale_if_match_is_412_and_keeps_object(self):
        self.put("k", b"v2")
        status, _, body = self.request(
            "DELETE", "/v1/objects/k", None,
            {"If-Match": self.etag(sha(b"v1"))})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], b"v2")

    def test_if_none_match_star_on_existing_is_412(self):
        self.put("k", b"v")
        status, _, body = self.request(
            "DELETE", "/v1/objects/k", None, {"If-None-Match": "*"})
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        self.assertEqual(self.request("GET", "/v1/objects/k")[2], b"v")

    def test_if_none_match_star_on_absent_is_still_404(self):
        status, _, body = self.request(
            "DELETE", "/v1/objects/ghost", None, {"If-None-Match": "*"})
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: ghost"})


class TestMalformedPreconditionHeaders(HTTPConditionTestCase):
    def test_bad_if_match_is_400(self):
        bad_values = [
            "*",                                  # belongs to If-None-Match
            sha(b"v"),                            # unquoted digest
            sha(b"v").upper().join(['"', '"']),   # uppercase hex
            '"%s0"' % sha(b"v"),                 # 65 hex digits
            '"%s"' % sha(b"v")[:63],              # 63 hex digits
            '""', 'W/"%s"' % sha(b"v"),           # empty / weak
            '"%s", "%s"' % (sha(b"a"), sha(b"b")),# list of etags
            '"%s, ' % sha(b"v"),                  # unterminated quote
        ]
        for value in bad_values:
            status, _, body = self.put("k", b"q", {"If-Match": value})
            self.assertEqual(status, 400, repr(value))
            self.assertEqual(self.document(body),
                             {"error": "invalid precondition"})

    def test_bad_if_none_match_is_400(self):
        # leading/trailing OWS is stripped by HTTP field parsing, so "* " and
        # " *" normalize to the valid "*" and are not malformed.
        for value in ['"%s"' % sha(b"v"), "**", '"*"', "tag"]:
            status, _, body = self.put("k", b"q", {"If-None-Match": value})
            self.assertEqual(status, 400, repr(value))
            self.assertEqual(self.document(body),
                             {"error": "invalid precondition"})

    def test_both_headers_is_400(self):
        status, _, body = self.put(
            "k", b"q", {"If-Match": self.etag(sha(b"q")),
                        "If-None-Match": "*"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})

    def test_repeated_header_is_400(self):
        digest = sha(b"v")
        raw = (
            b'PUT /v1/objects/k HTTP/1.1\r\nHost: x\r\nConnection: close\r\n'
            b'Content-Length: 1\r\nIf-Match: "%s"\r\nIf-Match: "%s"\r\n\r\nv'
            % (digest.encode(), digest.encode()))
        status, _, body = self.raw_request(raw)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid precondition"})
        raw = (b'DELETE /v1/objects/k HTTP/1.1\r\nHost: x\r\nConnection: close\r\n'
               b'If-None-Match: *\r\nIf-None-Match: *\r\n\r\n')
        status, _, body = self.raw_request(raw)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid precondition"})

    def test_invalid_header_leaves_route_and_body_validation_intact(self):
        # missing Content-Length still wins with 411 over the bad header
        raw = (b'PUT /v1/objects/k HTTP/1.1\r\nHost: x\r\nConnection: close\r\n'
               b'If-Match: nonsense\r\n\r\n')
        status, _, body = self.raw_request(raw)
        self.assertEqual(status, 411)
        self.assertEqual(json.loads(body), {"error": "length required"})
        # an empty key is reported before the conditional headers
        status, _, _ = self.request("DELETE", "/v1/objects/", None,
                                    {"If-Match": "nonsense"})
        self.assertEqual(status, 400)

    def test_conditional_headers_on_other_routes_are_ignored(self):
        self.put("k", b"v")
        for method, path, body, headers in [
            ("GET", "/v1/objects/k", None, {"If-Match": "garbage"}),
            ("HEAD", "/v1/objects/k", None, {"If-None-Match": "garbage"}),
            ("GET", "/v1/objects?limit=5", None, {"If-Match": "garbage"}),
        ]:
            status, _, _ = self.request(method, path, body, headers)
            self.assertEqual(status, 200, (method, path))


class TestConditionalUploadCreation(HTTPConditionTestCase):
    def begin(self, key, data, headers=None):
        declaration = json.dumps(
            {"key": key, "size": len(data), "sha256": sha(data)}).encode("utf-8")
        status, _, body = self.request("POST", "/v1/uploads", declaration,
                                       headers)
        self.assertEqual(status, 201, body)
        return self.document(body)["session"]

    def upload(self, session_id, data):
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, data)
        self.assertEqual(status, 200)

    def test_none_match_session_publishes_only_while_absent(self):
        session_id = self.begin("u", b"ab", {"If-None-Match": "*"})
        self.upload(session_id, b"ab")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 200)
        self.assertEqual(self.request("GET", "/v1/objects/u")[2], b"ab")

        second = self.begin("u", b"cd", {"If-None-Match": "*"})
        self.upload(second, b"cd")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % second)
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        # progress is retained and the object is untouched
        status, _, body = self.request("GET", "/v1/uploads/%s" % second)
        self.assertEqual(self.document(body)["offset"], 2)
        self.assertEqual(self.request("GET", "/v1/objects/u")[2], b"ab")
        # once the key is gone, completing again succeeds
        self.request("DELETE", "/v1/objects/u")
        status, _, _ = self.request(
            "POST", "/v1/uploads/%s/complete" % second)
        self.assertEqual(status, 200)
        self.assertEqual(self.request("GET", "/v1/objects/u")[2], b"cd")

    def test_if_match_session_publishes_only_on_old_digest(self):
        self.put("u", b"old")
        session_id = self.begin("u", b"new", {"If-Match": self.etag(sha(b"old"))})
        self.upload(session_id, b"new")
        self.assertEqual(self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)[0], 200)
        stale = self.begin("u", b"newer",
                           {"If-Match": self.etag(sha(b"old"))})
        self.upload(stale, b"newer")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % stale)
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})

    def test_no_condition_header_keeps_sessions_unconditional(self):
        self.put("u", b"old")
        session_id = self.begin("u", b"new")
        self.upload(session_id, b"new")
        self.assertEqual(self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)[0], 200)

    def test_malformed_header_on_create_is_400(self):
        declaration = json.dumps(
            {"key": "z", "size": 1, "sha256": sha(b"q")}).encode("utf-8")
        status, _, body = self.request("POST", "/v1/uploads", declaration,
                                       {"If-Match": "nope"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid precondition"})

    def test_body_errors_precede_the_precondition_header(self):
        # bad JSON/field values are reported instead of the bad header
        cases = [
            b"{not json",
            b'{"size":1,"sha256":"' + b"a" * 64 + b'"}',            # missing key
            b'{"key":"a/../b","size":1,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":-1,"sha256":"' + b"a" * 64 + b'"}',
            b'{"key":"k","size":1,"sha256":"ZZ"}',
        ]
        for case in cases:
            status, _, body = self.request(
                "POST", "/v1/uploads", case, {"If-Match": "nope"})
            self.assertEqual(status, 400, case)
            self.assertNotEqual(self.document(body)["error"],
                                "invalid precondition")

    def test_length_and_hash_conflicts_precede_the_condition(self):
        # key exists, session requires absence, but content is incomplete:
        # the 409 conflict is reported, not a 412
        session_id = self.begin("u", b"abcd", {"If-None-Match": "*"})
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"ab")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 409)
        self.assertNotIn("precondition", body.decode("utf-8"))
        # wrong declared hash likewise reports the 409 hash mismatch
        declaration = json.dumps(
            {"key": "u", "size": 3, "sha256": sha(b"zzz")}).encode("utf-8")
        session_id = self.document(
            self.request("POST", "/v1/uploads", declaration,
                         {"If-None-Match": "*"})[2])["session"]
        self.request("PUT", "/v1/uploads/%s?offset=0" % session_id, b"abc")
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id)
        self.assertEqual(status, 409)
        self.assertIn("hash mismatch", body.decode("utf-8"))

    def test_unknown_session_with_condition_header_is_404(self):
        # complete/append routes carry no precondition meaning; the unknown
        # session is still reported as before
        status, _, _ = self.request(
            "POST", "/v1/uploads/%s/complete" % ("f" * 32), b"",
            {"If-Match": "garbage"})
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()

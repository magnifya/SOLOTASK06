"""End-to-end tests for the rate-limit HTTP endpoints and enforcement."""

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


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPRateLimitTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-ratelimit-")
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

    def set_limit(self, tenant, **policy):
        status, _, body = self.request(
            "PUT", "/v1/rate-limit", json.dumps(policy),
            {"X-Objstore-Tenant": tenant})
        assert status == 200, body


class TestRateLimitEndpoint(HTTPRateLimitTestCase):
    def test_get_defaults_to_null(self):
        status, _, body = self.request(
            "GET", "/v1/rate-limit", headers={"X-Objstore-Tenant": "rl-unset"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_put_and_get_roundtrip(self):
        headers = {"X-Objstore-Tenant": "rl-round"}
        status, _, body = self.request(
            "PUT", "/v1/rate-limit",
            json.dumps({"window_seconds": 60, "max_requests": 10,
                        "max_bytes": 1000}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 1000})
        status, _, body = self.request("GET", "/v1/rate-limit", headers=headers)
        self.assertEqual(self.document(body),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 1000})

    def test_put_single_limit(self):
        headers = {"X-Objstore-Tenant": "rl-single"}
        status, _, body = self.request(
            "PUT", "/v1/rate-limit",
            json.dumps({"window_seconds": 3600, "max_bytes": 64}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"window_seconds": 3600, "max_requests": None,
                          "max_bytes": 64})

    def test_put_rejects_invalid_policies(self):
        headers = {"X-Objstore-Tenant": "rl-bad"}
        for document in ({}, {"window_seconds": 0}, {"window_seconds": -1},
                         {"window_seconds": 86401}, {"window_seconds": True},
                         {"window_seconds": "60"}, {"window_seconds": 1.5},
                         {"window_seconds": None},
                         {"window_seconds": 60},
                         {"window_seconds": 60, "max_requests": None,
                          "max_bytes": None},
                         {"window_seconds": 60, "max_requests": -1},
                         {"window_seconds": 60, "max_bytes": -1},
                         {"window_seconds": 60, "max_requests": True},
                         {"window_seconds": 60, "max_bytes": False},
                         {"window_seconds": 60, "max_requests": "5"},
                         {"window_seconds": 60, "max_bytes": 2.5},
                         {"window_seconds": 60, "max_requests": 1, "x": 2},
                         {"window_second": 60, "max_requests": 1}):
            status, _, body = self.request("PUT", "/v1/rate-limit",
                                           json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(body),
                             {"error": "invalid rate limit"})

    def test_put_rejects_bad_json(self):
        headers = {"X-Objstore-Tenant": "rl-badjson"}
        for body in ("{", "[1, 2]", "42"):
            status, _, payload = self.request("PUT", "/v1/rate-limit", body,
                                              headers)
            self.assertEqual(status, 400, body)
            self.assertEqual(self.document(payload),
                             {"error": "invalid rate limit"})

    def test_delete_clears_policy(self):
        headers = {"X-Objstore-Tenant": "rl-clear"}
        self.set_limit("rl-clear", window_seconds=60, max_requests=1)
        status, _, body = self.request("DELETE", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)
        status, _, body = self.request("GET", "/v1/rate-limit", headers=headers)
        self.assertEqual(self.document(body), None)
        # Clearing an unset tenant succeeds as well.
        status, _, body = self.request(
            "DELETE", "/v1/rate-limit",
            headers={"X-Objstore-Tenant": "rl-never"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_methods_not_allowed(self):
        for method, path in (("POST", "/v1/rate-limit"),
                             ("HEAD", "/v1/rate-limit"),
                             ("PUT", "/v1/rate-limit/usage"),
                             ("DELETE", "/v1/rate-limit/usage"),
                             ("POST", "/v1/rate-limit/usage")):
            status, _, _ = self.request(method, path,
                                        headers={"X-Objstore-Tenant": "rl-m"})
            self.assertEqual(status, 405, (method, path))

    def test_invalid_tenant_header(self):
        for method, path in (("GET", "/v1/rate-limit"),
                             ("GET", "/v1/rate-limit/usage")):
            status, _, body = self.request(
                method, path, headers={"X-Objstore-Tenant": "BAD!"})
            self.assertEqual(status, 400, (method, path))
            self.assertTrue(self.document(body)["error"].startswith(
                "invalid tenant"))

    def test_policy_persists_across_reopen(self):
        self.set_limit("rl-persist", window_seconds=60, max_requests=3)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_rate_limit(tenant="rl-persist"),
                         {"window_seconds": 60, "max_requests": 3,
                          "max_bytes": None})

    def test_io_error_returns_500(self):
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            headers = {"X-Objstore-Tenant": "rl-io"}
            status, _, body = self.request(
                "PUT", "/v1/rate-limit",
                json.dumps({"window_seconds": 60, "max_requests": 1}), headers)
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body),
                             {"error": "rate limit io error"})
            status, _, body = self.request("DELETE", "/v1/rate-limit",
                                           headers=headers)
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body),
                             {"error": "rate limit io error"})
        finally:
            os.rmdir(self.store.index_path)


class TestRateLimitEnforcement(HTTPRateLimitTestCase):
    def test_max_requests_returns_429_with_retry_after(self):
        tenant = "rl-max-req"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=2)
        status, _, _ = self.request("PUT", "/v1/objects/r1", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/r2", b"2", headers)
        self.assertEqual(status, 201)
        status, resp_headers, body = self.request("PUT", "/v1/objects/r3",
                                                  b"3", headers)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body), {"error": "rate limit exceeded"})
        retry_after = int(resp_headers["retry-after"])
        self.assertGreaterEqual(retry_after, 1)
        self.assertLessEqual(retry_after, 60)
        # The rejected write counted nothing.
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 2)
        # And it stored nothing: after clearing the policy the key is 404.
        status, _, _ = self.request("DELETE", "/v1/rate-limit",
                                    headers=headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request("GET", "/v1/objects/r3", headers=headers)
        self.assertEqual(status, 404)

    def test_max_bytes_counts_put_bodies(self):
        tenant = "rl-max-bytes"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_bytes=5)
        status, _, _ = self.request("PUT", "/v1/objects/b1", b"123", headers)
        self.assertEqual(status, 201)
        status, _, body = self.request("PUT", "/v1/objects/b2", b"123", headers)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body), {"error": "rate limit exceeded"})
        # Reads carry no body bytes and stay allowed.
        status, _, body = self.request("GET", "/v1/objects/b1", headers=headers)
        self.assertEqual((status, body), (200, b"123"))
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        usage = self.document(body)
        self.assertEqual(usage["bytes"], 3)
        self.assertEqual(usage["requests"], 2)

    def test_max_bytes_counts_upload_chunks(self):
        tenant = "rl-up-bytes"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_bytes=4)
        status, _, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "big", "size": 6, "sha256": sha(b"abcdef")}),
            headers)
        self.assertEqual(status, 201)
        session = self.document(body)["session"]
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session, b"ab", headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=2" % session, b"cd", headers)
        self.assertEqual(status, 200)
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=4" % session, b"ef", headers)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body), {"error": "rate limit exceeded"})
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["bytes"], 4)

    def test_management_requests_not_counted(self):
        tenant = "rl-mgmt"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        for _ in range(3):
            status, _, _ = self.request("GET", "/v1/rate-limit",
                                        headers=headers)
            self.assertEqual(status, 200)
            status, _, _ = self.request("GET", "/v1/rate-limit/usage",
                                        headers=headers)
            self.assertEqual(status, 200)
            status, _, _ = self.request("GET", "/healthz")
            self.assertEqual(status, 200)
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 0)

    def test_usage_fields(self):
        tenant = "rl-usage"
        headers = {"X-Objstore-Tenant": tenant}
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)
        self.set_limit(tenant, window_seconds=60, max_requests=10)
        self.request("PUT", "/v1/objects/u1", b"hello", headers)
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        usage = self.document(body)
        self.assertEqual(usage["requests"], 1)
        self.assertEqual(usage["bytes"], 5)
        self.assertEqual(usage["reset_at"] - usage["window_start"], 60)
        self.assertEqual(usage["window_start"] % 60, 0)

    def test_validation_failures_not_counted(self):
        tenant = "rl-novalid"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        # Invalid key, invalid precondition header, missing Content-Length.
        status, _, _ = self.request("PUT", "/v1/objects/a//b", b"1", headers)
        self.assertEqual(status, 400)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"1",
                                    dict(headers, **{"If-Match": "nope"}))
        self.assertEqual(status, 400)
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("PUT", "/v1/objects/k2")
            conn.putheader("X-Objstore-Tenant", tenant)
            conn.endheaders()
            response = conn.getresponse()
            response.read()
            self.assertEqual(response.status, 411)
        finally:
            conn.close()
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 0)

    def test_failed_precondition_not_counted(self):
        tenant = "rl-noprec"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"1", headers)
        self.assertEqual(status, 201)
        status, _, body = self.request(
            "PUT", "/v1/objects/k", b"2",
            dict(headers, **{"If-None-Match": "*"}))
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 1)

    def test_signed_read_without_signature_not_counted(self):
        tenant = "rl-signed"
        headers = {"X-Objstore-Tenant": tenant}
        status, _, _ = self.request(
            "PUT", "/v1/access-policy",
            json.dumps({"mode": "signed", "secret": "0123456789abcdef"}),
            headers)
        self.assertEqual(status, 200)
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"1", headers)
        self.assertEqual(status, 201)
        status, _, body = self.request("GET", "/v1/objects/k", headers=headers)
        self.assertEqual(status, 403)
        self.assertEqual(self.document(body), {"error": "access denied"})
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 1)

    def test_limit_is_tenant_scoped(self):
        tenant = "rl-scoped"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        status, _, _ = self.request("PUT", "/v1/objects/s1", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/s2", b"2", headers)
        self.assertEqual(status, 429)
        # The default tenant stays unlimited.
        status, _, _ = self.request("PUT", "/v1/objects/s1", b"1")
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/s2", b"2")
        self.assertEqual(status, 201)

    def test_clear_restores_unlimited(self):
        tenant = "rl-restore"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        self.request("PUT", "/v1/objects/k1", b"1", headers)
        status, _, _ = self.request("PUT", "/v1/objects/k2", b"2", headers)
        self.assertEqual(status, 429)
        status, _, _ = self.request("DELETE", "/v1/rate-limit", headers=headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request("PUT", "/v1/objects/k2", b"2", headers)
        self.assertEqual(status, 201)
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body), None)

    def test_blob_get_counts_against_default_tenant(self):
        tenant = "rl-blob"
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        payload = b"rl-blob-bytes"
        digest = sha(payload)
        status, _, _ = self.request("PUT", "/v1/objects/blobby", payload,
                                    {"X-Objstore-Tenant": tenant})
        self.assertEqual(status, 201)
        # The blob endpoint is global; the default tenant has no policy.
        status, _, body = self.request("GET", "/v1/blobs/%s" % digest)
        self.assertEqual((status, body), (200, payload))
        # But the default tenant's own policy applies to blob reads.
        self.set_limit("default", window_seconds=60, max_requests=1)
        status, _, _ = self.request("GET", "/v1/blobs/%s" % digest)
        self.assertEqual(status, 200)
        status, _, body = self.request("GET", "/v1/blobs/%s" % digest)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body), {"error": "rate limit exceeded"})
        status, _, _ = self.request("DELETE", "/v1/rate-limit",
                                    headers={"X-Objstore-Tenant": "default"})
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()

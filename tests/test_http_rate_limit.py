"""End-to-end tests for the rate-limit HTTP endpoints."""

import hashlib
import http.client
import json
import os
import shutil
import tempfile
import threading
import time
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
                                      kwargs={"poll_interval": 0.05},
                                      daemon=True)
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
            "GET", "/v1/rate-limit", headers={"X-Objstore-Tenant": "t-unset"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_put_and_get_roundtrip(self):
        headers = {"X-Objstore-Tenant": "t-round"}
        status, _, body = self.request(
            "PUT", "/v1/rate-limit",
            json.dumps({"window_seconds": 60, "max_requests": 10,
                        "max_bytes": 1000}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 1000})
        status, _, body = self.request("GET", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(self.document(body),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 1000})

    def test_put_single_limit(self):
        headers = {"X-Objstore-Tenant": "t-single"}
        status, _, body = self.request(
            "PUT", "/v1/rate-limit",
            json.dumps({"window_seconds": 30, "max_requests": 5}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"window_seconds": 30, "max_requests": 5,
                          "max_bytes": None})

    def test_put_rejects_invalid_policies(self):
        headers = {"X-Objstore-Tenant": "t-bad"}
        for document in ({}, {"max_requests": 1}, {"max_bytes": 1},
                         {"window_seconds": 60},
                         {"window_seconds": None, "max_requests": 1},
                         {"window_seconds": 0, "max_requests": 1},
                         {"window_seconds": -1, "max_requests": 1},
                         {"window_seconds": 86401, "max_requests": 1},
                         {"window_seconds": True, "max_requests": 1},
                         {"window_seconds": "60", "max_requests": 1},
                         {"window_seconds": 1.5, "max_requests": 1},
                         {"window_seconds": 60, "max_requests": -1},
                         {"window_seconds": 60, "max_bytes": -2},
                         {"window_seconds": 60, "max_requests": True},
                         {"window_seconds": 60, "max_bytes": "10"},
                         {"window_seconds": 60,
                          "max_requests": None, "max_bytes": None},
                         {"window_seconds": 60, "max_requests": 1,
                          "unknown": 2},
                         {"window_second": 60, "max_requests": 1}):
            status, _, body = self.request("PUT", "/v1/rate-limit",
                                           json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(body),
                             {"error": "invalid rate limit"})
        status, _, body = self.request("GET", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(self.document(body), None)

    def test_put_rejects_non_object_json(self):
        for body in ("[1, 2]", "not json", ""):
            status, _, payload = self.request(
                "PUT", "/v1/rate-limit", body,
                {"X-Objstore-Tenant": "t-badjson",
                 "Content-Length": str(len(body))})
            self.assertEqual(status, 400, body)
            self.assertEqual(self.document(payload),
                             {"error": "invalid rate limit"})

    def test_delete_returns_null(self):
        headers = {"X-Objstore-Tenant": "t-clear"}
        self.set_limit("t-clear", window_seconds=60, max_requests=5)
        status, _, body = self.request("DELETE", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)
        status, _, body = self.request("GET", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(self.document(body), None)
        # Clearing again is still a success.
        status, _, body = self.request("DELETE", "/v1/rate-limit",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_methods_not_allowed(self):
        for method, path in (("POST", "/v1/rate-limit"),
                             ("HEAD", "/v1/rate-limit"),
                             ("PUT", "/v1/rate-limit/usage"),
                             ("DELETE", "/v1/rate-limit/usage"),
                             ("POST", "/v1/rate-limit/usage")):
            status, _, _ = self.request(
                method, path, headers={"X-Objstore-Tenant": "t-methods"})
            self.assertEqual(status, 405, (method, path))

    def test_invalid_tenant_header(self):
        status, _, body = self.request(
            "GET", "/v1/rate-limit", headers={"X-Objstore-Tenant": "BAD!"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith(
            "invalid tenant"))

    def test_policy_survives_reopen(self):
        self.set_limit("t-reopen", window_seconds=60, max_requests=7)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_rate_limit(tenant="t-reopen"),
                         {"window_seconds": 60, "max_requests": 7,
                          "max_bytes": None})

    def test_rate_limit_io_error_returns_500(self):
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            status, _, body = self.request(
                "PUT", "/v1/rate-limit",
                json.dumps({"window_seconds": 60, "max_requests": 1}),
                {"X-Objstore-Tenant": "t-io"})
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body),
                             {"error": "rate limit io error"})
        finally:
            os.rmdir(self.store.index_path)


class TestRateLimitUsageEndpoint(HTTPRateLimitTestCase):
    def test_usage_defaults_to_null(self):
        status, _, body = self.request(
            "GET", "/v1/rate-limit/usage",
            headers={"X-Objstore-Tenant": "t-usage-unset"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_usage_fields(self):
        tenant = "t-usage"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=100)
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(status, 200)
        usage = self.document(body)
        now = int(time.time())
        self.assertEqual(usage, {"window_start": now - (now % 60),
                                 "reset_at": now - (now % 60) + 60,
                                 "requests": 0, "bytes": 0})
        self.request("PUT", "/v1/objects/u1", b"hello", headers)
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        usage = self.document(body)
        self.assertEqual(usage["requests"], 1)
        self.assertEqual(usage["bytes"], 5)


class TestRateLimitEnforcement(HTTPRateLimitTestCase):
    def test_exceeded_returns_429_with_retry_after(self):
        tenant = "t-limited"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=2)
        for key in ("a", "b"):
            status, _, _ = self.request("PUT", "/v1/objects/" + key, b"1",
                                        headers)
            self.assertEqual(status, 201)
        status, resp_headers, body = self.request("PUT", "/v1/objects/c",
                                                  b"1", headers)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body),
                         {"error": "rate limit exceeded"})
        retry_after = int(resp_headers["retry-after"])
        self.assertGreaterEqual(retry_after, 1)
        self.assertLessEqual(retry_after, 60)
        # The rejected request was not charged and stored nothing.
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        self.assertEqual(self.document(body)["requests"], 2)
        reopened = ContentAddressedStore(self.root)
        self.assertNotIn("c", reopened._objects.get(tenant, {}))

    def test_healthz_not_counted(self):
        tenant = "t-healthz"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        for _ in range(3):
            status, _, _ = self.request("GET", "/healthz")
            self.assertEqual(status, 200)
        status, _, _ = self.request("PUT", "/v1/objects/h", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/h2", b"2", headers)
        self.assertEqual(status, 429)

    def test_management_requests_not_counted(self):
        tenant = "t-mgmt"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        for _ in range(3):
            status, _, _ = self.request("GET", "/v1/rate-limit",
                                        headers=headers)
            self.assertEqual(status, 200)
            status, _, _ = self.request("GET", "/v1/rate-limit/usage",
                                        headers=headers)
            self.assertEqual(status, 200)
        status, _, _ = self.request("PUT", "/v1/objects/m", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/m2", b"2", headers)
        self.assertEqual(status, 429)
        # Management endpoints still answer with the window exhausted.
        status, _, _ = self.request("GET", "/v1/rate-limit", headers=headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request("DELETE", "/v1/rate-limit",
                                    headers=headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request("PUT", "/v1/objects/m2", b"2", headers)
        self.assertEqual(status, 201)

    def test_max_bytes_counts_put_and_chunk_bodies_only(self):
        tenant = "t-bytes"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_bytes=4)
        status, _, _ = self.request("PUT", "/v1/objects/b1", b"123", headers)
        self.assertEqual(status, 201)
        # The session declaration is metadata JSON and counts no bytes.
        status, _, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "big", "size": 4, "sha256": sha(b"1234")}),
            headers)
        self.assertEqual(status, 201)
        session = self.document(body)["session"]
        # Three put bytes plus a two-byte chunk exceed the budget of four.
        status, _, body = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session, b"12", headers)
        self.assertEqual(status, 429)
        self.assertEqual(self.document(body),
                         {"error": "rate limit exceeded"})
        status, _, body = self.request("GET", "/v1/rate-limit/usage",
                                       headers=headers)
        usage = self.document(body)
        self.assertEqual(usage["bytes"], 3)
        self.assertEqual(usage["requests"], 2)

    def test_validation_failures_not_counted(self):
        tenant = "t-invalid"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        # A malformed condition header keeps its own error and no charge.
        status, _, body = self.request("PUT", "/v1/objects/v", b"1",
                                       dict(headers, **{"If-Match": "junk"}))
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body),
                         {"error": "invalid precondition"})
        # An invalid tenant header is rejected before any charging.
        status, _, _ = self.request("PUT", "/v1/objects/v", b"1",
                                    {"X-Objstore-Tenant": "BAD!"})
        self.assertEqual(status, 400)
        # The first valid request still fits the window.
        status, _, _ = self.request("PUT", "/v1/objects/v", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/v2", b"2", headers)
        self.assertEqual(status, 429)

    def test_access_denied_not_counted(self):
        tenant = "t-signed"
        headers = {"X-Objstore-Tenant": tenant}
        self.store.set_access_policy("signed", secret="0123456789abcdef",
                                     tenant=tenant)
        self.store.put("doc", b"data", tenant=tenant)
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        status, _, body = self.request("GET", "/v1/objects/doc",
                                       headers=headers)
        self.assertEqual(status, 403)
        self.assertEqual(self.document(body), {"error": "access denied"})
        expires = int(time.time()) + 60
        signature = self.store.sign_request("GET", "doc", expires=expires,
                                            tenant=tenant)
        path = "/v1/objects/doc?expires=%d&signature=%s" % (expires, signature)
        status, _, _ = self.request("GET", path, headers=headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request("GET", path, headers=headers)
        self.assertEqual(status, 429)

    def test_limit_is_tenant_scoped(self):
        tenant = "t-scoped-rl"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_limit(tenant, window_seconds=60, max_requests=1)
        status, _, _ = self.request("PUT", "/v1/objects/s1", b"1", headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/s2", b"2", headers)
        self.assertEqual(status, 429)
        # Other tenants and the default tenant stay unlimited.
        status, _, _ = self.request("PUT", "/v1/objects/s2", b"2",
                                    {"X-Objstore-Tenant": "t-other-rl"})
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/s3", b"3")
        self.assertEqual(status, 201)


if __name__ == "__main__":
    unittest.main()

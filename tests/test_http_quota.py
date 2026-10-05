"""End-to-end tests for the quota and usage HTTP endpoints."""

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


TENANT = {"X-Objstore-Tenant": "acme"}


class HTTPQuotaTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-quota-")
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

    def set_quota(self, tenant, **policy):
        status, _, body = self.request(
            "PUT", "/v1/quota", json.dumps(policy),
            {"X-Objstore-Tenant": tenant})
        assert status == 200, body


class TestQuotaEndpoint(HTTPQuotaTestCase):
    def test_get_defaults_to_nulls(self):
        status, _, body = self.request(
            "GET", "/v1/quota", headers={"X-Objstore-Tenant": "t-unset"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_bytes": None, "max_objects": None})

    def test_put_and_get_roundtrip(self):
        headers = {"X-Objstore-Tenant": "t-round"}
        status, _, body = self.request("PUT", "/v1/quota",
                                       json.dumps({"max_bytes": 100,
                                                   "max_objects": 3}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_bytes": 100, "max_objects": 3})
        status, _, body = self.request("GET", "/v1/quota", headers=headers)
        self.assertEqual(self.document(body),
                         {"max_bytes": 100, "max_objects": 3})

    def test_put_single_limit(self):
        headers = {"X-Objstore-Tenant": "t-single"}
        status, _, body = self.request("PUT", "/v1/quota",
                                       json.dumps({"max_bytes": 64}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_bytes": 64, "max_objects": None})

    def test_put_rejects_invalid_policies(self):
        headers = {"X-Objstore-Tenant": "t-bad"}
        for document in ({}, {"max_bytes": None, "max_objects": None},
                         {"max_bytes": -1}, {"max_objects": -1},
                         {"max_bytes": True}, {"max_objects": False},
                         {"max_bytes": "100"}, {"max_objects": 1.5},
                         {"max_bytes": 1, "unknown": 2}, {"max_byte": 1}):
            status, _, body = self.request("PUT", "/v1/quota",
                                           json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(body), {"error": "invalid quota"})

    def test_put_rejects_non_object_json(self):
        status, _, body = self.request("PUT", "/v1/quota", "[1, 2]",
                                       {"X-Objstore-Tenant": "t-badjson"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))

    def test_delete_clears_policy(self):
        headers = {"X-Objstore-Tenant": "t-clear"}
        self.request("PUT", "/v1/quota", json.dumps({"max_bytes": 10}), headers)
        status, _, body = self.request("DELETE", "/v1/quota", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_bytes": None, "max_objects": None})
        status, _, body = self.request("GET", "/v1/quota", headers=headers)
        self.assertEqual(self.document(body),
                         {"max_bytes": None, "max_objects": None})

    def test_methods_not_allowed(self):
        for method, path in (("POST", "/v1/quota"), ("HEAD", "/v1/quota"),
                             ("PUT", "/v1/usage"), ("DELETE", "/v1/usage"),
                             ("POST", "/v1/usage")):
            status, _, _ = self.request(method, path, headers=TENANT)
            self.assertEqual(status, 405, (method, path))

    def test_invalid_tenant_header(self):
        status, _, body = self.request(
            "GET", "/v1/quota", headers={"X-Objstore-Tenant": "BAD!"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))

    def test_quota_is_tenant_scoped(self):
        self.set_quota("t-scoped", max_objects=1)
        status, _, _ = self.request(
            "PUT", "/v1/objects/scoped-a", b"1",
            {"X-Objstore-Tenant": "t-scoped"})
        self.assertEqual(status, 201)
        status, _, body = self.request(
            "PUT", "/v1/objects/scoped-b", b"2",
            {"X-Objstore-Tenant": "t-scoped"})
        self.assertEqual(status, 413)
        self.assertEqual(self.document(body), {"error": "quota exceeded"})
        # The default tenant stays unlimited.
        status, _, _ = self.request("PUT", "/v1/objects/scoped-a", b"1")
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/scoped-b", b"2")
        self.assertEqual(status, 201)


class TestUsageEndpoint(HTTPQuotaTestCase):
    def test_usage_fields(self):
        tenant = "t-usage"
        headers = {"X-Objstore-Tenant": tenant}
        status, _, body = self.request("GET", "/v1/usage", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"used_bytes": 0, "used_objects": 0,
                          "reserved_bytes": 0, "reserved_sessions": 0})
        self.request("PUT", "/v1/objects/u1", b"hello", headers)
        self.request("PUT", "/v1/objects/u2", b"hello", headers)
        status, _, body = self.request("GET", "/v1/usage", headers=headers)
        self.assertEqual(self.document(body),
                         {"used_bytes": 10, "used_objects": 2,
                          "reserved_bytes": 0, "reserved_sessions": 0})

    def test_usage_counts_active_sessions(self):
        tenant = "t-usage-upload"
        headers = {"X-Objstore-Tenant": tenant}
        status, _, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "big", "size": 100, "sha256": sha(b"x" * 100)}),
            headers)
        self.assertEqual(status, 201)
        session = self.document(body)["session"]
        status, _, body = self.request("GET", "/v1/usage", headers=headers)
        self.assertEqual(self.document(body),
                         {"used_bytes": 0, "used_objects": 0,
                          "reserved_bytes": 100, "reserved_sessions": 1})
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session, b"x" * 100, headers)
        self.assertEqual(status, 200)
        status, _, _ = self.request(
            "POST", "/v1/uploads/%s/complete" % session, headers=headers)
        self.assertEqual(status, 200)
        status, _, body = self.request("GET", "/v1/usage", headers=headers)
        self.assertEqual(self.document(body),
                         {"used_bytes": 100, "used_objects": 1,
                          "reserved_bytes": 0, "reserved_sessions": 0})


class TestQuotaEnforcement(HTTPQuotaTestCase):
    def test_put_object_over_quota_returns_413(self):
        tenant = "t-obj-quota"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_quota(tenant, max_bytes=10)
        status, _, _ = self.request("PUT", "/v1/objects/k1", b"123456", headers)
        self.assertEqual(status, 201)
        status, _, body = self.request("PUT", "/v1/objects/k2", b"12345",
                                       headers)
        self.assertEqual(status, 413)
        self.assertEqual(self.document(body), {"error": "quota exceeded"})
        status, _, _ = self.request("GET", "/v1/objects/k2", headers=headers)
        self.assertEqual(status, 404)

    def test_begin_upload_over_quota_returns_413(self):
        tenant = "t-up-quota"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_quota(tenant, max_bytes=10)
        self.request("PUT", "/v1/objects/k1", b"123456", headers)
        status, _, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "big", "size": 5, "sha256": sha(b"x" * 5)}),
            headers)
        self.assertEqual(status, 413)
        self.assertEqual(self.document(body), {"error": "quota exceeded"})

    def test_complete_over_object_quota_returns_413(self):
        tenant = "t-complete-quota"
        headers = {"X-Objstore-Tenant": tenant}
        self.set_quota(tenant, max_objects=1)
        self.request("PUT", "/v1/objects/k1", b"1", headers)
        status, _, body = self.request(
            "POST", "/v1/uploads",
            json.dumps({"key": "k2", "size": 2, "sha256": sha(b"bb")}), headers)
        self.assertEqual(status, 201)
        session = self.document(body)["session"]
        self.request("PUT", "/v1/uploads/%s?offset=0" % session, b"bb", headers)
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session, headers=headers)
        self.assertEqual(status, 413)
        self.assertEqual(self.document(body), {"error": "quota exceeded"})
        status, _, _ = self.request("GET", "/v1/objects/k2", headers=headers)
        self.assertEqual(status, 404)

    def test_quota_io_error_returns_500(self):
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            status, _, body = self.request(
                "PUT", "/v1/quota", json.dumps({"max_bytes": 1}),
                {"X-Objstore-Tenant": "t-io"})
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body), {"error": "quota io error"})
        finally:
            os.rmdir(self.store.index_path)

    def test_etag_and_condition_semantics_unchanged(self):
        tenant = "t-compat"
        headers = {"X-Objstore-Tenant": tenant}
        status, resp_headers, body = self.request(
            "PUT", "/v1/objects/c1", b"data", headers)
        self.assertEqual(status, 201)
        entry = self.document(body)
        status, resp_headers, _ = self.request("GET", "/v1/objects/c1",
                                               headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(resp_headers.get("etag"), '"%s"' % entry["sha256"])
        status, _, body = self.request(
            "PUT", "/v1/objects/c1", b"other",
            dict(headers, **{"If-None-Match": "*"}))
        self.assertEqual(status, 412)
        self.assertEqual(self.document(body), {"error": "precondition failed"})


if __name__ == "__main__":
    unittest.main()

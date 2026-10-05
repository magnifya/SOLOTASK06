"""End-to-end tests for the lifecycle HTTP endpoints."""

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


class HTTPLifecycleTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-life-")
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

    def put_policy(self, tenant, document):
        return self.request("PUT", "/v1/lifecycle", json.dumps(document),
                            {"X-Objstore-Tenant": tenant})

    def backdate(self, key, tenant="acme", seconds=3600):
        from datetime import datetime, timedelta, timezone
        old = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
        self.store._timestamps.setdefault(tenant, {})[key] = old


class TestLifecycleEndpoint(HTTPLifecycleTestCase):
    def test_get_without_policy_returns_null(self):
        status, _, body = self.request(
            "GET", "/v1/lifecycle", headers={"X-Objstore-Tenant": "t-null"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_put_and_get_roundtrip(self):
        headers = {"X-Objstore-Tenant": "t-set"}
        status, _, body = self.put_policy(
            "t-set", {"prefix": "tmp/", "max_age_seconds": 100})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"prefix": "tmp/", "max_age_seconds": 100})
        status, _, body = self.request("GET", "/v1/lifecycle", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"prefix": "tmp/", "max_age_seconds": 100})

    def test_put_without_prefix_defaults_to_empty_string(self):
        status, _, body = self.put_policy("t-prefix", {"max_age_seconds": 0})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"prefix": "", "max_age_seconds": 0})

    def test_put_rejects_invalid_policies(self):
        headers = {"X-Objstore-Tenant": "t-bad"}
        for document in (
                {},
                {"prefix": "tmp/"},
                {"max_age_seconds": -1},
                {"max_age_seconds": True},
                {"max_age_seconds": False},
                {"max_age_seconds": "60"},
                {"max_age_seconds": 1.5},
                {"prefix": 5, "max_age_seconds": 1},
                {"prefix": None, "max_age_seconds": 1},
        ):
            status, _, body = self.request(
                "PUT", "/v1/lifecycle", json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(body), {"error": "invalid lifecycle"})

    def test_put_rejects_unknown_fields(self):
        for document in ({"max_age_seconds": 1, "max_age": 2},
                         {"prefix": "", "max_age_seconds": 1, "extra": 0}):
            status, _, body = self.request(
                "PUT", "/v1/lifecycle", json.dumps(document),
                {"X-Objstore-Tenant": "t-unknown"})
            self.assertEqual(status, 400, document)
            self.assertTrue(
                self.document(body)["error"].startswith("invalid json"))

    def test_put_rejects_non_object_json(self):
        status, _, body = self.request(
            "PUT", "/v1/lifecycle", "[1, 2]",
            {"X-Objstore-Tenant": "t-badjson"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))

    def test_delete_clears_policy_and_returns_null(self):
        headers = {"X-Objstore-Tenant": "t-clear"}
        self.put_policy("t-clear", {"max_age_seconds": 10})
        status, _, body = self.request("DELETE", "/v1/lifecycle",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)
        status, _, body = self.request("GET", "/v1/lifecycle", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_delete_without_policy_is_idempotent(self):
        status, _, body = self.request(
            "DELETE", "/v1/lifecycle",
            headers={"X-Objstore-Tenant": "t-clear-none"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), None)

    def test_methods_not_allowed(self):
        for method, path in (("POST", "/v1/lifecycle"),
                             ("HEAD", "/v1/lifecycle"),
                             ("GET", "/v1/lifecycle/run"),
                             ("PUT", "/v1/lifecycle/run"),
                             ("DELETE", "/v1/lifecycle/run")):
            status, _, _ = self.request(method, path, headers=TENANT)
            self.assertEqual(status, 405, (method, path))

    def test_invalid_tenant_header(self):
        for method, path in (("GET", "/v1/lifecycle"),
                             ("PUT", "/v1/lifecycle"),
                             ("DELETE", "/v1/lifecycle"),
                             ("POST", "/v1/lifecycle/run")):
            status, _, body = self.request(
                method, path,
                body=json.dumps({"max_age_seconds": 1}) if method == "PUT"
                else None,
                headers={"X-Objstore-Tenant": "BAD!"})
            self.assertEqual(status, 400, (method, path))
            self.assertTrue(
                self.document(body)["error"].startswith("invalid tenant"))


class TestRunLifecycleEndpoint(HTTPLifecycleTestCase):
    def test_preview_default_with_no_body(self):
        tenant = "t-run-default"
        headers = {"X-Objstore-Tenant": tenant}
        self.put_policy(tenant, {"max_age_seconds": 60})
        self.request("PUT", "/v1/objects/a", b"1", headers)
        self.backdate("a", tenant=tenant)
        status, _, body = self.request("POST", "/v1/lifecycle/run",
                                       headers=headers)
        self.assertEqual(status, 200)
        result = self.document(body)
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["objects"],
                         [{"key": "a", "sha256": sha(b"1"), "size": 1}])
        self.assertEqual(result["bytes"], 1)
        # Default is a preview: the object still exists.
        status, _, _ = self.request("GET", "/v1/objects/a", headers=headers)
        self.assertEqual(status, 200)

    def test_explicit_dry_run_false_deletes(self):
        tenant = "t-run-execute"
        headers = {"X-Objstore-Tenant": tenant}
        self.put_policy(tenant, {"prefix": "tmp/", "max_age_seconds": 60})
        self.request("PUT", "/v1/objects/tmp/a", b"ab", headers)
        self.request("PUT", "/v1/objects/keep", b"cd", headers)
        self.backdate("tmp/a", tenant=tenant)
        status, _, body = self.request(
            "POST", "/v1/lifecycle/run", json.dumps({"dry_run": False}),
            headers)
        self.assertEqual(status, 200)
        result = self.document(body)
        self.assertFalse(result["dry_run"])
        self.assertEqual([o["key"] for o in result["objects"]], ["tmp/a"])
        self.assertEqual(result["bytes"], 2)
        status, _, _ = self.request("GET", "/v1/objects/tmp/a", headers=headers)
        self.assertEqual(status, 404)
        status, _, _ = self.request("GET", "/v1/objects/keep", headers=headers)
        self.assertEqual(status, 200)

    def test_run_without_policy_is_empty(self):
        headers = {"X-Objstore-Tenant": "t-run-none"}
        self.request("PUT", "/v1/objects/a", b"x", headers)
        status, _, body = self.request("POST", "/v1/lifecycle/run",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"objects": [], "bytes": 0, "dry_run": True})

    def test_run_rejects_unknown_fields(self):
        headers = {"X-Objstore-Tenant": "t-run-bad"}
        for document in ({"dry_run": False, "execute": True},
                         {"prefix": "x"}):
            status, _, body = self.request(
                "POST", "/v1/lifecycle/run", json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertTrue(
                self.document(body)["error"].startswith("invalid json"))

    def test_run_rejects_invalid_dry_run(self):
        headers = {"X-Objstore-Tenant": "t-run-dry"}
        self.put_policy("t-run-dry", {"max_age_seconds": 1})
        for value in ("yes", 1, 0, "false"):
            status, _, body = self.request(
                "POST", "/v1/lifecycle/run", json.dumps({"dry_run": value}),
                headers)
            self.assertEqual(status, 400, value)
            self.assertEqual(self.document(body), {"error": "invalid dry_run"})

    def test_run_rejects_non_object_json(self):
        status, _, body = self.request(
            "POST", "/v1/lifecycle/run", b"[1]",
            {"X-Objstore-Tenant": "t-run-json"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))

    def test_versioned_deletion_promotes_over_http(self):
        tenant = "t-run-ver"
        headers = {"X-Objstore-Tenant": tenant}
        self.request("PUT", "/v1/versioning", json.dumps({"enabled": True}),
                    headers)
        self.request("PUT", "/v1/objects/v", b"one", headers)
        self.request("PUT", "/v1/objects/v", b"two-longer", headers)
        self.put_policy(tenant, {"max_age_seconds": 0})
        status, _, body = self.request(
            "POST", "/v1/lifecycle/run", json.dumps({"dry_run": False}),
            headers)
        self.assertEqual(status, 200)
        result = self.document(body)
        self.assertEqual(result["objects"],
                         [{"key": "v", "sha256": sha(b"two-longer"),
                           "size": 10}])
        status, _, body = self.request("GET", "/v1/objects/v", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"one")

    def test_policy_persists_across_server_state(self):
        tenant = "t-run-persist"
        self.put_policy(tenant, {"max_age_seconds": 7})
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_lifecycle(tenant=tenant),
                         {"prefix": "", "max_age_seconds": 7})

    def test_lifecycle_io_error_returns_500(self):
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            status, _, body = self.request(
                "PUT", "/v1/lifecycle", json.dumps({"max_age_seconds": 1}),
                {"X-Objstore-Tenant": "t-run-io"})
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body), {"error": "lifecycle io error"})
            status, _, body = self.request(
                "DELETE", "/v1/lifecycle",
                headers={"X-Objstore-Tenant": "t-run-io"})
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body), {"error": "lifecycle io error"})
        finally:
            os.rmdir(self.store.index_path)


if __name__ == "__main__":
    unittest.main()

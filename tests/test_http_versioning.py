"""End-to-end tests for the versioning and retention HTTP endpoints."""

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


class HTTPVersioningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-versioning-")
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

    def enable(self, tenant=None):
        headers = {"X-Objstore-Tenant": tenant} if tenant else {}
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": True}), headers)
        self.assertEqual(status, 200, body)

    def put(self, key, payload, tenant=None):
        headers = {"X-Objstore-Tenant": tenant} if tenant else {}
        status, _, body = self.request("PUT", "/v1/objects/" + key, payload, headers)
        self.assertEqual(status, 201, body)
        return self.document(body)


class TestVersioningEndpoint(HTTPVersioningTestCase):
    def test_get_defaults_to_disabled(self):
        status, _, body = self.request("GET", "/v1/versioning")
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"enabled": False})

    def test_put_toggles_and_get_reflects(self):
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": True}))
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"enabled": True})
        status, _, body = self.request("GET", "/v1/versioning")
        self.assertEqual(self.document(body), {"enabled": True})
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": False}))
        self.assertEqual(self.document(body), {"enabled": False})

    def test_invalid_toggle_values(self):
        for body in (json.dumps({"enabled": "yes"}), json.dumps({"enabled": 1}),
                     json.dumps({"enabled": None})):
            status, _, payload = self.request("PUT", "/v1/versioning", body)
            self.assertEqual(status, 400, payload)
            self.assertEqual(self.document(payload), {"error": "invalid versioning"})

    def test_bad_bodies(self):
        status, _, body = self.request("PUT", "/v1/versioning", b"{not json")
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": True, "x": 1}))
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))
        status, _, body = self.request("PUT", "/v1/versioning", json.dumps({}))
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))

    def test_method_not_allowed(self):
        status, _, _ = self.request("DELETE", "/v1/versioning")
        self.assertEqual(status, 405)

    def test_tenant_header_selects_scope(self):
        self.enable("acme-http")
        status, _, body = self.request("GET", "/v1/versioning",
                                       headers={"X-Objstore-Tenant": "acme-http"})
        self.assertEqual(self.document(body), {"enabled": True})
        status, _, body = self.request("GET", "/v1/versioning")
        self.assertEqual(self.document(body), {"enabled": False})


class TestVersionedObjectHTTP(HTTPVersioningTestCase):
    def test_put_reports_version_id_only_when_enabled(self):
        self.enable()
        doc = self.put("http/k1", b"v1")
        self.assertIn("version_id", doc)
        self.assertEqual(doc["sha256"], sha(b"v1"))
        # a tenant without versioning keeps the old response shape
        doc = self.put("http/k2", b"v1", tenant="plain-tenant")
        self.assertEqual(doc, {"key": "http/k2", "sha256": sha(b"v1"), "size": 2})

    def test_list_versions(self):
        self.enable()
        first = self.put("http/list", b"v1")["version_id"]
        second = self.put("http/list", b"v2")["version_id"]
        status, _, body = self.request("GET", "/v1/objects/http/list/versions")
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [second, first])
        self.assertEqual([item["current"] for item in page["items"]],
                         [True, False])
        self.assertEqual(page["items"][0]["sha256"], sha(b"v2"))
        self.assertEqual(page["items"][0]["size"], 2)
        self.assertIn("created_at", page["items"][0])
        self.assertIsNone(page["next_after"])

    def test_list_versions_pagination(self):
        self.enable()
        vids = [self.put("http/paged", b"v%d" % i)["version_id"] for i in range(3)]
        status, _, body = self.request(
            "GET", "/v1/objects/http/paged/versions?limit=2")
        page = self.document(body)
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[2], vids[1]])
        self.assertEqual(page["next_after"], vids[1])
        status, _, body = self.request(
            "GET", "/v1/objects/http/paged/versions?limit=2&after=" + vids[1])
        page = self.document(body)
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[0]])
        self.assertIsNone(page["next_after"])

    def test_list_versions_requires_enabled(self):
        self.put("http/off", b"v1", tenant="off-tenant")
        status, _, body = self.request(
            "GET", "/v1/objects/http/off/versions",
            headers={"X-Objstore-Tenant": "off-tenant"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "versioning disabled"})

    def test_get_version(self):
        self.enable()
        first = self.put("http/getv", b"v1")["version_id"]
        self.put("http/getv", b"v2")
        status, headers, body = self.request(
            "GET", "/v1/objects/http/getv/versions/" + first)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"v1")
        self.assertEqual(headers["x-content-sha256"], sha(b"v1"))
        self.assertEqual(headers["etag"], '"%s"' % sha(b"v1"))

    def test_get_version_errors(self):
        self.enable()
        self.put("http/geterr", b"v1")
        status, _, body = self.request(
            "GET", "/v1/objects/http/geterr/versions/not-a-version")
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid version"})
        status, _, body = self.request(
            "GET", "/v1/objects/http/geterr/versions/" + "0" * 32)
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: version " + "0" * 32})
        status, _, body = self.request(
            "GET", "/v1/objects/http/missing/versions/" + "0" * 32)
        self.assertEqual(status, 404)

    def test_delete_version_promotes_and_removes(self):
        self.enable()
        first = self.put("http/delv", b"v1")["version_id"]
        second = self.put("http/delv", b"v2")["version_id"]
        status, _, _ = self.request(
            "DELETE", "/v1/objects/http/delv/versions/" + second)
        self.assertEqual(status, 204)
        status, _, body = self.request("GET", "/v1/objects/http/delv")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"v1")
        status, _, _ = self.request(
            "DELETE", "/v1/objects/http/delv/versions/" + first)
        self.assertEqual(status, 204)
        status, _, body = self.request("GET", "/v1/objects/http/delv")
        self.assertEqual(status, 404)

    def test_object_delete_keeps_history_readable(self):
        self.enable()
        first = self.put("http/delk", b"v1")["version_id"]
        self.put("http/delk", b"v2")
        status, _, _ = self.request("DELETE", "/v1/objects/http/delk")
        self.assertEqual(status, 204)
        status, _, body = self.request("GET", "/v1/objects/http/delk")
        self.assertEqual(status, 404)
        status, _, body = self.request(
            "GET", "/v1/objects/http/delk/versions/" + first)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"v1")

    def test_version_methods_not_allowed(self):
        self.enable()
        vid = self.put("http/methods", b"v1")["version_id"]
        status, _, _ = self.request("PUT", "/v1/objects/http/methods/versions", b"x")
        self.assertEqual(status, 405)
        status, _, _ = self.request(
            "POST", "/v1/objects/http/methods/versions/" + vid, b"")
        self.assertEqual(status, 405)

    def test_version_tenant_isolation(self):
        self.enable()
        self.enable("acme-v")
        doc = self.put("http/iso", b"acme", tenant="acme-v")
        status, _, body = self.request(
            "GET", "/v1/objects/http/iso/versions/" + doc["version_id"],
            headers={"X-Objstore-Tenant": "acme-v"})
        self.assertEqual(status, 200)
        status, _, body = self.request(
            "GET", "/v1/objects/http/iso/versions/" + doc["version_id"])
        self.assertEqual(status, 404)


class TestRetentionEndpoint(HTTPVersioningTestCase):
    def test_get_defaults_to_nulls(self):
        status, _, body = self.request("GET", "/v1/retention",
                                       headers={"X-Objstore-Tenant": "ret-none"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_versions": None, "max_age_seconds": None})

    def test_put_and_get_policy(self):
        status, _, body = self.request(
            "PUT", "/v1/retention", json.dumps({"max_versions": 2}),
            {"X-Objstore-Tenant": "ret-a"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_versions": 2, "max_age_seconds": None})
        status, _, body = self.request(
            "PUT", "/v1/retention",
            json.dumps({"max_versions": 2, "max_age_seconds": 30}),
            {"X-Objstore-Tenant": "ret-a"})
        self.assertEqual(self.document(body),
                         {"max_versions": 2, "max_age_seconds": 30})
        status, _, body = self.request("GET", "/v1/retention",
                                       headers={"X-Objstore-Tenant": "ret-a"})
        self.assertEqual(self.document(body),
                         {"max_versions": 2, "max_age_seconds": 30})

    def test_invalid_policies(self):
        for doc in ({}, {"max_versions": -1}, {"max_age_seconds": "x"},
                    {"max_versions": True}, {"max_versions": 1.5}):
            status, _, body = self.request("PUT", "/v1/retention", json.dumps(doc))
            self.assertEqual(status, 400, body)
            self.assertEqual(self.document(body), {"error": "invalid retention"})
        status, _, body = self.request(
            "PUT", "/v1/retention", json.dumps({"max_versions": 1, "x": 2}))
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid json"))

    def test_retention_method_not_allowed(self):
        status, _, _ = self.request("DELETE", "/v1/retention")
        self.assertEqual(status, 405)
        status, _, _ = self.request("GET", "/v1/retention/purge")
        self.assertEqual(status, 405)

    def test_purge_requires_policy(self):
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": True}),
            {"X-Objstore-Tenant": "ret-unset"})
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body),
                         {"error": "retention not configured"})

    def test_purge_rejects_non_boolean_dry_run(self):
        self.request("PUT", "/v1/retention", json.dumps({"max_versions": 1}),
                     {"X-Objstore-Tenant": "ret-dry"})
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": "yes"}),
            {"X-Objstore-Tenant": "ret-dry"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid dry_run"})

    def test_purge_dry_run_and_execute(self):
        tenant = "ret-purge"
        self.enable(tenant)
        headers = {"X-Objstore-Tenant": tenant}
        vids = [self.put("http/purge", b"v%d" % i, tenant=tenant)["version_id"]
                for i in range(3)]
        self.request("PUT", "/v1/retention", json.dumps({"max_versions": 1}),
                     headers)
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": True}), headers)
        self.assertEqual(status, 200)
        preview = self.document(body)
        self.assertEqual(preview, {"versions": [
            {"key": "http/purge", "version_id": vid}
            for vid in sorted([vids[0], vids[1]])], "bytes": 4, "dry_run": True})
        # the preview changed nothing
        status, _, body = self.request("GET", "/v1/objects/http/purge/versions",
                                       headers=headers)
        self.assertEqual(len(self.document(body)["items"]), 3)
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": False}), headers)
        result = self.document(body)
        self.assertEqual(result, {"versions": preview["versions"], "bytes": 4,
                                  "dry_run": False})
        status, _, body = self.request("GET", "/v1/objects/http/purge/versions",
                                       headers=headers)
        page = self.document(body)
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[2]])
        # the current object is untouched
        status, _, body = self.request("GET", "/v1/objects/http/purge",
                                       headers=headers)
        self.assertEqual(body, b"v2")

    def test_purge_without_body_defaults_to_dry_run(self):
        tenant = "ret-nobody"
        self.enable(tenant)
        headers = {"X-Objstore-Tenant": tenant}
        self.put("http/nobody", b"v1", tenant=tenant)
        self.put("http/nobody", b"v2", tenant=tenant)
        self.request("PUT", "/v1/retention", json.dumps({"max_versions": 1}),
                     headers)
        status, _, body = self.request("POST", "/v1/retention/purge",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertIs(self.document(body)["dry_run"], True)
        status, _, body = self.request("GET", "/v1/objects/http/nobody/versions",
                                       headers=headers)
        self.assertEqual(len(self.document(body)["items"]), 2)


if __name__ == "__main__":
    unittest.main()

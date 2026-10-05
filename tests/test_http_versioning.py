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


TENANT = {"X-Objstore-Tenant": "acme"}


class HTTPVersioningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-ver-")
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

    def enable(self, tenant="acme"):
        status, _, body = self.request(
            "PUT", "/v1/versioning", json.dumps({"enabled": True}),
            dict(TENANT, **{"Content-Type": "application/json"})
            if tenant == "acme" else
            {"X-Objstore-Tenant": tenant, "Content-Type": "application/json"})
        assert status == 200, body


class TestVersioningEndpoint(HTTPVersioningTestCase):
    def test_get_defaults_to_disabled(self):
        status, _, body = self.request("GET", "/v1/versioning",
                                       headers={"X-Objstore-Tenant": "t-gl"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"enabled": False})

    def test_put_toggles_and_get_reflects(self):
        headers = {"X-Objstore-Tenant": "t-toggle"}
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": True}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"enabled": True})
        status, _, body = self.request("GET", "/v1/versioning", headers=headers)
        self.assertEqual(self.document(body), {"enabled": True})
        status, _, body = self.request("PUT", "/v1/versioning",
                                       json.dumps({"enabled": False}), headers)
        self.assertEqual(status, 200)
        status, _, body = self.request("GET", "/v1/versioning", headers=headers)
        self.assertEqual(self.document(body), {"enabled": False})

    def test_invalid_toggle(self):
        for body in (json.dumps({"enabled": "yes"}), json.dumps({"enabled": 1}),
                     json.dumps({})):
            status, _, payload = self.request(
                "PUT", "/v1/versioning", body,
                {"X-Objstore-Tenant": "t-bad"})
            self.assertEqual(status, 400)
            self.assertEqual(self.document(payload), {"error": "invalid versioning"})

    def test_method_not_allowed(self):
        status, _, _ = self.request("DELETE", "/v1/versioning")
        self.assertEqual(status, 405)


class TestVersionedObjectHTTP(HTTPVersioningTestCase):
    def test_put_returns_version_id_when_enabled(self):
        self.enable()
        status, _, body = self.request("PUT", "/v1/objects/http/a.txt", b"one",
                                       TENANT)
        self.assertEqual(status, 201)
        first = self.document(body)
        self.assertIn("version_id", first)
        status, _, body = self.request("PUT", "/v1/objects/http/a.txt", b"two",
                                       TENANT)
        self.assertEqual(status, 201)
        second = self.document(body)
        self.assertNotEqual(first["version_id"], second["version_id"])

        # current reads are unaffected
        status, headers, body = self.request("GET", "/v1/objects/http/a.txt",
                                             headers=TENANT)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"two")
        self.assertEqual(headers["x-content-sha256"], sha(b"two"))
        self.assertEqual(headers["etag"], '"%s"' % sha(b"two"))

        # version listing, newest first
        status, _, body = self.request("GET", "/v1/objects/http/a.txt/versions",
                                       headers=TENANT)
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual([i["version_id"] for i in page["items"]],
                         [second["version_id"], first["version_id"]])
        self.assertEqual([i["current"] for i in page["items"]], [True, False])
        self.assertIsNone(page["next_after"])
        item = page["items"][1]
        self.assertEqual(item["sha256"], sha(b"one"))
        self.assertEqual(item["size"], 3)
        self.assertIn("created_at", item)

        # fetch the historical version
        status, headers, body = self.request(
            "GET", "/v1/objects/http/a.txt/versions/%s" % first["version_id"],
            headers=TENANT)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"one")
        self.assertEqual(headers["x-content-sha256"], sha(b"one"))
        self.assertEqual(headers["etag"], '"%s"' % sha(b"one"))

        # delete the current version: the previous one is promoted
        status, _, _ = self.request(
            "DELETE", "/v1/objects/http/a.txt/versions/%s" % second["version_id"],
            headers=TENANT)
        self.assertEqual(status, 204)
        status, _, body = self.request("GET", "/v1/objects/http/a.txt",
                                       headers=TENANT)
        self.assertEqual(body, b"one")

        # deleting the last version removes the key
        status, _, _ = self.request(
            "DELETE", "/v1/objects/http/a.txt/versions/%s" % first["version_id"],
            headers=TENANT)
        self.assertEqual(status, 204)
        status, _, _ = self.request("GET", "/v1/objects/http/a.txt",
                                    headers=TENANT)
        self.assertEqual(status, 404)

    def test_plain_delete_promotes_previous_version(self):
        self.enable()
        for payload in (b"p1", b"p2"):
            self.request("PUT", "/v1/objects/http/del.txt", payload, TENANT)
        status, _, _ = self.request("DELETE", "/v1/objects/http/del.txt",
                                    headers=TENANT)
        self.assertEqual(status, 204)
        status, _, body = self.request("GET", "/v1/objects/http/del.txt",
                                       headers=TENANT)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"p1")

    def test_versions_pagination(self):
        self.enable()
        for i in range(3):
            self.request("PUT", "/v1/objects/http/page.txt",
                         b"p%d" % i, TENANT)
        status, _, body = self.request(
            "GET", "/v1/objects/http/page.txt/versions?limit=2", headers=TENANT)
        page = self.document(body)
        self.assertEqual(len(page["items"]), 2)
        self.assertIsNotNone(page["next_after"])
        status, _, body = self.request(
            "GET", "/v1/objects/http/page.txt/versions?after=%s&limit=2"
            % page["next_after"], headers=TENANT)
        rest = self.document(body)
        self.assertEqual(len(rest["items"]), 1)
        self.assertIsNone(rest["next_after"])

    def test_version_errors(self):
        self.enable()
        # disabled tenant
        status, _, body = self.request("GET", "/v1/objects/http/none/versions",
                                       headers={"X-Objstore-Tenant": "t-off"})
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "versioning disabled"})
        # unknown key
        status, _, body = self.request("GET", "/v1/objects/http/none/versions",
                                       headers=TENANT)
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body)["error"].startswith("not found"),
                         True)
        # malformed version id
        self.request("PUT", "/v1/objects/http/err.txt", b"x", TENANT)
        status, _, body = self.request(
            "GET", "/v1/objects/http/err.txt/versions/not-a-version", headers=TENANT)
        self.assertEqual(status, 400)
        self.assertEqual(self.document(body), {"error": "invalid version"})
        # well-formed but unknown version id
        status, _, body = self.request(
            "GET", "/v1/objects/http/err.txt/versions/%s" % ("0" * 32),
            headers=TENANT)
        self.assertEqual(status, 404)
        # PUT on a versions path is not allowed
        status, _, _ = self.request("PUT", "/v1/objects/http/err.txt/versions",
                                    b"x", TENANT)
        self.assertEqual(status, 405)


class TestRetentionHTTP(HTTPVersioningTestCase):
    def test_policy_set_get_and_purge(self):
        headers = {"X-Objstore-Tenant": "t-ret"}
        self.enable("t-ret")
        status, _, body = self.request("GET", "/v1/retention", headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_versions": None, "max_age_seconds": None})
        status, _, body = self.request(
            "PUT", "/v1/retention", json.dumps({"max_versions": 1}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"max_versions": 1, "max_age_seconds": None})
        status, _, body = self.request("GET", "/v1/retention", headers=headers)
        self.assertEqual(self.document(body),
                         {"max_versions": 1, "max_age_seconds": None})

        ids = []
        for i in range(3):
            status, _, body = self.request("PUT", "/v1/objects/ret/k",
                                           b"r%d" % i, headers)
            ids.append(self.document(body)["version_id"])

        # dry run preview changes nothing
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": True}), headers)
        self.assertEqual(status, 200)
        preview = self.document(body)
        self.assertTrue(preview["dry_run"])
        self.assertEqual([v["version_id"] for v in preview["versions"]],
                         sorted(ids[:2]))
        self.assertEqual(preview["bytes"], 4)
        status, _, body = self.request("GET", "/v1/objects/ret/k/versions",
                                       headers=headers)
        self.assertEqual(len(self.document(body)["items"]), 3)

        # real purge
        status, _, body = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": False}), headers)
        result = self.document(body)
        self.assertFalse(result["dry_run"])
        self.assertEqual([v["version_id"] for v in result["versions"]],
                         sorted(ids[:2]))
        status, _, body = self.request("GET", "/v1/objects/ret/k/versions",
                                       headers=headers)
        items = self.document(body)["items"]
        self.assertEqual([i["version_id"] for i in items], [ids[2]])

    def test_invalid_policy_and_dry_run(self):
        headers = {"X-Objstore-Tenant": "t-inv"}
        for body in (json.dumps({}), json.dumps({"max_versions": -1}),
                     json.dumps({"max_age_seconds": "x"})):
            status, _, payload = self.request("PUT", "/v1/retention", body, headers)
            self.assertEqual(status, 400)
            self.assertEqual(self.document(payload), {"error": "invalid retention"})
        status, _, payload = self.request(
            "POST", "/v1/retention/purge", json.dumps({"dry_run": "yes"}), headers)
        self.assertEqual(status, 400)
        self.assertEqual(self.document(payload), {"error": "invalid dry_run"})

    def test_methods(self):
        status, _, _ = self.request("DELETE", "/v1/retention")
        self.assertEqual(status, 405)
        status, _, _ = self.request("GET", "/v1/retention/purge")
        self.assertEqual(status, 405)


if __name__ == "__main__":
    unittest.main()

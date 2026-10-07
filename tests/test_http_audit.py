"""End-to-end tests for the tenant-scoped audit HTTP endpoints."""

import hashlib
import http.client
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.http_app import create_server
from objstore.store import AUDIT_NAME, ContentAddressedStore


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPAuditTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-audit-")
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
            return response.status, {k.lower(): v
                                     for k, v in response.getheaders()}, payload
        finally:
            conn.close()

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def put_object(self, tenant, key, payload):
        status, _, body = self.request(
            "PUT", "/v1/objects/" + key, payload,
            {"X-Objstore-Tenant": tenant,
             "Content-Length": str(len(payload))})
        assert status in (200, 201), body


class TestAuditEndpoint(HTTPAuditTestCase):
    def test_events_are_tenant_scoped(self):
        self.put_object("audit-a", "ka", b"aaa")
        self.put_object("audit-b", "kb", b"bbb")
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "audit-a"})
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertTrue(page["items"])
        self.assertTrue(all(e["tenant"] == "audit-a" for e in page["items"]))
        self.assertTrue(all(e["operation"] == "put" for e in page["items"]))
        event = page["items"][-1]
        self.assertEqual(event["key"], "ka")
        self.assertEqual(event["sha256"], sha(b"aaa"))
        self.assertEqual(event["size"], 3)
        self.assertEqual(event["result"], "ok")

    def test_default_tenant_when_header_missing(self):
        self.put_object("default", "audit-default-key", b"d")
        status, _, body = self.request("GET", "/v1/audit")
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertTrue(any(e["key"] == "audit-default-key"
                            for e in page["items"]))

    def test_unknown_tenant_gets_empty_page(self):
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "audit-ghost"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"items": [], "next_after": None})

    def test_pagination_with_query_params(self):
        for index in range(3):
            self.put_object("audit-paged", "k%d" % index, b"v")
        status, _, body = self.request(
            "GET", "/v1/audit?limit=2",
            headers={"X-Objstore-Tenant": "audit-paged"})
        self.assertEqual(status, 200)
        page1 = self.document(body)
        self.assertEqual(len(page1["items"]), 2)
        self.assertEqual(page1["next_after"], page1["items"][-1]["seq"])
        status, _, body = self.request(
            "GET", "/v1/audit?after=%d&limit=2" % page1["next_after"],
            headers={"X-Objstore-Tenant": "audit-paged"})
        page2 = self.document(body)
        self.assertEqual([e["seq"] for e in page2["items"]],
                         [page1["next_after"] + 1])
        self.assertIsNone(page2["next_after"])

    def test_invalid_query_params_are_400(self):
        for query in ("?after=-1", "?after=abc", "?limit=0", "?limit=1001",
                      "?limit=x"):
            status, _, body = self.request("GET", "/v1/audit" + query)
            self.assertEqual(status, 400, query)
            self.assertEqual(self.document(body),
                             {"error": "invalid audit query"})

    def test_wrong_method_is_405(self):
        status, _, _ = self.request("POST", "/v1/audit", b"{}",
                                    {"Content-Length": "2"})
        self.assertEqual(status, 405)
        status, _, _ = self.request("POST", "/v1/audit/verify", b"{}",
                                    {"Content-Length": "2"})
        self.assertEqual(status, 405)

    def test_audit_requests_record_no_events(self):
        self.put_object("audit-quiet", "k", b"v")
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "audit-quiet"})
        before = len(self.document(body)["items"])
        self.request("GET", "/v1/audit",
                     headers={"X-Objstore-Tenant": "audit-quiet"})
        self.request("GET", "/v1/audit/verify",
                     headers={"X-Objstore-Tenant": "audit-quiet"})
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "audit-quiet"})
        self.assertEqual(len(self.document(body)["items"]), before)


class TestAuditVerifyEndpoint(HTTPAuditTestCase):
    def test_verify_ok(self):
        self.put_object("audit-verify", "k", b"v")
        status, _, body = self.request(
            "GET", "/v1/audit/verify",
            headers={"X-Objstore-Tenant": "audit-verify"})
        self.assertEqual(status, 200)
        result = self.document(body)
        self.assertEqual(result["ok"], True)
        self.assertGreaterEqual(result["events"], 1)
        self.assertTrue(isinstance(result["last_seq"], int))
        self.assertTrue(isinstance(result["head"], str))

    def test_verify_empty_tenant(self):
        status, _, body = self.request(
            "GET", "/v1/audit/verify",
            headers={"X-Objstore-Tenant": "audit-verify-ghost"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"ok": True, "events": 0,
                          "last_seq": None, "head": None})


class TestAuditVerifyCorruption(unittest.TestCase):
    """A tampered log surfaces as 500 audit corrupted over HTTP."""

    def test_corrupted_log_is_500(self):
        root = tempfile.mkdtemp(prefix="objstore-http-audit-corrupt-")
        store = ContentAddressedStore(root)
        server = create_server(store, "127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
            conn.request("PUT", "/v1/objects/k", b"v",
                         {"Content-Length": "1"})
            self.assertEqual(conn.getresponse().read() is not None, True)
            conn.close()
            with open(os.path.join(root, AUDIT_NAME), "ab") as handle:
                handle.write(b"garbage line\n")
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
            conn.request("GET", "/v1/audit/verify")
            response = conn.getresponse()
            self.assertEqual(response.status, 500)
            self.assertEqual(json.loads(response.read().decode("utf-8")),
                             {"error": "audit corrupted"})
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=10)
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

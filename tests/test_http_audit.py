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
from objstore.store import ContentAddressedStore


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class HTTPAuditTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-http-audit-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02},
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)

    def _stop(self):
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

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))

    def put_object(self, key, payload, tenant=None):
        headers = {"Content-Length": str(len(payload))}
        if tenant is not None:
            headers["X-Objstore-Tenant"] = tenant
        status, _, _ = self.request("PUT", "/v1/objects/" + key, payload,
                                    headers)
        self.assertEqual(status, 201)


class TestAuditEndpoint(HTTPAuditTestCase):
    def test_events_are_tenant_scoped(self):
        self.put_object("a", b"1", tenant="acme")
        self.put_object("b", b"2")
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual(len(page["items"]), 1)
        event = page["items"][0]
        self.assertEqual(event["tenant"], "acme")
        self.assertEqual(event["operation"], "put")
        self.assertEqual(event["key"], "a")
        self.assertEqual(event["seq"], 1)
        self.assertEqual(event["sha256"], sha(b"1"))
        self.assertEqual(event["size"], 1)
        self.assertIsNone(page["next_after"])
        # the default tenant sees its own events only
        status, _, body = self.request("GET", "/v1/audit")
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual([item["key"] for item in page["items"]], ["b"])

    def test_unknown_tenant_gets_empty_page(self):
        self.put_object("a", b"1")
        status, _, body = self.request(
            "GET", "/v1/audit", headers={"X-Objstore-Tenant": "ghost"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"items": [], "next_after": None})

    def test_pagination(self):
        for index in range(3):
            self.put_object("k%d" % index, b"x")
        status, _, body = self.request("GET", "/v1/audit?limit=2")
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual([item["seq"] for item in page["items"]], [1, 2])
        self.assertEqual(page["next_after"], 2)
        status, _, body = self.request("GET", "/v1/audit?after=2&limit=2")
        self.assertEqual(status, 200)
        page = self.document(body)
        self.assertEqual([item["seq"] for item in page["items"]], [3])
        self.assertIsNone(page["next_after"])

    def test_invalid_query_is_400(self):
        for query in ("?after=-1", "?after=abc", "?after=1.5", "?after=",
                      "?limit=0", "?limit=1001", "?limit=-2", "?limit=abc"):
            status, _, body = self.request("GET", "/v1/audit" + query)
            self.assertEqual(status, 400, query)
            self.assertEqual(self.document(body),
                             {"error": "invalid audit query"})

    def test_method_not_allowed(self):
        for path in ("/v1/audit", "/v1/audit/verify"):
            status, _, body = self.request("POST", path, b"{}",
                                           {"Content-Length": "2"})
            self.assertEqual(status, 405, path)

    def test_verify_endpoint(self):
        self.put_object("a", b"1")
        status, _, body = self.request("GET", "/v1/audit/verify")
        self.assertEqual(status, 200)
        result = self.document(body)
        self.assertTrue(result["ok"])
        self.assertEqual(result["events"], 1)
        self.assertEqual(result["last_seq"], 1)
        self.assertIsNotNone(result["head"])
        status, _, body = self.request(
            "GET", "/v1/audit/verify", headers={"X-Objstore-Tenant": "ghost"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body),
                         {"ok": True, "events": 0,
                          "last_seq": None, "head": None})

    def test_audit_endpoints_record_no_events(self):
        self.put_object("a", b"1")
        self.request("GET", "/v1/audit")
        self.request("GET", "/v1/audit/verify")
        status, _, body = self.request("GET", "/v1/audit/verify")
        self.assertEqual(self.document(body)["events"], 1)

    def test_corrupted_log_is_500(self):
        self.put_object("a", b"1")
        self.put_object("b", b"2")
        audit_path = os.path.join(self.root, "audit.log")
        with open(audit_path, "rb") as handle:
            lines = handle.readlines()
        lines[0] = b"garbage\n"
        with open(audit_path, "wb") as handle:
            handle.writelines(lines)
        for path in ("/v1/audit", "/v1/audit/verify"):
            status, _, body = self.request("GET", path)
            self.assertEqual(status, 500, path)
            self.assertEqual(self.document(body),
                             {"error": "audit corrupted"})

    def test_audit_io_error_is_500(self):
        self.put_object("a", b"1")
        audit_path = os.path.join(self.root, "audit.log")
        os.remove(audit_path)
        os.mkdir(audit_path)
        try:
            for path in ("/v1/audit", "/v1/audit/verify"):
                status, _, body = self.request("GET", path)
                self.assertEqual(status, 500, path)
                self.assertEqual(self.document(body),
                                 {"error": "audit io error"})
            # a write whose audit record cannot be stored fails too
            status, _, body = self.request(
                "PUT", "/v1/objects/b", b"2", {"Content-Length": "1"})
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body), {"error": "audit io error"})
        finally:
            os.rmdir(audit_path)
        # the failed write was rolled back
        status, _, _ = self.request("GET", "/v1/objects/b")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()

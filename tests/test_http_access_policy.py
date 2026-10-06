"""End-to-end tests for the access-policy endpoint and signed reads."""

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

SECRET = "0123456789abcdef"


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


def future(seconds=3600):
    return int(time.time()) + seconds


class HTTPAccessPolicyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-access-")
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

    def tenant_headers(self, tenant):
        return {"X-Objstore-Tenant": tenant}

    def make_signed(self, tenant, secret=SECRET):
        status, _, body = self.request(
            "PUT", "/v1/access-policy",
            json.dumps({"mode": "signed", "secret": secret}),
            self.tenant_headers(tenant))
        assert status == 200, body

    def signed_query(self, tenant, method, key, version_id=None,
                     expires=None):
        expires = expires or future()
        signature = self.store.sign_request(
            method, key, version_id=version_id, expires=expires,
            tenant=tenant)
        return "expires=%d&signature=%s" % (expires, signature)


class TestAccessPolicyEndpoint(HTTPAccessPolicyTestCase):
    def test_get_defaults_to_public(self):
        status, _, body = self.request(
            "GET", "/v1/access-policy",
            headers=self.tenant_headers("t-ap-unset"))
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_put_and_get_roundtrip(self):
        headers = self.tenant_headers("t-ap-round")
        status, _, body = self.request(
            "PUT", "/v1/access-policy",
            json.dumps({"mode": "signed", "secret": SECRET}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "signed"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "signed"})

    def test_put_public_and_delete_restore_public(self):
        headers = self.tenant_headers("t-ap-clear")
        self.make_signed("t-ap-clear")
        status, _, body = self.request(
            "PUT", "/v1/access-policy", json.dumps({"mode": "public"}),
            headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})
        self.make_signed("t-ap-clear")
        status, _, body = self.request("DELETE", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_put_rejects_invalid_json(self):
        headers = self.tenant_headers("t-ap-badjson")
        for body in ("not json", "[1, 2]", '"signed"', ""):
            status, _, payload = self.request("PUT", "/v1/access-policy",
                                              body, headers)
            self.assertEqual(status, 400, body)
            self.assertEqual(self.document(payload),
                             {"error": "invalid access policy"})

    def test_put_rejects_invalid_policies(self):
        headers = self.tenant_headers("t-ap-bad")
        for document in ({}, {"mode": "private"},
                         {"mode": "signed"},
                         {"mode": "signed", "secret": "short"},
                         {"mode": "signed", "secret": "x" * 257},
                         {"mode": "signed", "secret": 1234567890123456},
                         {"mode": "public", "secret": SECRET},
                         {"mode": "signed", "secret": SECRET, "extra": 1},
                         {"mode": True}):
            status, _, payload = self.request("PUT", "/v1/access-policy",
                                              json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(payload),
                             {"error": "invalid access policy"})
        status, _, payload = self.request("GET", "/v1/access-policy",
                                          headers=headers)
        self.assertEqual(self.document(payload), {"mode": "public"})

    def test_methods_not_allowed(self):
        for method in ("POST", "HEAD"):
            status, _, _ = self.request(
                method, "/v1/access-policy",
                headers=self.tenant_headers("t-ap-405"))
            self.assertEqual(status, 405, method)

    def test_invalid_tenant_header(self):
        status, _, body = self.request(
            "GET", "/v1/access-policy",
            headers={"X-Objstore-Tenant": "BAD!"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith(
            "invalid tenant"))

    def test_io_error_returns_500_and_keeps_policy(self):
        tenant = "t-ap-io"
        headers = self.tenant_headers(tenant)
        self.make_signed(tenant)
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            for method, body in (("PUT", json.dumps({"mode": "public"})),
                                 ("DELETE", None)):
                status, _, payload = self.request(
                    method, "/v1/access-policy", body, headers)
                self.assertEqual(status, 500, method)
                self.assertEqual(self.document(payload),
                                 {"error": "access policy io error"})
        finally:
            os.rmdir(self.store.index_path)
        status, _, payload = self.request("GET", "/v1/access-policy",
                                          headers=headers)
        self.assertEqual(self.document(payload), {"mode": "signed"})


class TestSignedObjectReads(HTTPAccessPolicyTestCase):
    TENANT = "t-signed"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store.put("doc", b"hello", content_type="text/plain",
                      tenant=cls.TENANT)

    def headers(self):
        return self.tenant_headers(self.TENANT)

    def test_unsigned_reads_work_before_and_after_policy(self):
        status, _, payload = self.request("GET", "/v1/objects/doc",
                                          headers=self.headers())
        self.assertEqual(status, 200)
        self.assertEqual(payload, b"hello")
        self.make_signed(self.TENANT)
        try:
            status, _, body = self.request("GET", "/v1/objects/doc",
                                           headers=self.headers())
            self.assertEqual(status, 403)
            self.assertEqual(self.document(body),
                             {"error": "access denied"})
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())
        status, _, payload = self.request("GET", "/v1/objects/doc",
                                          headers=self.headers())
        self.assertEqual(status, 200)
        self.assertEqual(payload, b"hello")

    def test_signed_get_and_head(self):
        self.make_signed(self.TENANT)
        try:
            query = self.signed_query(self.TENANT, "GET", "doc")
            status, resp_headers, payload = self.request(
                "GET", "/v1/objects/doc?%s" % query, headers=self.headers())
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"hello")
            self.assertEqual(resp_headers.get("etag"), '"%s"' % sha(b"hello"))
            self.assertEqual(resp_headers.get("x-content-sha256"),
                             sha(b"hello"))
            self.assertEqual(resp_headers.get("content-type"), "text/plain")
            query = self.signed_query(self.TENANT, "HEAD", "doc")
            status, resp_headers, payload = self.request(
                "HEAD", "/v1/objects/doc?%s" % query, headers=self.headers())
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"")
            self.assertEqual(resp_headers.get("etag"), '"%s"' % sha(b"hello"))
            self.assertEqual(resp_headers.get("content-length"), "5")
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())

    def test_denied_read_variants(self):
        self.make_signed(self.TENANT)
        try:
            expires = future()
            good = self.signed_query(self.TENANT, "GET", "doc",
                                     expires=expires)
            bad_signature = self.signed_query(self.TENANT, "GET", "other",
                                              expires=expires)
            queries = (
                "",                                  # missing both
                "expires=%d" % expires,              # missing signature
                "signature=%s" % good.split("=")[2],  # missing expires
                "expires=%d&expires=%d&signature=x" % (expires, expires),
                "expires=abc&signature=x",           # malformed expires
                "expires=&signature=x",
                "expires=%d&signature=" % expires,   # empty signature
                "expires=%d&signature=%s&signature=%s" % ((expires,) +
                                                          ("ab",) * 2),
                "expires=%d&%s" % (int(time.time()) - 1,
                                   good.split("&")[1]),  # expired
                bad_signature,                       # signature for other key
            )
            for query in queries:
                status, _, body = self.request(
                    "GET", "/v1/objects/doc?%s" % query,
                    headers=self.headers())
                self.assertEqual(status, 403, query)
                self.assertEqual(self.document(body),
                                 {"error": "access denied"})
                status, _, _ = self.request(
                    "HEAD", "/v1/objects/doc?%s" % query,
                    headers=self.headers())
                self.assertEqual(status, 403, query)
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())

    def test_valid_signature_on_unknown_key_is_404(self):
        self.make_signed(self.TENANT)
        try:
            query = self.signed_query(self.TENANT, "GET", "missing")
            status, _, body = self.request(
                "GET", "/v1/objects/missing?%s" % query,
                headers=self.headers())
            self.assertEqual(status, 404)
            self.assertEqual(self.document(body),
                             {"error": "not found: missing"})
            # Without a signature the same unknown key is denied first.
            status, _, body = self.request(
                "GET", "/v1/objects/missing", headers=self.headers())
            self.assertEqual(status, 403)
            self.assertEqual(self.document(body),
                             {"error": "access denied"})
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())

    def test_writes_and_lists_stay_open_under_signed_policy(self):
        self.make_signed(self.TENANT)
        try:
            status, _, _ = self.request("PUT", "/v1/objects/new", b"n",
                                        self.headers())
            self.assertEqual(status, 201)
            status, _, body = self.request("GET", "/v1/objects",
                                           headers=self.headers())
            self.assertEqual(status, 200)
            keys = [item["key"] for item in self.document(body)["items"]]
            self.assertIn("new", keys)
            status, _, _ = self.request("DELETE", "/v1/objects/new",
                                        headers=self.headers())
            self.assertEqual(status, 204)
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())

    def test_policy_is_tenant_scoped(self):
        self.make_signed(self.TENANT)
        try:
            # The default tenant keeps the same key readable without a
            # signature.
            self.store.put("scoped-doc", b"public")
            status, _, payload = self.request("GET", "/v1/objects/scoped-doc")
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"public")
            status, _, _ = self.request("GET", "/v1/objects/doc",
                                        headers=self.headers())
            self.assertEqual(status, 403)
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())
            self.store.delete("scoped-doc")


class TestSignedVersionReads(HTTPAccessPolicyTestCase):
    TENANT = "t-signed-versions"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store.set_versioning(True, tenant=cls.TENANT)
        cls.store.put("doc", b"v1", tenant=cls.TENANT)
        cls.version_id = cls.store.put("doc", b"v2",
                                       tenant=cls.TENANT)["version_id"]

    def headers(self):
        return self.tenant_headers(self.TENANT)

    def test_signed_version_get_and_head(self):
        self.make_signed(self.TENANT)
        try:
            base = "/v1/objects/doc/versions/%s" % self.version_id
            status, _, body = self.request("GET", base,
                                           headers=self.headers())
            self.assertEqual(status, 403)
            self.assertEqual(self.document(body), {"error": "access denied"})
            query = self.signed_query(self.TENANT, "GET", "doc",
                                      version_id=self.version_id)
            status, resp_headers, payload = self.request(
                "GET", "%s?%s" % (base, query), headers=self.headers())
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"v2")
            self.assertEqual(resp_headers.get("etag"), '"%s"' % sha(b"v2"))
            query = self.signed_query(self.TENANT, "HEAD", "doc",
                                      version_id=self.version_id)
            status, resp_headers, payload = self.request(
                "HEAD", "%s?%s" % (base, query), headers=self.headers())
            self.assertEqual(status, 200)
            self.assertEqual(payload, b"")
            self.assertEqual(resp_headers.get("x-content-sha256"), sha(b"v2"))
            # A signature over the object (no version id) does not fit.
            query = self.signed_query(self.TENANT, "GET", "doc")
            status, _, body = self.request("GET", "%s?%s" % (base, query),
                                           headers=self.headers())
            self.assertEqual(status, 403)
            self.assertEqual(self.document(body), {"error": "access denied"})
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())

    def test_valid_signature_on_unknown_version_is_404(self):
        self.make_signed(self.TENANT)
        try:
            unknown = "0" * 32
            query = self.signed_query(self.TENANT, "GET", "doc",
                                      version_id=unknown)
            status, _, body = self.request(
                "GET", "/v1/objects/doc/versions/%s?%s" % (unknown, query),
                headers=self.headers())
            self.assertEqual(status, 404)
            self.assertEqual(self.document(body),
                             {"error": "not found: version %s" % unknown})
        finally:
            self.request("DELETE", "/v1/access-policy",
                         headers=self.headers())


if __name__ == "__main__":
    unittest.main()

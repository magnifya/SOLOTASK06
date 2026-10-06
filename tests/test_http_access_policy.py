"""End-to-end tests for the access-policy endpoint and signed reads."""

import hashlib
import hmac
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


def sign(secret, tenant, method, key, version_id, expires):
    payload = "\n".join((tenant, method, key, version_id or "", str(expires)))
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"),
                    hashlib.sha256).hexdigest()


def future():
    return int(time.time()) + 3600


class HTTPAccessPolicyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-access-")
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

    def set_signed(self, tenant, secret=SECRET):
        status, _, body = self.request(
            "PUT", "/v1/access-policy",
            json.dumps({"mode": "signed", "secret": secret}),
            {"X-Objstore-Tenant": tenant})
        assert status == 200, body

    def signed_query(self, tenant, method, key, version_id=None,
                     expires=None):
        expires = expires or future()
        signature = sign(SECRET, tenant, method, key, version_id, expires)
        return "expires=%d&signature=%s" % (expires, signature)


class TestAccessPolicyEndpoint(HTTPAccessPolicyTestCase):
    def test_get_defaults_to_public(self):
        status, _, body = self.request(
            "GET", "/v1/access-policy",
            headers={"X-Objstore-Tenant": "t-ap-unset"})
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_put_and_get_roundtrip(self):
        headers = {"X-Objstore-Tenant": "t-ap-round"}
        status, _, body = self.request(
            "PUT", "/v1/access-policy",
            json.dumps({"mode": "signed", "secret": SECRET}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "signed"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "signed"})

    def test_get_does_not_leak_secret(self):
        headers = {"X-Objstore-Tenant": "t-ap-leak"}
        self.set_signed("t-ap-leak")
        for method in ("GET",):
            status, _, body = self.request(method, "/v1/access-policy",
                                           headers=headers)
            self.assertNotIn(SECRET, body.decode("utf-8"))

    def test_put_public_restores_default(self):
        headers = {"X-Objstore-Tenant": "t-ap-public"}
        self.set_signed("t-ap-public")
        status, _, body = self.request("PUT", "/v1/access-policy",
                                       json.dumps({"mode": "public"}), headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_delete_restores_public(self):
        headers = {"X-Objstore-Tenant": "t-ap-delete"}
        self.set_signed("t-ap-delete")
        status, _, body = self.request("DELETE", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body), {"mode": "public"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_put_rejects_invalid_json(self):
        headers = {"X-Objstore-Tenant": "t-ap-badjson"}
        for body in ("not json", "[1, 2]", '"signed"', "42", ""):
            status, _, payload = self.request("PUT", "/v1/access-policy",
                                              body, headers)
            self.assertEqual(status, 400, body)
            self.assertEqual(self.document(payload),
                             {"error": "invalid access policy"})

    def test_put_rejects_invalid_policies(self):
        headers = {"X-Objstore-Tenant": "t-ap-bad"}
        for document in ({}, {"mode": "private"}, {"mode": "SIGNED"},
                         {"mode": None}, {"mode": True}, {"mode": "signed"},
                         {"mode": "signed", "secret": None},
                         {"mode": "signed", "secret": "short"},
                         {"mode": "signed", "secret": "x" * 257},
                         {"mode": "signed", "secret": 42},
                         {"mode": "signed", "secret": SECRET, "extra": 1},
                         {"secret": SECRET}):
            status, _, body = self.request("PUT", "/v1/access-policy",
                                           json.dumps(document), headers)
            self.assertEqual(status, 400, document)
            self.assertEqual(self.document(body),
                             {"error": "invalid access policy"})
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_methods_not_allowed(self):
        for method in ("POST", "HEAD"):
            status, _, _ = self.request(method, "/v1/access-policy",
                                        headers={"X-Objstore-Tenant": "t-ap-m"})
            self.assertEqual(status, 405, method)

    def test_invalid_tenant_header(self):
        status, _, body = self.request(
            "GET", "/v1/access-policy", headers={"X-Objstore-Tenant": "BAD!"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))

    def test_policy_is_tenant_scoped(self):
        self.set_signed("t-ap-scoped")
        status, _, body = self.request("GET", "/v1/access-policy")
        self.assertEqual(self.document(body), {"mode": "public"})

    def test_io_error_returns_500_and_keeps_policy(self):
        headers = {"X-Objstore-Tenant": "t-ap-io"}
        self.set_signed("t-ap-io")
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            status, _, body = self.request(
                "PUT", "/v1/access-policy",
                json.dumps({"mode": "signed", "secret": "x" * 32}), headers)
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body),
                             {"error": "access policy io error"})
            status, _, body = self.request("DELETE", "/v1/access-policy",
                                           headers=headers)
            self.assertEqual(status, 500)
            self.assertEqual(self.document(body),
                             {"error": "access policy io error"})
        finally:
            os.rmdir(self.store.index_path)
        status, _, body = self.request("GET", "/v1/access-policy",
                                       headers=headers)
        self.assertEqual(self.document(body), {"mode": "signed"})


class TestSignedObjectReads(HTTPAccessPolicyTestCase):
    tenant = "t-signed-objects"
    headers = {"X-Objstore-Tenant": tenant}

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store.put("doc", b"payload", tenant=cls.tenant)

    def test_public_reads_ignore_query_params(self):
        status, _, body = self.request("GET", "/v1/objects/pub?expires=x")
        self.assertEqual(status, 404)  # unknown key, not a parameter error
        self.request("PUT", "/v1/objects/pub", b"data")
        status, _, body = self.request(
            "GET", "/v1/objects/pub?expires=x&signature=y")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"data")

    def test_signed_read_requires_parameters(self):
        self.set_signed(self.tenant)
        for path in ("/v1/objects/doc",
                     "/v1/objects/doc?expires=%d" % future(),
                     "/v1/objects/doc?signature=%s" % ("0" * 64),
                     "/v1/objects/doc?expires=abc&signature=%s" % ("0" * 64),
                     "/v1/objects/doc?expires=&signature=%s" % ("0" * 64),
                     "/v1/objects/doc?expires=%d&expires=%d&signature=%s"
                     % (future(), future(), "0" * 64),
                     "/v1/objects/doc?expires=%d&signature=%s&signature=%s"
                     % (future(), "0" * 64, "1" * 64),
                     "/v1/objects/doc?expires=%d&signature=zzzz"
                     % future()):
            status, _, body = self.request("GET", path, headers=self.headers)
            self.assertEqual(status, 403, path)
            self.assertEqual(self.document(body), {"error": "access denied"})

    def test_signed_head_requires_parameters(self):
        self.set_signed(self.tenant)
        status, _, _ = self.request("HEAD", "/v1/objects/doc",
                                    headers=self.headers)
        self.assertEqual(status, 403)

    def test_expired_and_mismatched_signatures(self):
        self.set_signed(self.tenant)
        expired = int(time.time()) - 10
        signature = sign(SECRET, self.tenant, "GET", "doc", None, expired)
        status, _, _ = self.request(
            "GET", "/v1/objects/doc?expires=%d&signature=%s"
            % (expired, signature), headers=self.headers)
        self.assertEqual(status, 403)
        # Well-formed but wrong signature.
        status, _, _ = self.request(
            "GET", "/v1/objects/doc?expires=%d&signature=%s"
            % (future(), "0" * 64), headers=self.headers)
        self.assertEqual(status, 403)
        # Signature for a different key.
        query = self.signed_query(self.tenant, "GET", "other")
        status, _, _ = self.request("GET", "/v1/objects/doc?%s" % query,
                                    headers=self.headers)
        self.assertEqual(status, 403)
        # A GET signature does not authorize HEAD.
        query = self.signed_query(self.tenant, "GET", "doc")
        status, _, _ = self.request("HEAD", "/v1/objects/doc?%s" % query,
                                    headers=self.headers)
        self.assertEqual(status, 403)

    def test_signed_get_and_head_ok(self):
        self.set_signed(self.tenant)
        query = self.signed_query(self.tenant, "GET", "doc")
        status, headers, body = self.request("GET", "/v1/objects/doc?%s" % query,
                                             headers=self.headers)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"payload")
        self.assertEqual(headers.get("etag"), '"%s"' % sha(b"payload"))
        self.assertEqual(headers.get("x-content-sha256"), sha(b"payload"))
        query = self.signed_query(self.tenant, "HEAD", "doc")
        status, headers, body = self.request("HEAD", "/v1/objects/doc?%s" % query,
                                             headers=self.headers)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        self.assertEqual(headers.get("etag"), '"%s"' % sha(b"payload"))
        self.assertEqual(headers.get("content-length"), "7")

    def test_valid_signature_unknown_key_is_404(self):
        self.set_signed(self.tenant)
        query = self.signed_query(self.tenant, "GET", "missing")
        status, _, body = self.request("GET", "/v1/objects/missing?%s" % query,
                                       headers=self.headers)
        self.assertEqual(status, 404)
        self.assertEqual(self.document(body), {"error": "not found: missing"})

    def test_writes_stay_open(self):
        self.set_signed(self.tenant)
        status, _, _ = self.request("PUT", "/v1/objects/doc2", b"new",
                                    headers=self.headers)
        self.assertEqual(status, 201)
        status, _, _ = self.request("DELETE", "/v1/objects/doc2",
                                    headers=self.headers)
        self.assertEqual(status, 204)


class TestSignedVersionReads(HTTPAccessPolicyTestCase):
    tenant = "t-signed-versions"
    headers = {"X-Objstore-Tenant": tenant}

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.store.set_versioning(True, tenant=cls.tenant)
        cls.store.put("doc", b"v1", tenant=cls.tenant)
        cls.second = cls.store.put("doc", b"v2", tenant=cls.tenant)
        cls.store.set_access_policy("signed", secret=SECRET, tenant=cls.tenant)

    def test_version_read_requires_signature(self):
        version_id = self.second["version_id"]
        status, _, body = self.request(
            "GET", "/v1/objects/doc/versions/%s" % version_id,
            headers=self.headers)
        self.assertEqual(status, 403)
        self.assertEqual(self.document(body), {"error": "access denied"})
        status, _, _ = self.request(
            "HEAD", "/v1/objects/doc/versions/%s" % version_id,
            headers=self.headers)
        self.assertEqual(status, 403)

    def test_signed_version_get_and_head_ok(self):
        version_id = self.second["version_id"]
        query = self.signed_query(self.tenant, "GET", "doc",
                                  version_id=version_id)
        status, headers, body = self.request(
            "GET", "/v1/objects/doc/versions/%s?%s" % (version_id, query),
            headers=self.headers)
        self.assertEqual(status, 200)
        self.assertEqual(body, b"v2")
        self.assertEqual(headers.get("etag"), '"%s"' % sha(b"v2"))
        query = self.signed_query(self.tenant, "HEAD", "doc",
                                  version_id=version_id)
        status, headers, body = self.request(
            "HEAD", "/v1/objects/doc/versions/%s?%s" % (version_id, query),
            headers=self.headers)
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("etag"), '"%s"' % sha(b"v2"))

    def test_valid_signature_unknown_version_is_404(self):
        version_id = "1" * 32
        query = self.signed_query(self.tenant, "GET", "doc",
                                  version_id=version_id)
        status, _, _ = self.request(
            "GET", "/v1/objects/doc/versions/%s?%s" % (version_id, query),
            headers=self.headers)
        self.assertEqual(status, 404)

    def test_object_signature_does_not_cover_version_path(self):
        version_id = self.second["version_id"]
        # Signed without the version id: the payload differs, so 403.
        query = self.signed_query(self.tenant, "GET", "doc")
        status, _, _ = self.request(
            "GET", "/v1/objects/doc/versions/%s?%s" % (version_id, query),
            headers=self.headers)
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()

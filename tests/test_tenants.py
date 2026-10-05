"""Tests for the multi-tenant object namespaces (run from the repository root)."""

import hashlib
import http.client
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

from objstore.http_app import create_server
from objstore.store import ContentAddressedStore, ObjectStoreError

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class TenantTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-tenant-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)


class TestTenantValidation(TenantTestCase):
    BAD = ["", "A", "Default", "-abc", "_abc", "a b", "a.b", "a/b",
           "a" * 65, None, 7, b"acme"]

    GOOD = ["a", "0", "default", "tenant-1_x", "a" * 64]

    def test_object_operations_reject_bad_tenants(self):
        for bad in self.BAD:
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.put("k", b"v", tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.get("k", tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.head("k", tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.delete("k", tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.list_objects(tenant=bad)

    def test_session_operations_reject_bad_tenants(self):
        for bad in self.BAD:
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.begin_upload("k", 0, sha(b""), tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.upload_status("0" * 32, tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.append_upload("0" * 32, 0, b"", tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.complete_upload("0" * 32, tenant=bad)
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid tenant"):
                self.store.abort_upload("0" * 32, tenant=bad)

    def test_good_tenants_are_accepted(self):
        for good in self.GOOD:
            self.store.put("k", b"v", tenant=good)
            self.assertEqual(self.store.get("k", tenant=good)[0], b"v")

    def test_invalid_tenant_changes_nothing(self):
        self.store.put("k", b"v")
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"other", tenant="Bad")
        with self.assertRaises(ObjectStoreError):
            self.store.delete("k", tenant="Bad")
        self.assertEqual(self.store.get("k")[0], b"v")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("k")[0], b"v")
        self.assertEqual(reopened.list_objects(tenant="acme"),
                         {"items": [], "next_after": None})


class TestTenantIsolation(TenantTestCase):
    def test_same_key_is_independent_per_tenant(self):
        self.store.put("k", b"aaa")
        self.store.put("k", b"bbb", tenant="acme")
        self.assertEqual(self.store.get("k")[0], b"aaa")
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"bbb")
        self.store.delete("k", tenant="acme")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            self.store.head("k", tenant="acme")
        self.assertEqual(self.store.get("k")[0], b"aaa")

    def test_missing_key_stays_missing_despite_other_tenants(self):
        self.store.put("k", b"v", tenant="acme")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            self.store.head("k")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            self.store.delete("k")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            self.store.get("k", tenant="ghost")

    def test_listing_is_scoped_and_reports_raw_keys(self):
        self.store.put("a/1", b"1", tenant="acme")
        self.store.put("a/2", b"2", tenant="acme")
        self.store.put("a/3", b"3", tenant="acme")
        self.store.put("a/9", b"9")
        page = self.store.list_objects(prefix="a/", limit=2, tenant="acme")
        self.assertEqual([item["key"] for item in page["items"]], ["a/1", "a/2"])
        rest = self.store.list_objects(prefix="a/", after=page["next_after"],
                                       tenant="acme")
        self.assertEqual([item["key"] for item in rest["items"]], ["a/3"])
        self.assertIsNone(rest["next_after"])
        default_page = self.store.list_objects(prefix="a/")
        self.assertEqual([item["key"] for item in default_page["items"]], ["a/9"])

    def test_tenant_without_objects_lists_empty_page(self):
        self.assertEqual(self.store.list_objects(tenant="ghost"),
                         {"items": [], "next_after": None})

    def test_condition_only_compares_owning_tenant(self):
        self.store.put("k", b"aaa")
        self.store.put("k", b"bbb", tenant="acme")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"x", expected_sha256=sha(b"bbb"))
        self.store.put("k", b"x", expected_sha256=sha(b"bbb"), tenant="acme")
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"x")
        self.assertEqual(self.store.get("k")[0], b"aaa")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=sha(b"x"))
        self.store.delete("k", expected_sha256=sha(b"x"), tenant="acme")

    def test_same_content_shared_across_tenants_stored_once(self):
        self.store.put("k", b"same")
        self.store.put("k", b"same", tenant="acme")
        self.store.put("k", b"same", tenant="other")
        self.assertEqual(self.store.blob_digests(), [sha(b"same")])

    def test_gc_counts_references_of_every_tenant(self):
        self.store.put("k", b"same")
        self.store.put("k", b"same", tenant="acme")
        self.store.delete("k")
        self.assertEqual(self.store.collect_garbage(dry_run=True)["digests"], [])
        self.store.delete("k", tenant="acme")
        self.assertEqual(self.store.collect_garbage(dry_run=True)["digests"],
                         [sha(b"same")])

    def test_tenants_persist_across_reopen(self):
        self.store.put("k", b"aaa")
        self.store.put("k", b"bbb", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("k")[0], b"aaa")
        self.assertEqual(reopened.get("k", tenant="acme")[0], b"bbb")
        self.assertEqual([i["key"] for i in
                          reopened.list_objects(tenant="acme")["items"]], ["k"])


class TestTenantUploads(TenantTestCase):
    def begin(self, key="k", data=b"payload", tenant="acme", **kwargs):
        return self.store.begin_upload(key, len(data), sha(data), tenant=tenant,
                                       **kwargs)

    def test_session_is_bound_to_creating_tenant(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"pay", tenant="acme")
        for operation in (
                lambda: self.store.upload_status(session_id),
                lambda: self.store.append_upload(session_id, 3, b"load"),
                lambda: self.store.complete_upload(session_id),
                lambda: self.store.abort_upload(session_id)):
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Aunknown upload session"):
                operation()
        # the session itself is undisturbed
        self.assertEqual(
            self.store.upload_status(session_id, tenant="acme")["offset"], 3)
        self.store.append_upload(session_id, 3, b"load", tenant="acme")
        entry = self.store.complete_upload(session_id, tenant="acme")
        self.assertEqual(entry["sha256"], sha(b"payload"))
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"payload")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            self.store.head("k")

    def test_cross_tenant_session_matches_unknown_session_error(self):
        session_id = self.begin()
        with self.assertRaises(ObjectStoreError) as unknown:
            self.store.upload_status("1" * 32)
        with self.assertRaises(ObjectStoreError) as foreign:
            self.store.upload_status(session_id)
        self.assertEqual(str(unknown.exception).replace("1" * 32, session_id),
                         str(foreign.exception))

    def test_completed_record_is_bound_to_tenant(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"payload", tenant="acme")
        self.store.complete_upload(session_id, tenant="acme")
        status = self.store.upload_status(session_id, tenant="acme")
        self.assertTrue(status["completed"])
        with self.assertRaisesRegex(ObjectStoreError, r"\Aunknown upload session"):
            self.store.upload_status(session_id)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aunknown upload session"):
            self.store.complete_upload(session_id)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aunknown upload session"):
            self.store.abort_upload(session_id)

    def test_repeated_complete_returns_first_result_per_tenant(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"payload", tenant="acme")
        first = self.store.complete_upload(session_id, tenant="acme")
        self.store.put("k", b"later", tenant="acme")
        self.assertEqual(self.store.complete_upload(session_id, tenant="acme"),
                         first)
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"later")

    def test_completion_condition_only_checks_owning_tenant(self):
        self.store.put("k", b"old", tenant="acme")
        session_id = self.begin(data=b"new", expected_sha256=sha(b"old"))
        self.store.append_upload(session_id, 0, b"new", tenant="acme")
        self.store.put("k", b"unrelated")  # default tenant churn is irrelevant
        self.store.put("k", b"changed", tenant="acme")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(session_id, tenant="acme")
        # confirmed progress survives the failed completion
        self.assertEqual(
            self.store.upload_status(session_id, tenant="acme")["offset"], 3)
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"changed")
        self.store.delete("k", tenant="acme")
        self.store.put("k", b"old", tenant="acme")
        self.assertEqual(self.store.complete_upload(session_id, tenant="acme")
                         ["sha256"], sha(b"new"))

    def test_session_tenant_survives_reopen(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"pay", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(
            reopened.upload_status(session_id, tenant="acme")["offset"], 3)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aunknown upload session"):
            reopened.upload_status(session_id)
        reopened.append_upload(session_id, 3, b"load", tenant="acme")
        reopened.complete_upload(session_id, tenant="acme")
        self.assertEqual(ContentAddressedStore(self.root)
                         .get("k", tenant="acme")[0], b"payload")

    def test_gc_keeps_incomplete_session_data_of_any_tenant(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"pay", tenant="acme")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [], "bytes": 0, "dry_run": False})
        self.assertEqual(
            self.store.upload_status(session_id, tenant="acme")["offset"], 3)


class TestTenantMigration(TenantTestCase):
    def test_legacy_directory_belongs_to_default_tenant(self):
        # a directory written before tenants existed: flat objects, uploads
        # records and session files without any tenant field
        self.store.put("legacy", b"old")
        session_id = self.store.begin_upload("up", 3, sha(b"xyz"))
        self.store.append_upload(session_id, 0, b"xy")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(sorted(document), ["objects", "version"])
        meta_path = self.store._session_meta_path(session_id)
        with open(meta_path, "rb") as handle:
            self.assertNotIn("tenant", json.loads(handle.read().decode("utf-8")))
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("legacy")[0], b"old")
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found"):
            reopened.head("legacy", tenant="acme")
        self.assertEqual(reopened.upload_status(session_id)["offset"], 2)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aunknown upload session"):
            reopened.upload_status(session_id, tenant="acme")
        reopened.append_upload(session_id, 2, b"z")
        reopened.complete_upload(session_id)
        self.assertEqual(reopened.get("up")[0], b"xyz")
        self.assertTrue(ContentAddressedStore(self.root)
                        .upload_status(session_id)["completed"])

    def test_default_tenant_index_keeps_legacy_shape(self):
        self.store.put("k", b"v")
        self.store.put("k", b"w", tenant="acme")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(sorted(document), ["objects", "tenants", "version"])
        self.assertEqual(document["objects"]["k"]["sha256"], sha(b"v"))
        self.assertEqual(document["tenants"]["acme"]["k"]["sha256"], sha(b"w"))

    def test_corrupted_tenant_data_is_reported(self):
        self.store.put("k", b"v")
        path = os.path.join(self.root, "index.json")
        with open(path, "rb") as handle:
            good = handle.read()
        base = json.loads(good.decode("utf-8"))
        for extra in ({"tenants": []}, {"tenants": {"Bad": {}}},
                      {"tenants": {"default": {}}}, {"tenants": {"ok": []}},
                      {"tenants": {"ok": {"k": "not-an-entry"}}}):
            document = dict(base)
            document.update(extra)
            with open(path, "wb") as handle:
                handle.write(json.dumps(document).encode("utf-8"))
            with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted index"):
                ContentAddressedStore(self.root)
        with open(path, "wb") as handle:
            handle.write(good)
        self.assertEqual(ContentAddressedStore(self.root).get("k")[0], b"v")


class HTTPTenantTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="objstore-http-tenant-")
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
            return response.status, payload
        finally:
            conn.close()

    def document(self, payload):
        return json.loads(payload.decode("utf-8"))


class TestHTTPTenants(HTTPTenantTestCase):
    def test_object_endpoints_honor_tenant_header(self):
        status, _ = self.request("PUT", "/v1/objects/k", b"aaa")
        self.assertEqual(status, 201)
        status, _ = self.request("PUT", "/v1/objects/k", b"bbb",
                                 {"X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 201)
        status, body = self.request("GET", "/v1/objects/k")
        self.assertEqual((status, body), (200, b"aaa"))
        status, body = self.request("GET", "/v1/objects/k",
                                    headers={"X-Objstore-Tenant": "acme"})
        self.assertEqual((status, body), (200, b"bbb"))
        status, body = self.request("GET", "/v1/objects/k",
                                    headers={"X-Objstore-Tenant": "ghost"})
        self.assertEqual(status, 404)
        status, _ = self.request("DELETE", "/v1/objects/k",
                                 headers={"X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 204)
        status, body = self.request("GET", "/v1/objects/k")
        self.assertEqual((status, body), (200, b"aaa"))

    def test_list_endpoint_filters_by_tenant(self):
        self.request("PUT", "/v1/objects/l1", b"1",
                     {"X-Objstore-Tenant": "acme"})
        self.request("PUT", "/v1/objects/l2", b"2")
        status, body = self.request("GET", "/v1/objects?prefix=l",
                                    headers={"X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 200)
        self.assertEqual([i["key"] for i in self.document(body)["items"]],
                         ["l1"])
        status, body = self.request("GET", "/v1/objects?prefix=l")
        self.assertEqual([i["key"] for i in self.document(body)["items"]],
                         ["l2"])
        status, body = self.request("GET", "/v1/objects",
                                    headers={"X-Objstore-Tenant": "ghost"})
        self.assertEqual(self.document(body), {"items": [], "next_after": None})

    def test_conditional_headers_apply_per_tenant(self):
        self.request("PUT", "/v1/objects/c", b"aaa")
        self.request("PUT", "/v1/objects/c", b"bbb", {"X-Objstore-Tenant": "acme"})
        status, body = self.request(
            "PUT", "/v1/objects/c", b"x",
            {"If-Match": '"%s"' % sha(b"bbb")})
        self.assertEqual(status, 412)
        status, body = self.request(
            "PUT", "/v1/objects/c", b"x",
            {"If-Match": '"%s"' % sha(b"bbb"), "X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 201)

    def test_upload_sessions_honor_tenant_header(self):
        tenant = {"X-Objstore-Tenant": "acme"}
        declaration = json.dumps({"key": "u", "size": 3, "sha256": sha(b"xyz")})
        status, body = self.request("POST", "/v1/uploads", declaration, tenant)
        self.assertEqual(status, 201)
        session_id = self.document(body)["session"]
        status, _ = self.request("PUT", "/v1/uploads/%s?offset=0" % session_id,
                                 b"xyz", tenant)
        self.assertEqual(status, 200)
        for method, path in (
                ("GET", "/v1/uploads/%s" % session_id),
                ("PUT", "/v1/uploads/%s?offset=3" % session_id),
                ("POST", "/v1/uploads/%s/complete" % session_id),
                ("DELETE", "/v1/uploads/%s" % session_id)):
            status, body = self.request(method, path, b"" if method != "GET" else None)
            self.assertEqual(status, 404, (method, body))
            self.assertTrue(self.document(body)["error"]
                            .startswith("unknown upload session"), (method, body))
        status, body = self.request("GET", "/v1/uploads/%s" % ("0" * 32,))
        self.assertEqual(status, 404)
        self.assertTrue(self.document(body)["error"]
                        .startswith("unknown upload session"))
        status, body = self.request("POST", "/v1/uploads/%s/complete" % session_id,
                                    None, tenant)
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body)["sha256"], sha(b"xyz"))
        status, body = self.request("GET", "/v1/objects/u", headers=tenant)
        self.assertEqual((status, body), (200, b"xyz"))
        status, body = self.request("GET", "/v1/objects/u")
        self.assertEqual(status, 404)

    def test_invalid_tenant_header_is_a_400(self):
        for bad in ("Bad", "-x", "", "a" * 65, "a b"):
            status, body = self.request("GET", "/v1/objects/k",
                                        headers={"X-Objstore-Tenant": bad})
            self.assertEqual(status, 400, (bad, body))
            self.assertTrue(self.document(body)["error"]
                            .startswith("invalid tenant"), (bad, body))
            self.assertEqual(sorted(self.document(body)), ["error"])

    def test_repeated_tenant_header_is_a_400(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("GET", "/v1/objects/k")
            conn.putheader("X-Objstore-Tenant", "acme")
            conn.putheader("X-Objstore-Tenant", "acme")
            conn.endheaders()
            response = conn.getresponse()
            body = response.read()
        finally:
            conn.close()
        self.assertEqual(response.status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))

    def test_invalid_tenant_takes_precedence(self):
        # invalid tenant beats a missing body length, a bad precondition and
        # a malformed upload declaration, and changes nothing
        status, _ = self.request("PUT", "/v1/objects/prec", b"aaa")
        self.assertEqual(status, 201)
        status, body = self.request("PUT", "/v1/objects/prec", None,
                                    {"X-Objstore-Tenant": "Bad"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))
        status, body = self.request(
            "PUT", "/v1/objects/prec", b"v",
            {"X-Objstore-Tenant": "Bad", "If-Match": "junk"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))
        status, body = self.request("POST", "/v1/uploads", b"{",
                                    {"X-Objstore-Tenant": "Bad"})
        self.assertEqual(status, 400)
        self.assertTrue(self.document(body)["error"].startswith("invalid tenant"))
        status, body = self.request("GET", "/v1/objects/prec")
        self.assertEqual((status, body), (200, b"aaa"))

    def test_blob_endpoint_stays_global(self):
        status, _ = self.request("PUT", "/v1/objects/blobby", b"aaa",
                                 {"X-Objstore-Tenant": "acme"})
        self.assertEqual(status, 201)
        digest = sha(b"aaa")
        status, body = self.request("GET", "/v1/blobs/%s" % digest,
                                    headers={"X-Objstore-Tenant": "acme"})
        self.assertEqual((status, body), (200, b"aaa"))
        status, body = self.request("GET", "/v1/blobs/%s" % digest,
                                    headers={"X-Objstore-Tenant": "Bad"})
        self.assertEqual((status, body), (200, b"aaa"))


class TestCLITenants(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="objstore-cli-tenant-")
        self.data_dir = os.path.join(self.tmp, "data")
        os.makedirs(self.data_dir)
        self.payload = os.path.join(self.tmp, "payload.bin")
        with open(self.payload, "wb") as handle:
            handle.write(b"cli-bytes")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "objstore", "--data-dir", self.data_dir, *args],
            capture_output=True, cwd=REPO_ROOT,
        )

    def document(self, result):
        return json.loads(result.stdout.decode("utf-8"))

    def test_put_get_list_delete_with_tenant(self):
        result = self.run_cli("put", "--key", "k", "--file", self.payload)
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.run_cli("put", "--key", "k", "--file", self.payload,
                              "--tenant", "acme")
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(self.payload, "wb") as handle:
            handle.write(b"other-bytes")
        result = self.run_cli("put", "--key", "k", "--file", self.payload,
                              "--tenant", "acme")
        self.assertEqual(result.returncode, 0, result.stderr)
        out = os.path.join(self.tmp, "out.bin")
        result = self.run_cli("get", "--key", "k", "--out", out)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, "rb") as handle:
            self.assertEqual(handle.read(), b"cli-bytes")
        result = self.run_cli("get", "--key", "k", "--out", out,
                              "--tenant", "acme")
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, "rb") as handle:
            self.assertEqual(handle.read(), b"other-bytes")
        result = self.run_cli("list", "--tenant", "acme")
        self.assertEqual([i["key"] for i in self.document(result)["items"]],
                         ["k"])
        result = self.run_cli("delete", "--key", "k", "--tenant", "acme")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.run_cli("list", "--tenant", "acme")
        self.assertEqual(self.document(result)["items"], [])
        result = self.run_cli("list")
        self.assertEqual([i["key"] for i in self.document(result)["items"]],
                         ["k"])

    def test_invalid_tenant_fails_like_other_store_errors(self):
        result = self.run_cli("put", "--key", "k", "--file", self.payload,
                              "--tenant", "Bad")
        self.assertNotEqual(result.returncode, 0)
        error = json.loads(result.stderr.decode("utf-8"))
        self.assertEqual(sorted(error), ["error", "ok"])
        self.assertFalse(error["ok"])
        self.assertTrue(error["error"].startswith("invalid tenant"))
        result = self.run_cli("list", "--tenant", "a b")
        self.assertNotEqual(result.returncode, 0)
        result = self.run_cli("list")
        self.assertEqual(self.document(result)["items"], [])


if __name__ == "__main__":
    unittest.main()

"""Tests for multi-tenant object namespaces across the three front ends."""

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


class TestTenantIsolation(TenantTestCase):
    def test_same_key_in_two_tenants_is_independent(self):
        self.store.put("docs/a.txt", b"alpha")
        self.store.put("docs/a.txt", b"beta", tenant="team-b")
        self.assertEqual(self.store.get("docs/a.txt")[0], b"alpha")
        self.assertEqual(self.store.get("docs/a.txt", tenant="team-b")[0], b"beta")
        self.store.delete("docs/a.txt", tenant="team-b")
        self.assertEqual(self.store.get("docs/a.txt")[0], b"alpha")
        with self.assertRaises(ObjectStoreError):
            self.store.get("docs/a.txt", tenant="team-b")

    def test_default_tenant_is_used_when_omitted(self):
        entry = self.store.put("k", b"v")
        self.assertEqual(self.store.get("k", tenant="default")[0], b"v")
        self.assertEqual(self.store.head("k"), entry)
        self.store.delete("k", tenant="default")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_missing_key_stays_missing_even_if_other_tenant_has_it(self):
        self.store.put("k", b"v", tenant="team-b")
        with self.assertRaisesRegex(ObjectStoreError, "not found"):
            self.store.get("k")
        with self.assertRaisesRegex(ObjectStoreError, "not found"):
            self.store.head("k")
        with self.assertRaisesRegex(ObjectStoreError, "not found"):
            self.store.delete("k")

    def test_listing_is_scoped_and_reports_raw_keys(self):
        self.store.put("shared/1", b"a")
        self.store.put("shared/2", b"b")
        self.store.put("other", b"c")
        self.store.put("shared/1", b"x", tenant="team-b")
        page = self.store.list_objects(prefix="shared/", tenant="team-b")
        self.assertEqual([item["key"] for item in page["items"]], ["shared/1"])
        self.assertIsNone(page["next_after"])
        default_keys = [item["key"] for item in self.store.list_objects()["items"]]
        self.assertEqual(default_keys, ["other", "shared/1", "shared/2"])

    def test_listing_paginates_within_the_tenant(self):
        for index in range(3):
            self.store.put("k%d" % index, b"v", tenant="team-b")
        for index in range(5):
            self.store.put("k%d" % index, b"v")
        first = self.store.list_objects(limit=2, tenant="team-b")
        self.assertEqual([item["key"] for item in first["items"]], ["k0", "k1"])
        second = self.store.list_objects(after=first["next_after"], limit=2,
                                         tenant="team-b")
        self.assertEqual([item["key"] for item in second["items"]], ["k2"])
        self.assertIsNone(second["next_after"])

    def test_tenant_without_objects_lists_an_empty_page(self):
        self.assertEqual(self.store.list_objects(tenant="ghost"),
                         {"items": [], "next_after": None})

    def test_tenants_need_no_prior_creation(self):
        entry = self.store.put("k", b"v", tenant="brand-new")
        self.assertEqual(self.store.get("k", tenant="brand-new")[1], entry)

    def test_condition_compares_only_the_own_tenant(self):
        self.store.put("k", b"v1")
        # the default tenant's digest must not satisfy team-b's condition
        with self.assertRaisesRegex(ObjectStoreError, "precondition failed"):
            self.store.put("k", b"x", expected_sha256=sha(b"v1"), tenant="team-b")
        # and team-b's absent state must not block the default tenant
        with self.assertRaisesRegex(ObjectStoreError, "precondition failed"):
            self.store.put("k", b"x", expected_sha256=None)
        entry = self.store.put("k", b"v2", expected_sha256=None, tenant="team-b")
        with self.assertRaisesRegex(ObjectStoreError, "precondition failed"):
            self.store.delete("k", expected_sha256=entry["sha256"])
        self.store.delete("k", expected_sha256=entry["sha256"], tenant="team-b")
        self.assertEqual(self.store.get("k")[0], b"v1")

    def test_same_content_is_stored_once_across_tenants(self):
        self.store.put("a", b"shared-bytes")
        self.store.put("b", b"shared-bytes", tenant="team-b")
        self.assertEqual(self.store.blob_digests(), [sha(b"shared-bytes")])

    def test_gc_counts_references_from_every_tenant(self):
        self.store.put("a", b"shared-bytes")
        self.store.put("b", b"shared-bytes", tenant="team-b")
        self.store.delete("a")
        self.assertEqual(self.store.collect_garbage()["digests"], [])
        self.store.delete("b", tenant="team-b")
        result = self.store.collect_garbage()
        self.assertEqual(result["digests"], [sha(b"shared-bytes")])
        self.assertEqual(self.store.blob_digests(), [sha(b"shared-bytes")])

    def test_tenant_state_survives_reopen(self):
        self.store.put("k", b"v-default")
        self.store.put("k", b"v-b", tenant="team-b")
        self.store.put("gone", b"x", tenant="team-c")
        self.store.delete("gone", tenant="team-c")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("k")[0], b"v-default")
        self.assertEqual(reopened.get("k", tenant="team-b")[0], b"v-b")
        self.assertEqual(reopened.list_objects(tenant="team-c"),
                         {"items": [], "next_after": None})


class TestTenantNames(TenantTestCase):
    GOOD = ["default", "a", "0", "team-b", "team_b", "a1-_",
            "x" * 64, "9lives"]
    BAD = ["", "A", "Team", "-lead", "_lead", "has space", "has/slash",
           "has.dot", "x" * 65, "ünïcode", "a" * 64 + "-", None, 5,
           "default\x00x"]

    def test_valid_names_are_accepted(self):
        for tenant in self.GOOD:
            self.store.put("k", b"v", tenant=tenant)
            self.assertEqual(self.store.get("k", tenant=tenant)[0], b"v")
            self.assertEqual(
                [item["key"] for item in
                 self.store.list_objects(tenant=tenant)["items"]], ["k"])

    def test_invalid_names_raise_invalid_tenant(self):
        for tenant in self.BAD:
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.put("k", b"v", tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.get("k", tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.head("k", tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.delete("k", tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.list_objects(tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.begin_upload("k", 1, sha(b"x"), tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.upload_status("1" * 32, tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.append_upload("1" * 32, 0, b"x", tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.complete_upload("1" * 32, tenant=tenant)
            with self.assertRaisesRegex(ObjectStoreError, "invalid tenant"):
                self.store.abort_upload("1" * 32, tenant=tenant)

    def test_invalid_tenant_changes_nothing(self):
        self.store.put("k", b"v")
        for tenant in self.BAD:
            with self.assertRaises(ObjectStoreError):
                self.store.put("k", b"other", tenant=tenant)
        self.assertEqual(self.store.get("k")[0], b"v")
        self.assertEqual(len(self.store.list_objects()["items"]), 1)


class TestTenantUploads(TenantTestCase):
    def begin(self, key="k", data=b"hello world", tenant="default", **kwargs):
        return self.store.begin_upload(key, len(data), sha(data), tenant=tenant,
                                       **kwargs)

    def test_session_is_pinned_to_its_tenant(self):
        session_id = self.begin(tenant="team-b")
        self.store.append_upload(session_id, 0, b"hello ", tenant="team-b")
        for method in (
                lambda: self.store.upload_status(session_id),
                lambda: self.store.append_upload(session_id, 6, b"world"),
                lambda: self.store.complete_upload(session_id),
                lambda: self.store.abort_upload(session_id),
                lambda: self.store.upload_status(session_id, tenant="team-c"),
                lambda: self.store.append_upload(session_id, 6, b"world",
                                                 tenant="team-c"),
                lambda: self.store.complete_upload(session_id, tenant="team-c"),
                lambda: self.store.abort_upload(session_id, tenant="team-c")):
            with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
                method()
        # progress is untouched by the cross-tenant probes
        status = self.store.upload_status(session_id, tenant="team-b")
        self.assertEqual(status["offset"], 6)
        self.assertFalse(status["completed"])
        self.store.append_upload(session_id, 6, b"world", tenant="team-b")
        entry = self.store.complete_upload(session_id, tenant="team-b")
        self.assertEqual(entry["sha256"], sha(b"hello world"))
        self.assertEqual(self.store.get("k", tenant="team-b")[0], b"hello world")
        with self.assertRaisesRegex(ObjectStoreError, "not found"):
            self.store.get("k")

    def test_completed_session_is_invisible_to_other_tenants(self):
        session_id = self.begin(tenant="team-b")
        self.store.append_upload(session_id, 0, b"hello world", tenant="team-b")
        entry = self.store.complete_upload(session_id, tenant="team-b")
        self.assertEqual(
            self.store.complete_upload(session_id, tenant="team-b"), entry)
        self.assertEqual(
            self.store.upload_status(session_id, tenant="team-b")["completed"],
            True)
        for method in (
                lambda: self.store.upload_status(session_id),
                lambda: self.store.complete_upload(session_id),
                lambda: self.store.abort_upload(session_id)):
            with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
                method()
        # aborting through the owning tenant still works
        self.assertIsNone(self.store.abort_upload(session_id, tenant="team-b"))
        with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
            self.store.upload_status(session_id, tenant="team-b")

    def test_cross_tenant_session_id_matches_unknown_session_error(self):
        session_id = self.begin(tenant="team-b")
        unknown = "f" * 32
        for bad in (session_id, unknown):
            with self.assertRaises(ObjectStoreError) as caught:
                self.store.upload_status(bad)
            self.assertEqual(str(caught.exception),
                             "unknown upload session: %r" % (bad,))

    def test_completion_condition_checks_only_the_owning_tenant(self):
        self.store.put("k", b"default-value")
        session_id = self.begin(data=b"tenant-value", tenant="team-b",
                                expected_sha256=None)
        self.store.append_upload(session_id, 0, b"tenant-value", tenant="team-b")
        # the default tenant holding the key must not fail team-b's condition
        entry = self.store.complete_upload(session_id, tenant="team-b")
        self.assertEqual(entry["sha256"], sha(b"tenant-value"))
        self.assertEqual(self.store.get("k")[0], b"default-value")
        self.assertEqual(self.store.get("k", tenant="team-b")[0], b"tenant-value")

    def test_failed_completion_condition_keeps_confirmed_bytes(self):
        self.store.put("k", b"old", tenant="team-b")
        session_id = self.begin(data=b"new-value", tenant="team-b",
                                expected_sha256=None)
        self.store.append_upload(session_id, 0, b"new-value", tenant="team-b")
        with self.assertRaisesRegex(ObjectStoreError, "precondition failed"):
            self.store.complete_upload(session_id, tenant="team-b")
        self.assertEqual(
            self.store.upload_status(session_id, tenant="team-b")["offset"],
            len(b"new-value"))
        self.assertEqual(self.store.get("k", tenant="team-b")[0], b"old")
        self.store.delete("k", tenant="team-b")
        entry = self.store.complete_upload(session_id, tenant="team-b")
        self.assertEqual(entry["sha256"], sha(b"new-value"))

    def test_repeated_completion_does_not_undo_later_writes(self):
        session_id = self.begin(data=b"first", tenant="team-b")
        self.store.append_upload(session_id, 0, b"first", tenant="team-b")
        entry = self.store.complete_upload(session_id, tenant="team-b")
        self.store.put("k", b"second", tenant="team-b")
        self.assertEqual(
            self.store.complete_upload(session_id, tenant="team-b"), entry)
        self.assertEqual(self.store.get("k", tenant="team-b")[0], b"second")
        self.store.delete("k", tenant="team-b")
        self.assertEqual(
            self.store.complete_upload(session_id, tenant="team-b"), entry)
        with self.assertRaisesRegex(ObjectStoreError, "not found"):
            self.store.get("k", tenant="team-b")

    def test_tenant_attribution_and_progress_survive_reopen(self):
        pending = self.begin(data=b"pending-data", tenant="team-b")
        self.store.append_upload(pending, 0, b"pending", tenant="team-b")
        done = self.begin(key="done", data=b"done", tenant="team-b")
        self.store.append_upload(done, 0, b"done", tenant="team-b")
        entry = self.store.complete_upload(done, tenant="team-b")
        reopened = ContentAddressedStore(self.root)
        with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
            reopened.upload_status(pending)
        self.assertEqual(
            reopened.upload_status(pending, tenant="team-b")["offset"], 7)
        reopened.append_upload(pending, 7, b"-data", tenant="team-b")
        self.assertEqual(reopened.complete_upload(pending, tenant="team-b")["sha256"],
                         sha(b"pending-data"))
        self.assertEqual(reopened.complete_upload(done, tenant="team-b"), entry)
        with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
            reopened.upload_status(done)


class TestLegacyDataMigration(TenantTestCase):
    def test_legacy_index_objects_land_in_default(self):
        self.store.put("legacy", b"old")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(sorted(document), ["objects", "version"])
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("legacy")[0], b"old")
        self.assertEqual(reopened.list_objects(tenant="team-b"),
                         {"items": [], "next_after": None})

    def test_legacy_session_and_record_land_in_default(self):
        session_id = self.store.begin_upload("k", 2, sha(b"ab"))
        self.store.append_upload(session_id, 0, b"ab")
        entry = self.store.complete_upload(session_id)
        with open(self.store.index_path, "rb") as handle:
            record = json.loads(
                handle.read().decode("utf-8"))["uploads"][session_id]
        self.assertEqual(sorted(record), ["entry", "key"])
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.complete_upload(session_id), entry)
        with self.assertRaisesRegex(ObjectStoreError, "unknown upload session"):
            reopened.upload_status(session_id, tenant="team-b")

    def test_non_default_tenant_objects_are_stored_under_tenants(self):
        self.store.put("k", b"v", tenant="team-b")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(document["objects"], {})
        self.assertEqual(document["tenants"]["team-b"]["k"]["sha256"], sha(b"v"))
        # deleting the last key of a tenant removes its map again
        self.store.delete("k", tenant="team-b")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertNotIn("tenants", document)


class HTTPTenantTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-http-tenant-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()

    def tearDown(self):
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

    def tenant(self, name):
        return {"X-Objstore-Tenant": name}


class TestHTTPTenants(HTTPTenantTestCase):
    def test_object_crud_is_scoped_by_header(self):
        status, _, _ = self.request("PUT", "/v1/objects/k", b"default")
        self.assertEqual(status, 201)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"tenant-b",
                                    self.tenant("team-b"))
        self.assertEqual(status, 201)
        status, _, body = self.request("GET", "/v1/objects/k")
        self.assertEqual((status, body), (200, b"default"))
        status, _, body = self.request("GET", "/v1/objects/k",
                                       headers=self.tenant("team-b"))
        self.assertEqual((status, body), (200, b"tenant-b"))
        status, _, _ = self.request("DELETE", "/v1/objects/k",
                                    headers=self.tenant("team-b"))
        self.assertEqual(status, 204)
        status, _, _ = self.request("GET", "/v1/objects/k",
                                    headers=self.tenant("team-b"))
        self.assertEqual(status, 404)
        status, _, body = self.request("GET", "/v1/objects/k")
        self.assertEqual((status, body), (200, b"default"))

    def test_listing_is_scoped_by_header(self):
        self.request("PUT", "/v1/objects/a", b"1")
        self.request("PUT", "/v1/objects/b", b"2", self.tenant("team-b"))
        status, _, body = self.request("GET", "/v1/objects",
                                       headers=self.tenant("team-b"))
        self.assertEqual(status, 200)
        self.assertEqual([item["key"] for item in self.document(body)["items"]],
                         ["b"])
        status, _, body = self.request("GET", "/v1/objects",
                                       headers=self.tenant("ghost"))
        self.assertEqual(self.document(body), {"items": [], "next_after": None})

    def test_condition_headers_compare_within_the_tenant(self):
        self.request("PUT", "/v1/objects/k", b"v1")
        quoted = '"%s"' % sha(b"v1")
        status, _, _ = self.request("PUT", "/v1/objects/k", b"x",
                                    {"If-Match": quoted, "X-Objstore-Tenant": "tb"})
        self.assertEqual(status, 412)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"x",
                                    {"If-None-Match": "*"})
        self.assertEqual(status, 412)
        status, _, _ = self.request("PUT", "/v1/objects/k", b"x",
                                    {"If-None-Match": "*", "X-Objstore-Tenant": "tb"})
        self.assertEqual(status, 201)

    def test_invalid_and_repeated_tenant_headers_get_400(self):
        for value in ("", "Bad", "-x", "x" * 65, "a b"):
            status, _, body = self.request(
                "GET", "/v1/objects/k", headers={"X-Objstore-Tenant": value})
            self.assertEqual(status, 400, value)
            self.assertIn("invalid tenant", self.document(body)["error"])
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            conn.putrequest("GET", "/v1/objects/k")
            conn.putheader("X-Objstore-Tenant", "team-b")
            conn.putheader("X-Objstore-Tenant", "team-c")
            conn.endheaders()
            response = conn.getresponse()
            body = response.read()
            self.assertEqual(response.status, 400)
            self.assertIn("invalid tenant", json.loads(body.decode("utf-8"))["error"])
        finally:
            conn.close()

    def test_invalid_tenant_takes_precedence(self):
        # an invalid key plus an invalid tenant reports the tenant first
        status, _, body = self.request(
            "GET", "/v1/objects/bad//key", headers={"X-Objstore-Tenant": "Bad"})
        self.assertEqual(status, 400)
        self.assertIn("invalid tenant", self.document(body)["error"])
        status, _, body = self.request(
            "GET", "/v1/objects?limit=nope", headers={"X-Objstore-Tenant": "Bad"})
        self.assertEqual(status, 400)
        self.assertIn("invalid tenant", self.document(body)["error"])
        status, _, body = self.request(
            "GET", "/v1/uploads/" + "1" * 32, headers={"X-Objstore-Tenant": "Bad"})
        self.assertEqual(status, 400)
        self.assertIn("invalid tenant", self.document(body)["error"])

    def test_blob_endpoint_keeps_global_semantics(self):
        self.request("PUT", "/v1/objects/k", b"blobby", self.tenant("team-b"))
        digest = sha(b"blobby")
        status, _, body = self.request("GET", "/v1/blobs/%s" % digest)
        self.assertEqual((status, body), (200, b"blobby"))
        status, _, body = self.request("GET", "/v1/blobs/%s" % digest,
                                       headers=self.tenant("team-b"))
        self.assertEqual((status, body), (200, b"blobby"))

    def test_upload_session_is_pinned_to_its_tenant(self):
        data = b"http-tenant-upload"
        declaration = json.dumps(
            {"key": "k", "size": len(data), "sha256": sha(data)}).encode("utf-8")
        status, _, body = self.request("POST", "/v1/uploads", declaration,
                                       self.tenant("team-b"))
        self.assertEqual(status, 201)
        session_id = self.document(body)["session"]
        status, _, _ = self.request(
            "PUT", "/v1/uploads/%s?offset=0" % session_id, data,
            self.tenant("team-b"))
        self.assertEqual(status, 200)
        for method, path in (
                ("GET", "/v1/uploads/%s" % session_id),
                ("POST", "/v1/uploads/%s/complete" % session_id),
                ("DELETE", "/v1/uploads/%s" % session_id)):
            status, _, body = self.request(method, path)
            self.assertEqual(status, 404, (method, path))
            self.assertIn("unknown upload session",
                          self.document(body)["error"])
        # the session is still usable by its owner
        status, _, body = self.request(
            "POST", "/v1/uploads/%s/complete" % session_id,
            headers=self.tenant("team-b"))
        self.assertEqual(status, 200)
        self.assertEqual(self.document(body)["sha256"], sha(data))
        status, _, body = self.request("GET", "/v1/objects/k",
                                       headers=self.tenant("team-b"))
        self.assertEqual((status, body), (200, data))
        status, _, _ = self.request("GET", "/v1/objects/k")
        self.assertEqual(status, 404)


class CLITenantTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="objstore-cli-tenant-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.data_dir = os.path.join(self.tmp, "data")
        os.makedirs(self.data_dir)
        self.source = os.path.join(self.tmp, "src.bin")
        with open(self.source, "wb") as handle:
            handle.write(b"cli-tenant-payload")

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "objstore", "--data-dir", self.data_dir, *args],
            capture_output=True, cwd=REPO_ROOT,
        )

    def test_tenant_option_scopes_put_get_list_delete(self):
        proc = self.run_cli("put", "--key", "k", "--file", self.source,
                            "--tenant", "team-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = os.path.join(self.tmp, "out.bin")
        proc = self.run_cli("get", "--key", "k", "--out", out,
                            "--tenant", "team-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(out, "rb") as handle:
            self.assertEqual(handle.read(), b"cli-tenant-payload")
        proc = self.run_cli("list", "--tenant", "team-b")
        self.assertEqual([item["key"] for item in
                          json.loads(proc.stdout.decode("utf-8"))["items"]], ["k"])
        # the default tenant does not see the object
        proc = self.run_cli("list")
        self.assertEqual(json.loads(proc.stdout.decode("utf-8"))["items"], [])
        proc = self.run_cli("get", "--key", "k", "--out", out)
        self.assertNotEqual(proc.returncode, 0)
        proc = self.run_cli("delete", "--key", "k", "--tenant", "team-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        proc = self.run_cli("list", "--tenant", "team-b")
        self.assertEqual(json.loads(proc.stdout.decode("utf-8"))["items"], [])

    def test_invalid_tenant_fails_without_changing_data(self):
        proc = self.run_cli("put", "--key", "k", "--file", self.source)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        proc = self.run_cli("put", "--key", "k", "--file", self.source,
                            "--tenant", "Bad Tenant")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, b"")
        error = json.loads(proc.stderr.decode("utf-8"))
        self.assertFalse(error["ok"])
        self.assertIn("invalid tenant", error["error"])
        proc = self.run_cli("list")
        self.assertEqual([item["key"] for item in
                          json.loads(proc.stdout.decode("utf-8"))["items"]], ["k"])


if __name__ == "__main__":
    unittest.main()

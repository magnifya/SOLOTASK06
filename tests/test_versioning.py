"""Tests for tenant-level object versioning and retention policies."""

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class VersioningTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-versions-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def enable(self, tenant="acme"):
        self.store.set_versioning(True, tenant=tenant)


class TestVersioningToggle(VersioningTestCase):
    def test_default_is_disabled(self):
        self.assertFalse(self.store.get_versioning())
        self.assertFalse(self.store.get_versioning(tenant="acme"))

    def test_enable_disable_roundtrip(self):
        self.store.set_versioning(True, tenant="acme")
        self.assertTrue(self.store.get_versioning(tenant="acme"))
        self.assertFalse(self.store.get_versioning())
        self.store.set_versioning(False, tenant="acme")
        self.assertFalse(self.store.get_versioning(tenant="acme"))

    def test_invalid_flag(self):
        for bad in ("true", 1, 0, None, []):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_versioning(bad, tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid versioning")

    def test_flag_survives_reopen(self):
        self.enable()
        reopened = ContentAddressedStore(self.root)
        self.assertTrue(reopened.get_versioning(tenant="acme"))

    def test_disabled_tenant_keeps_legacy_behaviour(self):
        entry = self.store.put("k", b"v1", tenant="acme")
        self.assertNotIn("version_id", entry)
        self.store.put("k", b"v2", tenant="acme")
        data, head = self.store.get("k", tenant="acme")
        self.assertEqual(data, b"v2")
        self.assertNotIn("version_id", head)
        self.store.delete("k", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k", tenant="acme")

    def test_version_calls_require_enabled(self):
        self.store.put("k", b"v1", tenant="acme")
        for call in (
            lambda: self.store.list_versions("k", tenant="acme"),
            lambda: self.store.get_version("k", "0" * 32, tenant="acme"),
            lambda: self.store.head_version("k", "0" * 32, tenant="acme"),
            lambda: self.store.delete_version("k", "0" * 32, tenant="acme"),
        ):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertEqual(str(ctx.exception), "versioning disabled")


class TestVersionedWrites(VersioningTestCase):
    def test_put_generates_version_ids(self):
        self.enable()
        first = self.store.put("docs/a", b"one", tenant="acme")
        second = self.store.put("docs/a", b"two", tenant="acme")
        self.assertRegex(first["version_id"], r"\A[0-9a-f]{32}\Z")
        self.assertRegex(second["version_id"], r"\A[0-9a-f]{32}\Z")
        self.assertNotEqual(first["version_id"], second["version_id"])

    def test_reads_and_listings_show_only_current(self):
        self.enable()
        self.store.put("docs/a", b"one", tenant="acme")
        self.store.put("docs/a", b"two", tenant="acme")
        data, head = self.store.get("docs/a", tenant="acme")
        self.assertEqual(data, b"two")
        self.assertEqual(head["sha256"], sha(b"two"))
        page = self.store.list_objects(tenant="acme")
        self.assertEqual([item["sha256"] for item in page["items"]], [sha(b"two")])

    def test_complete_upload_creates_version(self):
        self.enable()
        session = self.store.begin_upload("big", 3, sha(b"abc"), tenant="acme")
        self.store.append_upload(session, 0, b"abc", tenant="acme")
        entry = self.store.complete_upload(session, tenant="acme")
        self.assertIn("version_id", entry)
        again = self.store.complete_upload(session, tenant="acme")
        self.assertEqual(again, entry)
        versions = self.store.list_versions("big", tenant="acme")
        self.assertEqual(len(versions["items"]), 1)
        self.assertEqual(versions["items"][0]["version_id"], entry["version_id"])

    def test_list_versions_newest_first_with_current_flag(self):
        self.enable()
        ids = [self.store.put("k", b"v%d" % i, tenant="acme")["version_id"]
               for i in range(3)]
        page = self.store.list_versions("k", tenant="acme")
        self.assertIsNone(page["next_after"])
        self.assertEqual([item["version_id"] for item in page["items"]],
                         list(reversed(ids)))
        self.assertEqual([item["current"] for item in page["items"]],
                         [True, False, False])
        newest = page["items"][0]
        self.assertEqual(newest["sha256"], sha(b"v2"))
        self.assertEqual(newest["size"], 2)
        datetime.fromisoformat(newest["created_at"])  # parses as a timestamp

    def test_list_versions_pagination(self):
        self.enable()
        ids = [self.store.put("k", b"v%d" % i, tenant="acme")["version_id"]
               for i in range(4)]
        first = self.store.list_versions("k", limit=2, tenant="acme")
        self.assertEqual([i["version_id"] for i in first["items"]],
                         [ids[3], ids[2]])
        self.assertEqual(first["next_after"], ids[2])
        second = self.store.list_versions("k", after=first["next_after"],
                                          limit=2, tenant="acme")
        self.assertEqual([i["version_id"] for i in second["items"]],
                         [ids[1], ids[0]])
        self.assertIsNone(second["next_after"])

    def test_list_versions_unknown_key(self):
        self.enable()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("missing", tenant="acme")
        self.assertIn("not found", str(ctx.exception))

    def test_list_versions_bad_after(self):
        self.enable()
        self.store.put("k", b"v", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("k", after="0" * 32, tenant="acme")
        self.assertEqual(str(ctx.exception), "invalid version")

    def test_tenants_are_isolated(self):
        self.enable("acme")
        self.store.put("k", b"acme", tenant="acme")
        self.store.put("k", b"other", tenant="other")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("k", tenant="other")
        self.assertEqual(str(ctx.exception), "versioning disabled")
        data, _ = self.store.get("k", tenant="other")
        self.assertEqual(data, b"other")


class TestVersionReadsAndDeletes(VersioningTestCase):
    def setUp(self):
        super().setUp()
        self.enable()
        self.ids = [self.store.put("k", b"v%d" % i, tenant="acme")["version_id"]
                    for i in range(3)]

    def test_get_version_returns_historical_bytes(self):
        data, entry = self.store.get_version("k", self.ids[0], tenant="acme")
        self.assertEqual(data, b"v0")
        self.assertEqual(entry["version_id"], self.ids[0])
        self.assertFalse(entry["current"])
        self.assertEqual(entry["sha256"], sha(b"v0"))

    def test_head_version(self):
        entry = self.store.head_version("k", self.ids[2], tenant="acme")
        self.assertTrue(entry["current"])
        self.assertEqual(entry["size"], 2)

    def test_get_version_validates_blob(self):
        os.remove(os.path.join(self.store.blobs_dir, sha(b"v0")))
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", self.ids[0], tenant="acme")
        self.assertIn("not found", str(ctx.exception))

    def test_invalid_version_id(self):
        for bad in ("xyz", "0" * 31, "0" * 33, None, 42):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.head_version("k", bad, tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid version")

    def test_unknown_version_id(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head_version("k", "0" * 32, tenant="acme")
        self.assertIn("not found", str(ctx.exception))

    def test_delete_removes_current_and_promotes(self):
        self.store.delete("k", tenant="acme")
        data, head = self.store.get("k", tenant="acme")
        self.assertEqual(data, b"v1")
        versions = self.store.list_versions("k", tenant="acme")
        self.assertEqual([i["version_id"] for i in versions["items"]],
                         [self.ids[1], self.ids[0]])
        self.assertTrue(versions["items"][0]["current"])

    def test_delete_down_to_no_versions_removes_key(self):
        for _ in range(3):
            self.store.delete("k", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.list_versions("k", tenant="acme")

    def test_delete_version_keeps_current(self):
        self.store.delete_version("k", self.ids[0], tenant="acme")
        data, head = self.store.get("k", tenant="acme")
        self.assertEqual(data, b"v2")
        versions = self.store.list_versions("k", tenant="acme")
        self.assertEqual([i["version_id"] for i in versions["items"]],
                         [self.ids[2], self.ids[1]])

    def test_delete_current_version_promotes(self):
        self.store.delete_version("k", self.ids[2], tenant="acme")
        data, _ = self.store.get("k", tenant="acme")
        self.assertEqual(data, b"v1")

    def test_delete_last_version_removes_key(self):
        for version_id in list(self.ids):
            self.store.delete_version("k", version_id, tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k", tenant="acme")

    def test_versions_survive_reopen(self):
        reopened = ContentAddressedStore(self.root)
        page = reopened.list_versions("k", tenant="acme")
        self.assertEqual([i["version_id"] for i in page["items"]],
                         list(reversed(self.ids)))
        data, _ = reopened.get_version("k", self.ids[0], tenant="acme")
        self.assertEqual(data, b"v0")

    def test_disable_keeps_history(self):
        self.store.set_versioning(False, tenant="acme")
        self.store.put("k", b"plain", tenant="acme")  # no new version
        self.store.set_versioning(True, tenant="acme")
        page = self.store.list_versions("k", tenant="acme")
        self.assertEqual(len(page["items"]), 3)
        data, _ = self.store.get_version("k", self.ids[0], tenant="acme")
        self.assertEqual(data, b"v0")


class TestRetention(VersioningTestCase):
    def test_policy_roundtrip(self):
        self.assertIsNone(self.store.get_retention(tenant="acme"))
        self.store.set_retention(max_versions=3, tenant="acme")
        self.assertEqual(self.store.get_retention(tenant="acme"),
                         {"max_versions": 3, "max_age_seconds": None})
        self.store.set_retention(max_age_seconds=60, tenant="acme")
        self.assertEqual(self.store.get_retention(tenant="acme"),
                         {"max_versions": None, "max_age_seconds": 60})
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_retention(tenant="acme"),
                         {"max_versions": None, "max_age_seconds": 60})

    def test_invalid_policy(self):
        for kwargs in ({}, {"max_versions": -1}, {"max_age_seconds": -1},
                       {"max_versions": True}, {"max_versions": "3"},
                       {"max_age_seconds": 1.5}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_retention(tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "invalid retention")

    def test_purge_requires_boolean_dry_run(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.purge_retention(dry_run="yes", tenant="acme")
        self.assertEqual(str(ctx.exception), "invalid dry_run")

    def test_purge_without_policy_is_empty(self):
        result = self.store.purge_retention(tenant="acme")
        self.assertEqual(result, {"versions": [], "bytes": 0, "dry_run": True})

    def test_purge_max_versions_keeps_newest_and_current(self):
        self.enable()
        ids = [self.store.put("k", b"v%d" % i, tenant="acme")["version_id"]
               for i in range(5)]
        self.store.set_retention(max_versions=2, tenant="acme")
        preview = self.store.purge_retention(tenant="acme")
        self.assertTrue(preview["dry_run"])
        self.assertEqual([v["version_id"] for v in preview["versions"]],
                         sorted(ids[:3]))
        self.assertEqual(preview["bytes"], 6)
        # dry run changed nothing
        self.assertEqual(len(self.store.list_versions("k", tenant="acme")["items"]), 5)
        result = self.store.purge_retention(dry_run=False, tenant="acme")
        self.assertFalse(result["dry_run"])
        self.assertEqual([v["version_id"] for v in result["versions"]],
                         sorted(ids[:3]))
        page = self.store.list_versions("k", tenant="acme")
        self.assertEqual([i["version_id"] for i in page["items"]],
                         [ids[4], ids[3]])
        data, _ = self.store.get("k", tenant="acme")
        self.assertEqual(data, b"v4")

    def test_purge_max_age_keeps_current(self):
        self.enable()
        old_id = self.store.put("k", b"old", tenant="acme")["version_id"]
        new_id = self.store.put("k", b"new", tenant="acme")["version_id"]
        # Backdate the older version beyond the policy horizon.
        stale = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
        self.store._versions["acme"]["k"][0]["created_at"] = stale
        self.store._save_index()
        self.store.set_retention(max_age_seconds=60, tenant="acme")
        result = self.store.purge_retention(dry_run=False, tenant="acme")
        self.assertEqual([v["version_id"] for v in result["versions"]], [old_id])
        page = self.store.list_versions("k", tenant="acme")
        self.assertEqual([i["version_id"] for i in page["items"]], [new_id])

    def test_purge_never_removes_current(self):
        self.enable()
        only = self.store.put("k", b"only", tenant="acme")["version_id"]
        stale = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        self.store._versions["acme"]["k"][0]["created_at"] = stale
        self.store._save_index()
        self.store.set_retention(max_versions=0, max_age_seconds=0, tenant="acme")
        result = self.store.purge_retention(dry_run=False, tenant="acme")
        self.assertEqual(result["versions"], [])
        self.assertEqual(self.store.list_versions("k", tenant="acme")["items"][0]
                         ["version_id"], only)


class TestVersionGC(VersioningTestCase):
    def test_gc_keeps_version_referenced_blobs(self):
        self.enable()
        self.store.put("k", b"one", tenant="acme")
        self.store.put("k", b"two", tenant="acme")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], [])
        self.assertIn(sha(b"one"), self.store.blob_digests())

    def test_gc_collects_after_version_delete(self):
        self.enable()
        first = self.store.put("k", b"one", tenant="acme")
        self.store.put("k", b"two", tenant="acme")
        self.store.delete_version("k", first["version_id"], tenant="acme")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], [sha(b"one")])

    def test_gc_keeps_incomplete_upload_digest(self):
        digest = sha(b"pending")
        self.store.begin_upload("k", 7, digest, tenant="acme")
        blob_path = os.path.join(self.store.blobs_dir, digest)
        with open(blob_path, "wb") as handle:
            handle.write(b"pending")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], [])


class TestIndexCompatibility(unittest.TestCase):
    def test_old_index_gains_no_versions(self):
        root = tempfile.mkdtemp(prefix="objstore-oldidx-")
        try:
            os.makedirs(os.path.join(root, "blobs"), exist_ok=True)
            payload = b"legacy"
            document = {"version": 1, "objects": {
                "k": {"sha256": sha(payload), "size": len(payload),
                      "content_type": None}}}
            with open(os.path.join(root, "index.json"), "w") as handle:
                json.dump(document, handle)
            with open(os.path.join(root, "blobs", sha(payload)), "wb") as handle:
                handle.write(payload)
            store = ContentAddressedStore(root)
            self.assertFalse(store.get_versioning())
            self.assertIsNone(store.get_retention())
            store.set_versioning(True)
            with self.assertRaises(ObjectStoreError):
                store.list_versions("k")
            data, _ = store.get("k")
            self.assertEqual(data, payload)
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

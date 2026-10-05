"""Tests for per-tenant lifecycle policies and last-modified timestamps."""

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


def utc_now():
    return datetime.now(timezone.utc)


class LifecycleTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-lifecycle-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def backdate(self, key, seconds=3600, tenant="default"):
        old = (utc_now() - timedelta(seconds=seconds)).isoformat()
        self.store._timestamps.setdefault(tenant, {})[key] = old
        return old

    def write_index(self, document):
        with open(os.path.join(self.root, "index.json"), "w") as handle:
            handle.write(json.dumps(document))

    def seed_legacy_object(self, key, payload, tenant=None):
        """Write a blob plus an old-style index entry without last_modified."""
        digest = sha(payload)
        with open(os.path.join(self.root, "blobs", digest), "wb") as handle:
            handle.write(payload)
        document = {"version": 1, "objects": {}}
        if tenant is None:
            document["objects"][key] = {
                "sha256": digest, "size": len(payload), "content_type": None}
        else:
            document["tenants"] = {tenant: {key: {
                "sha256": digest, "size": len(payload),
                "content_type": None}}}
        self.write_index(document)


class TestLastModified(LifecycleTestCase):
    def test_public_entry_shape_is_unchanged(self):
        entry = self.store.put("a", b"hello", content_type="text/plain")
        self.assertEqual(entry,
                         {"sha256": sha(b"hello"), "size": 5,
                          "content_type": "text/plain"})
        self.assertEqual(sorted(entry), ["content_type", "sha256", "size"])
        self.assertEqual(sorted(self.store.head("a")),
                         ["content_type", "sha256", "size"])
        _, got = self.store.get("a")
        self.assertEqual(sorted(got), ["content_type", "sha256", "size"])

    def test_timestamp_is_persisted_in_index(self):
        before = utc_now()
        self.store.put("a", b"hello", tenant="acme")
        after = utc_now()
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        raw = document["tenants"]["acme"]["a"]["last_modified"]
        saved = datetime.fromisoformat(raw)
        self.assertIn("last_modified", document["tenants"]["acme"]["a"])
        self.assertTrue(before <= saved <= after)

    def test_timestamp_survives_reopen(self):
        self.store.put("a", b"hello", tenant="acme")
        with open(self.store.index_path, "rb") as handle:
            raw = json.loads(handle.read().decode("utf-8"))
        raw = raw["tenants"]["acme"]["a"]
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened._timestamps["acme"]["a"],
                         raw["last_modified"])

    def test_overwrite_refreshes_timestamp(self):
        self.store.put("k", b"old")
        first = self.store._timestamps["default"]["k"]
        self.backdate("k", seconds=10000)
        self.store.put("k", b"new")
        refreshed = self.store._timestamps["default"]["k"]
        self.assertNotEqual(refreshed, first)
        saved = datetime.fromisoformat(refreshed)
        self.assertTrue(utc_now() - saved < timedelta(seconds=10))

    def test_completed_upload_records_timestamp(self):
        session = self.store.begin_upload("up", 3, sha(b"abc"))
        self.store.append_upload(session, 0, b"abc")
        self.store.complete_upload(session)
        self.assertIn("up", self.store._timestamps["default"])
        reopened = ContentAddressedStore(self.root)
        self.assertIn("up", reopened._timestamps["default"])

    def test_legacy_entries_without_timestamp_load_and_stay_timeless(self):
        self.seed_legacy_object("old", b"old")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("old")[0], b"old")
        self.assertEqual(reopened._timestamps.get("default", {}), {})
        # A new write sits next to the timeless object without gaining a time.
        reopened.put("new", b"new")
        self.assertIsNone(reopened._timestamps["default"].get("old"))
        self.assertIsNotNone(reopened._timestamps["default"].get("new"))

    def test_bad_saved_timestamp_is_corrupted_index(self):
        for bad in (123, "not-a-timestamp"):
            self.write_index({"version": 1, "objects": {
                "k": {"sha256": sha(b"x"), "size": 1, "content_type": None,
                      "last_modified": bad}}})
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"), bad)


class TestLifecyclePolicy(LifecycleTestCase):
    def test_default_is_unset(self):
        self.assertIsNone(self.store.get_lifecycle())
        self.assertIsNone(self.store.get_lifecycle(tenant="acme"))

    def test_set_and_get_roundtrip(self):
        self.store.set_lifecycle(100, prefix="tmp/", tenant="acme")
        self.assertEqual(self.store.get_lifecycle(tenant="acme"),
                         {"prefix": "tmp/", "max_age_seconds": 100})
        self.assertIsNone(self.store.get_lifecycle())

    def test_prefix_defaults_to_empty_string(self):
        self.store.set_lifecycle(0, tenant="acme")
        self.assertEqual(self.store.get_lifecycle(tenant="acme"),
                         {"prefix": "", "max_age_seconds": 0})

    def test_set_replaces_policy(self):
        self.store.set_lifecycle(100, prefix="a/", tenant="acme")
        self.store.set_lifecycle(5, tenant="acme")
        self.assertEqual(self.store.get_lifecycle(tenant="acme"),
                         {"prefix": "", "max_age_seconds": 5})

    def test_invalid_lifecycle(self):
        for args, kwargs in (
                ((None,), {}),
                ((-1,), {}),
                ((True,), {}),
                ((False,), {}),
                ((1.5,), {}),
                (("10",), {}),
                (([1],), {}),
                ((10,), {"prefix": 5}),
                ((10,), {"prefix": b"x"}),
                ((10,), {"prefix": None}),
        ):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_lifecycle(*args, tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "invalid lifecycle", args)
        self.assertIsNone(self.store.get_lifecycle(tenant="acme"))

    def test_invalid_tenant(self):
        for call in (lambda: self.store.set_lifecycle(1, tenant="BAD!"),
                     lambda: self.store.get_lifecycle(tenant="BAD!"),
                     lambda: self.store.clear_lifecycle(tenant="BAD!"),
                     lambda: self.store.run_lifecycle(tenant="BAD!")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen(self):
        self.store.set_lifecycle(60, prefix="logs/", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_lifecycle(tenant="acme"),
                         {"prefix": "logs/", "max_age_seconds": 60})

    def test_clear_removes_policy(self):
        self.store.set_lifecycle(60, tenant="acme")
        self.assertIsNone(self.store.clear_lifecycle(tenant="acme"))
        self.assertIsNone(self.store.get_lifecycle(tenant="acme"))

    def test_clear_without_policy_is_noop(self):
        self.assertIsNone(self.store.clear_lifecycle(tenant="acme"))
        self.assertIsNone(self.store.get_lifecycle(tenant="acme"))

    def test_corrupted_lifecycle_in_index(self):
        for bad in (
                {"lifecycle": []},
                {"lifecycle": {"acme": {}}},
                {"lifecycle": {"acme": None}},
                {"lifecycle": {"acme": {"max_age_seconds": -1}}},
                {"lifecycle": {"acme": {"max_age_seconds": True}}},
                {"lifecycle": {"acme": {"max_age_seconds": "60"}}},
                {"lifecycle": {"acme": {"prefix": 1, "max_age_seconds": 1}}},
                {"lifecycle": {"BAD TENANT": {"max_age_seconds": 1}}}):
            document = {"version": 1, "objects": {}}
            document.update(bad)
            self.write_index(document)
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"), bad)

    def test_io_error_keeps_previous_policy(self):
        self.store.set_lifecycle(100, tenant="acme")
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_lifecycle(50, tenant="acme")
            self.assertEqual(str(ctx.exception), "lifecycle io error")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.clear_lifecycle(tenant="acme")
            self.assertEqual(str(ctx.exception), "lifecycle io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_lifecycle(tenant="acme"),
                         {"prefix": "", "max_age_seconds": 100})


class TestRunLifecycle(LifecycleTestCase):
    def test_no_policy_returns_empty_result(self):
        self.store.put("a", b"x")
        self.assertEqual(self.store.run_lifecycle(tenant="acme"),
                         {"objects": [], "bytes": 0, "dry_run": True})
        self.assertEqual(self.store.run_lifecycle(dry_run=False, tenant="acme"),
                         {"objects": [], "bytes": 0, "dry_run": False})

    def test_invalid_dry_run(self):
        for value in ("yes", 1, 0, None):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.run_lifecycle(dry_run=value, tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid dry_run", value)

    def test_candidates_sorted_by_key_with_totals(self):
        self.store.set_lifecycle(0, tenant="acme")
        self.store.put("c", b"ccc", tenant="acme")
        self.store.put("a", b"a", tenant="acme")
        self.store.put("b", b"bb", tenant="acme")
        result = self.store.run_lifecycle(tenant="acme")
        self.assertTrue(result["dry_run"])
        self.assertEqual([item["key"] for item in result["objects"]],
                         ["a", "b", "c"])
        self.assertEqual(result["objects"][0],
                         {"key": "a", "sha256": sha(b"a"), "size": 1})
        self.assertEqual(result["bytes"], 6)
        # A preview changes nothing.
        self.assertEqual(
            [item["key"] for item in
             self.store.list_objects(tenant="acme")["items"]],
            ["a", "b", "c"])

    def test_prefix_selects_candidates(self):
        self.store.set_lifecycle(0, prefix="tmp/", tenant="acme")
        self.store.put("tmp/a", b"1", tenant="acme")
        self.store.put("tmp-b", b"2", tenant="acme")
        self.store.put("other", b"333", tenant="acme")
        result = self.store.run_lifecycle(tenant="acme")
        self.assertEqual([item["key"] for item in result["objects"]], ["tmp/a"])
        self.assertEqual(result["bytes"], 1)

    def test_max_age_cutoff(self):
        self.store.set_lifecycle(60, tenant="acme")
        self.store.put("fresh", b"1", tenant="acme")
        self.store.put("stale", b"22", tenant="acme")
        self.backdate("stale", seconds=120, tenant="acme")
        result = self.store.run_lifecycle(tenant="acme")
        self.assertEqual([item["key"] for item in result["objects"]], ["stale"])
        self.assertEqual(result["bytes"], 2)

    def test_timeless_legacy_objects_are_always_skipped(self):
        self.seed_legacy_object("old", b"old")
        store = ContentAddressedStore(self.root)
        store.put("young", b"y")
        store.set_lifecycle(0)
        result = store.run_lifecycle(dry_run=False)
        self.assertEqual([item["key"] for item in result["objects"]], ["young"])
        self.assertEqual(store.get("old")[0], b"old")
        with self.assertRaises(ObjectStoreError):
            store.head("young")
        # The legacy entry still carries no timestamp after the rewrite.
        with open(store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertNotIn("last_modified", document["objects"]["old"])

    def test_actual_run_removes_unversioned_mappings(self):
        self.store.set_lifecycle(60, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"22", tenant="acme")
        self.backdate("a", tenant="acme")
        self.backdate("b", tenant="acme")
        preview = self.store.run_lifecycle(dry_run=True, tenant="acme")
        result = self.store.run_lifecycle(dry_run=False, tenant="acme")
        self.assertFalse(result["dry_run"])
        self.assertEqual(result["objects"], preview["objects"])
        self.assertEqual(result["bytes"], preview["bytes"])
        with self.assertRaises(ObjectStoreError):
            self.store.head("a", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("b", tenant="acme")
        self.assertEqual(self.store._timestamps.get("acme", {}), {})
        # A second run has nothing left to do.
        self.assertEqual(self.store.run_lifecycle(dry_run=False, tenant="acme"),
                         {"objects": [], "bytes": 0, "dry_run": False})

    def test_versioned_run_deletes_current_and_promotes(self):
        self.store.set_versioning(True, tenant="acme")
        self.store.put("k", b"v1", tenant="acme")
        self.store.put("k", b"v2-longer", tenant="acme")
        self.store.set_lifecycle(0, tenant="acme")
        result = self.store.run_lifecycle(dry_run=False, tenant="acme")
        self.assertEqual(result["objects"],
                         [{"key": "k", "sha256": sha(b"v2-longer"), "size": 9}])
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"v1")
        versions = self.store.list_versions("k", tenant="acme")["items"]
        self.assertEqual(len(versions), 1)
        # The promoted version is old as well; the next run removes the key.
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k", tenant="acme")
        self.assertNotIn("k", self.store._versions.get("acme", {}))

    def test_versioned_promoted_timestamp_is_its_created_at(self):
        self.store.set_versioning(True, tenant="acme")
        first = self.store.put("k", b"v1", tenant="acme")
        self.store.put("k", b"v2", tenant="acme")
        self.store.set_lifecycle(0, tenant="acme")
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        versions = self.store.list_versions("k", tenant="acme")["items"]
        created_at = versions[0]["created_at"]
        self.assertEqual(self.store._timestamps["acme"]["k"], created_at)
        self.assertEqual(first["version_id"], versions[0]["version_id"])

    def test_blob_bytes_remain_for_garbage_collection(self):
        self.store.set_lifecycle(0, tenant="acme")
        entry = self.store.put("a", b"data", tenant="acme")
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        self.assertEqual(self.store.blob(entry["sha256"]), b"data")
        gc = self.store.collect_garbage(dry_run=False)
        self.assertEqual(gc["digests"], [entry["sha256"]])
        self.assertEqual(gc["bytes"], 4)

    def test_active_upload_sessions_are_unaffected(self):
        self.store.set_lifecycle(0, tenant="acme")
        session = self.store.begin_upload("up", 3, sha(b"abc"), tenant="acme")
        self.store.append_upload(session, 0, b"ab", tenant="acme")
        self.store.put("old", b"x", tenant="acme")
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        status = self.store.upload_status(session, tenant="acme")
        self.assertFalse(status["completed"])
        self.assertEqual(status["offset"], 2)
        self.store.append_upload(session, 2, b"c", tenant="acme")
        self.assertEqual(self.store.complete_upload(session, tenant="acme")
                         ["sha256"], sha(b"abc"))

    def test_policy_is_scoped_per_tenant(self):
        self.store.set_lifecycle(0, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("a", b"2")
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("a", tenant="acme")
        self.assertEqual(self.store.get("a")[0], b"2")
        # The default tenant has no policy and is not touched by acme's run.
        self.assertEqual(
            self.store.run_lifecycle(dry_run=False),
            {"objects": [], "bytes": 0, "dry_run": False})

    def test_quota_usage_follows_removals(self):
        self.store.set_versioning(True, tenant="acme")
        self.store.put("k", b"v1", tenant="acme")
        self.store.put("k", b"v2-longer", tenant="acme")
        self.store.set_lifecycle(0, tenant="acme")
        usage_before = self.store.get_usage(tenant="acme")
        self.assertEqual(
            (usage_before["used_bytes"], usage_before["used_objects"]),
            (11, 2))
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        usage_after = self.store.get_usage(tenant="acme")
        self.assertEqual(
            (usage_after["used_bytes"], usage_after["used_objects"]), (2, 1))

    def test_io_error_rolls_everything_back(self):
        self.store.set_lifecycle(0, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"22", tenant="acme")
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.run_lifecycle(dry_run=False, tenant="acme")
            self.assertEqual(str(ctx.exception), "lifecycle io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get("a", tenant="acme")[0], b"1")
        self.assertEqual(self.store.get("b", tenant="acme")[0], b"22")
        result = self.store.run_lifecycle(dry_run=False, tenant="acme")
        self.assertEqual([item["key"] for item in result["objects"]],
                         ["a", "b"])

    def test_persistence_after_execution(self):
        self.store.set_lifecycle(60, prefix="", tenant="acme")
        self.store.put("k", b"old", tenant="acme")
        self.backdate("k", tenant="acme")
        self.store.run_lifecycle(dry_run=False, tenant="acme")
        reopened = ContentAddressedStore(self.root)
        with self.assertRaises(ObjectStoreError):
            reopened.head("k", tenant="acme")
        self.assertEqual(reopened.get_lifecycle(tenant="acme"),
                         {"prefix": "", "max_age_seconds": 60})


if __name__ == "__main__":
    unittest.main()

"""Unit tests for tenant object versioning and retention (run from repo root)."""

import hashlib
import json
import os
import shutil
import tempfile
import unittest

from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class VersioningTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-versioning-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)

    def enable(self, tenant="default"):
        self.store.set_versioning(True, tenant=tenant)

    def put_versions(self, key, payloads, tenant="default"):
        return [self.store.put(key, payload, tenant=tenant)["version_id"]
                for payload in payloads]


class TestVersioningToggle(VersioningTestCase):
    def test_disabled_by_default(self):
        self.assertIs(self.store.get_versioning(), False)
        self.assertIs(self.store.get_versioning(tenant="acme"), False)

    def test_enable_disable_roundtrip(self):
        self.store.set_versioning(True)
        self.assertIs(self.store.get_versioning(), True)
        self.store.set_versioning(False)
        self.assertIs(self.store.get_versioning(), False)

    def test_toggle_is_per_tenant(self):
        self.store.set_versioning(True, tenant="acme")
        self.assertIs(self.store.get_versioning(tenant="acme"), True)
        self.assertIs(self.store.get_versioning(), False)

    def test_invalid_toggle_values(self):
        for bad in ("yes", 1, 0, None, [], {}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_versioning(bad)
            self.assertEqual(str(ctx.exception), "invalid versioning")
        with self.assertRaises(ObjectStoreError):
            self.store.set_versioning(True, tenant="Bad Tenant")

    def test_toggle_survives_reopen(self):
        self.store.set_versioning(True, tenant="acme")
        fresh = ContentAddressedStore(self.root)
        self.assertIs(fresh.get_versioning(tenant="acme"), True)
        self.assertIs(fresh.get_versioning(), False)

    def test_disabled_put_has_no_version_id(self):
        entry = self.store.put("k", b"hello")
        self.assertEqual(entry, {"sha256": sha(b"hello"), "size": 5,
                                 "content_type": None})

    def test_disabling_keeps_history(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"v1"])
        self.store.set_versioning(False)
        self.store.set_versioning(True)
        self.assertEqual(self.store.get_version("k", vid)[0], b"v1")


class TestVersionedWrites(VersioningTestCase):
    def test_put_returns_version_id_and_sets_current(self):
        self.enable()
        entry = self.store.put("k", b"v1", content_type="text/plain")
        self.assertEqual(entry["sha256"], sha(b"v1"))
        self.assertEqual(entry["size"], 2)
        self.assertEqual(entry["content_type"], "text/plain")
        vid = entry["version_id"]
        self.assertRegex(vid, r"\A[0-9a-f]{32}\Z")
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]], [vid])
        self.assertIs(page["items"][0]["current"], True)

    def test_every_put_appends_a_version(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2", b"v3"])
        self.assertEqual(len(set(vids)), 3)
        page = self.store.list_versions("k")
        # newest first
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[2], vids[1], vids[0]])
        self.assertEqual([item["current"] for item in page["items"]],
                         [True, False, False])
        self.assertEqual([item["sha256"] for item in page["items"]],
                         [sha(b"v3"), sha(b"v2"), sha(b"v1")])
        self.assertEqual([item["size"] for item in page["items"]], [2, 2, 2])
        for item in page["items"]:
            self.assertIsInstance(item["created_at"], str)

    def test_get_head_list_only_see_current(self):
        self.enable()
        self.store.put("k", b"v1")
        self.store.put("k", b"v2")
        data, entry = self.store.get("k")
        self.assertEqual(data, b"v2")
        self.assertEqual(entry["sha256"], sha(b"v2"))
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v2"))
        page = self.store.list_objects()
        self.assertEqual([item["sha256"] for item in page["items"]],
                         [sha(b"v2")])

    def test_delete_only_removes_current_pointer(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.delete("k")
        with self.assertRaises(ObjectStoreError):
            self.store.get("k")
        self.assertEqual(self.store.list_objects()["items"], [])
        # history stays readable, nothing is current anymore
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[1], vids[0]])
        self.assertEqual([item["current"] for item in page["items"]],
                         [False, False])
        self.assertEqual(self.store.get_version("k", vids[0])[0], b"v1")
        # a new write starts a fresh current version on top of the history
        entry = self.store.put("k", b"v3")
        self.assertEqual(self.store.get("k")[0], b"v3")
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [entry["version_id"], vids[1], vids[0]])

    def test_same_content_still_creates_distinct_versions(self):
        self.enable()
        first = self.store.put("k", b"same")["version_id"]
        second = self.store.put("k", b"same")["version_id"]
        self.assertNotEqual(first, second)
        self.assertEqual(len(self.store.list_versions("k")["items"]), 2)

    def test_precondition_still_applies(self):
        self.enable()
        self.store.put("k", b"v1")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"v2", expected_sha256=None)
        self.assertEqual(str(ctx.exception), "precondition failed")
        self.assertEqual(len(self.store.list_versions("k")["items"]), 1)

    def test_writes_before_enabling_have_no_versions(self):
        self.store.put("k", b"old")
        self.enable()
        self.assertEqual(self.store.list_versions("k")["items"], [])
        entry = self.store.put("k", b"new")
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [entry["version_id"]])
        # deleting the only version makes the key disappear entirely
        self.store.delete_version("k", entry["version_id"])
        with self.assertRaises(ObjectStoreError):
            self.store.get("k")

    def test_write_while_disabled_unsets_current_but_keeps_history(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.set_versioning(False)
        self.store.put("k", b"off-mode")
        self.store.set_versioning(True)
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[1], vids[0]])
        self.assertEqual([item["current"] for item in page["items"]],
                         [False, False])
        self.assertEqual(self.store.get("k")[0], b"off-mode")

    def test_upload_completion_creates_version(self):
        self.enable()
        payload = b"uploaded-bytes"
        session = self.store.begin_upload("k", len(payload), sha(payload))
        self.store.append_upload(session, 0, payload)
        entry = self.store.complete_upload(session)
        vid = entry["version_id"]
        self.assertEqual(self.store.list_versions("k")["items"][0]["version_id"],
                         vid)
        # repeating completion returns the first result, no extra version
        again = self.store.complete_upload(session)
        self.assertEqual(again["version_id"], vid)
        self.assertEqual(len(self.store.list_versions("k")["items"]), 1)

    def test_upload_version_id_survives_reopen(self):
        self.enable()
        payload = b"uploaded-bytes"
        session = self.store.begin_upload("k", len(payload), sha(payload))
        self.store.append_upload(session, 0, payload)
        entry = self.store.complete_upload(session)
        fresh = ContentAddressedStore(self.root)
        again = fresh.complete_upload(session)
        self.assertEqual(again["version_id"], entry["version_id"])

    def test_versions_survive_reopen(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        fresh = ContentAddressedStore(self.root)
        page = fresh.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[1], vids[0]])
        self.assertEqual(fresh.get_version("k", vids[0])[0], b"v1")

    def test_tenants_are_isolated(self):
        self.enable()
        self.enable("acme")
        self.store.put("k", b"default-v1")
        self.store.put("k", b"acme-v1", tenant="acme")
        self.store.put("k", b"acme-v2", tenant="acme")
        self.assertEqual(len(self.store.list_versions("k")["items"]), 1)
        self.assertEqual(len(self.store.list_versions("k", tenant="acme")["items"]), 2)
        self.assertEqual(self.store.get("k")[0], b"default-v1")
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"acme-v2")


class TestVersionReads(VersioningTestCase):
    def test_get_version_roundtrip(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        data, meta = self.store.get_version("k", vids[0])
        self.assertEqual(data, b"v1")
        self.assertEqual(meta["version_id"], vids[0])
        self.assertEqual(meta["sha256"], sha(b"v1"))
        self.assertEqual(meta["size"], 2)
        self.assertIs(meta["current"], False)
        _, meta = self.store.get_version("k", vids[1])
        self.assertIs(meta["current"], True)

    def test_get_version_verifies_blob(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"v1"])
        with open(os.path.join(self.store.blobs_dir, sha(b"v1")), "wb") as handle:
            handle.write(b"tampered")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", vid)
        self.assertEqual(str(ctx.exception), "corrupted blob")

    def test_unknown_key_and_version_are_not_found(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"v1"])
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("missing", vid)
        self.assertTrue(str(ctx.exception).startswith("not found"))
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", "0" * 32)
        self.assertTrue(str(ctx.exception).startswith("not found"))

    def test_invalid_version_id(self):
        self.enable()
        self.store.put("k", b"v1")
        for bad in ("not-a-version", "", "0" * 31, "0" * 33, "g" * 32, None, 7):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.get_version("k", bad)
            self.assertEqual(str(ctx.exception), "invalid version")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.delete_version("k", bad)
            self.assertEqual(str(ctx.exception), "invalid version")

    def test_versioning_disabled_errors(self):
        self.store.put("k", b"v1")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("k")
        self.assertEqual(str(ctx.exception), "versioning disabled")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", "0" * 32)
        self.assertEqual(str(ctx.exception), "versioning disabled")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.delete_version("k", "0" * 32)
        self.assertEqual(str(ctx.exception), "versioning disabled")

    def test_list_versions_pagination(self):
        self.enable()
        vids = self.put_versions("k", [b"v%d" % i for i in range(5)])
        expected = list(reversed(vids))
        seen = []
        after = None
        while True:
            page = self.store.list_versions("k", after=after, limit=2)
            seen.extend(item["version_id"] for item in page["items"])
            if page["next_after"] is None:
                break
            after = page["next_after"]
        self.assertEqual(seen, expected)

    def test_list_versions_unknown_key_is_empty_page(self):
        self.enable()
        self.assertEqual(self.store.list_versions("missing"),
                         {"items": [], "next_after": None})

    def test_list_versions_invalid_arguments(self):
        self.enable()
        self.store.put("k", b"v1")
        with self.assertRaises(ObjectStoreError):
            self.store.list_versions("k", limit=0)
        with self.assertRaises(ObjectStoreError):
            self.store.list_versions("k", after=7)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("k", after="bogus")
        self.assertTrue(str(ctx.exception).startswith("invalid after"))
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_versions("k", after="0" * 32)
        self.assertTrue(str(ctx.exception).startswith("invalid after"))


class TestVersionDeletes(VersioningTestCase):
    def test_delete_history_keeps_current(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2", b"v3"])
        self.store.delete_version("k", vids[0])
        self.assertEqual(self.store.get("k")[0], b"v3")
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[2], vids[1]])
        self.assertEqual([item["current"] for item in page["items"]],
                         [True, False])

    def test_delete_current_promotes_newest_remaining(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2", b"v3"])
        self.store.delete_version("k", vids[2])
        self.assertEqual(self.store.get("k")[0], b"v2")
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v2"))
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[1], vids[0]])
        self.assertEqual([item["current"] for item in page["items"]],
                         [True, False])

    def test_delete_last_version_removes_key(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"v1"])
        self.store.delete_version("k", vid)
        with self.assertRaises(ObjectStoreError):
            self.store.get("k")
        self.assertEqual(self.store.list_versions("k")["items"], [])
        # the key is fully gone from the index after a reopen
        fresh = ContentAddressedStore(self.root)
        self.assertEqual(fresh.list_objects()["items"], [])

    def test_delete_version_unknown(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"v1"])
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.delete_version("missing", vid)
        self.assertTrue(str(ctx.exception).startswith("not found"))
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.delete_version("k", "0" * 32)
        self.assertTrue(str(ctx.exception).startswith("not found"))

    def test_delete_version_survives_reopen(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.delete_version("k", vids[1])
        fresh = ContentAddressedStore(self.root)
        self.assertEqual(fresh.get("k")[0], b"v1")
        self.assertEqual([item["version_id"]
                          for item in fresh.list_versions("k")["items"]],
                         [vids[0]])


class TestRetention(VersioningTestCase):
    def test_set_and_get_policy(self):
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": None, "max_age_seconds": None})
        self.store.set_retention(max_versions=3)
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": 3, "max_age_seconds": None})
        self.store.set_retention(max_age_seconds=60)
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": None, "max_age_seconds": 60})
        self.store.set_retention(max_versions=3, max_age_seconds=60)
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": 3, "max_age_seconds": 60})

    def test_policy_is_per_tenant_and_persistent(self):
        self.store.set_retention(max_versions=2, tenant="acme")
        fresh = ContentAddressedStore(self.root)
        self.assertEqual(fresh.get_retention(tenant="acme"),
                         {"max_versions": 2, "max_age_seconds": None})
        self.assertEqual(fresh.get_retention(),
                         {"max_versions": None, "max_age_seconds": None})

    def test_invalid_policies(self):
        for kwargs in ({}, {"max_versions": None, "max_age_seconds": None},
                       {"max_versions": -1}, {"max_age_seconds": -1},
                       {"max_versions": 1.5}, {"max_age_seconds": "60"},
                       {"max_versions": True}, {"max_age_seconds": False}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_retention(**kwargs)
            self.assertEqual(str(ctx.exception), "invalid retention")
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": None, "max_age_seconds": None})

    def test_zero_limits_are_valid(self):
        self.store.set_retention(max_versions=0, max_age_seconds=0)
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": 0, "max_age_seconds": 0})

    def test_purge_requires_policy(self):
        self.enable()
        self.store.put("k", b"v1")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.purge_retention()
        self.assertEqual(str(ctx.exception), "retention not configured")

    def test_purge_invalid_dry_run(self):
        self.store.set_retention(max_versions=1)
        for bad in ("yes", 1, 0, None):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.purge_retention(dry_run=bad)
            self.assertEqual(str(ctx.exception), "invalid dry_run")

    def test_purge_max_versions_keeps_newest_and_current(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2", b"v3", b"v4"])
        self.store.set_retention(max_versions=2)
        preview = self.store.purge_retention()
        self.assertIs(preview["dry_run"], True)
        self.assertEqual([item["version_id"] for item in preview["versions"]],
                         sorted([vids[0], vids[1]]))
        self.assertEqual(preview["bytes"], 4)
        # dry run changed nothing
        self.assertEqual(len(self.store.list_versions("k")["items"]), 4)
        result = self.store.purge_retention(dry_run=False)
        self.assertEqual(result, {"versions": preview["versions"], "bytes": 4,
                                  "dry_run": False})
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[3], vids[2]])
        self.assertEqual(self.store.get("k")[0], b"v4")
        # purged versions are gone
        with self.assertRaises(ObjectStoreError):
            self.store.get_version("k", vids[0])

    def test_purge_max_age(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.set_retention(max_age_seconds=3600)
        # age the older version by rewriting its timestamp
        info = self.store._versions["default"]["k"]
        info["versions"][0]["created_at"] = "2000-01-01T00:00:00+00:00"
        self.store._save_index()
        result = self.store.purge_retention(dry_run=False)
        self.assertEqual([item["version_id"] for item in result["versions"]],
                         [vids[0]])
        self.assertEqual(result["bytes"], 2)
        self.assertEqual(self.store.get("k")[0], b"v2")

    def test_purge_never_deletes_current(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.set_retention(max_versions=0, max_age_seconds=0)
        info = self.store._versions["default"]["k"]
        for record in info["versions"]:
            record["created_at"] = "2000-01-01T00:00:00+00:00"
        self.store._save_index()
        result = self.store.purge_retention(dry_run=False)
        self.assertEqual([item["version_id"] for item in result["versions"]],
                         [vids[0]])
        page = self.store.list_versions("k")
        self.assertEqual([item["version_id"] for item in page["items"]],
                         [vids[1]])
        self.assertIs(page["items"][0]["current"], True)
        self.assertEqual(self.store.get("k")[0], b"v2")

    def test_purge_spans_keys_and_reports_sorted(self):
        self.enable()
        vids_a = self.put_versions("a", [b"a1", b"a2"])
        vids_b = self.put_versions("b", [b"b1", b"b2"])
        self.store.set_retention(max_versions=1)
        result = self.store.purge_retention(dry_run=False)
        expected = sorted([vids_a[0], vids_b[0]])
        self.assertEqual([item["version_id"] for item in result["versions"]],
                         expected)
        by_id = {item["version_id"]: item["key"] for item in result["versions"]}
        self.assertEqual(by_id, {vids_a[0]: "a", vids_b[0]: "b"})
        self.assertEqual(result["bytes"], 4)

    def test_purge_result_is_persistent(self):
        self.enable()
        self.put_versions("k", [b"v1", b"v2"])
        self.store.set_retention(max_versions=1)
        self.store.purge_retention(dry_run=False)
        fresh = ContentAddressedStore(self.root)
        self.assertEqual(len(fresh.list_versions("k")["items"]), 1)
        self.assertEqual(fresh.purge_retention(dry_run=False),
                         {"versions": [], "bytes": 0, "dry_run": False})

    def test_purge_works_with_versioning_disabled(self):
        self.enable()
        self.put_versions("k", [b"v1", b"v2"])
        self.store.set_versioning(False)
        self.store.set_retention(max_versions=1)
        result = self.store.purge_retention(dry_run=False)
        self.assertEqual(len(result["versions"]), 1)


class TestVersioningAndGC(VersioningTestCase):
    def test_gc_keeps_version_blobs(self):
        self.enable()
        self.put_versions("k", [b"v1", b"v2"])
        self.store.delete("k")
        # the key is gone but both version blobs are still referenced
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [])

    def test_gc_collects_after_version_delete(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self.store.delete("k")
        self.store.delete_version("k", vids[0])
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [sha(b"v1")])
        self.store.delete_version("k", vids[1])
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [sha(b"v2")])

    def test_gc_collects_after_purge(self):
        self.enable()
        self.put_versions("k", [b"v1", b"v2"])
        self.store.set_retention(max_versions=1)
        self.store.purge_retention(dry_run=False)
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [sha(b"v1")])

    def test_gc_keeps_shared_blob_while_version_references_it(self):
        self.enable()
        (vid,) = self.put_versions("k", [b"shared"])
        self.store.set_versioning(False)
        self.store.put("other", b"shared")
        self.store.delete("other")
        # the version record keeps the blob alive
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [])
        self.store.set_versioning(True)
        self.store.delete_version("k", vid)
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"],
                         [sha(b"shared")])


class TestVersioningIOErrors(VersioningTestCase):
    def _fail_saves(self):
        def boom():
            raise OSError("disk full")
        self.store._save_index = boom

    def test_purge_io_error_rolls_back(self):
        self.enable()
        self.put_versions("k", [b"v1", b"v2"])
        self.store.set_retention(max_versions=1)
        self._fail_saves()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.purge_retention(dry_run=False)
        self.assertEqual(str(ctx.exception), "version io error")
        self.assertEqual(len(self.store.list_versions("k")["items"]), 2)

    def test_delete_version_io_error_rolls_back(self):
        self.enable()
        vids = self.put_versions("k", [b"v1", b"v2"])
        self._fail_saves()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.delete_version("k", vids[1])
        self.assertEqual(str(ctx.exception), "version io error")
        self.assertEqual(len(self.store.list_versions("k")["items"]), 2)
        self.assertEqual(self.store.get("k")[0], b"v2")

    def test_toggle_and_retention_io_error_roll_back(self):
        self.enable()
        self.store.set_retention(max_versions=1)
        self._fail_saves()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.set_versioning(False)
        self.assertEqual(str(ctx.exception), "version io error")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.set_retention(max_versions=5)
        self.assertEqual(str(ctx.exception), "version io error")
        self.assertIs(self.store.get_versioning(), True)
        self.assertEqual(self.store.get_retention(),
                         {"max_versions": 1, "max_age_seconds": None})


class TestVersioningIndexCompat(VersioningTestCase):
    def test_old_index_without_versioning_fields_loads(self):
        self.store.put("k", b"old")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertNotIn("versions", document)
        self.assertNotIn("versioning", document)
        self.assertNotIn("retention", document)
        fresh = ContentAddressedStore(self.root)
        self.assertEqual(fresh.get("k")[0], b"old")
        self.assertIs(fresh.get_versioning(), False)
        # no fabricated history for pre-existing objects
        fresh.set_versioning(True)
        self.assertEqual(fresh.list_versions("k")["items"], [])

    def test_index_roundtrip_is_byte_stable(self):
        self.enable()
        self.store.set_retention(max_versions=2)
        self.put_versions("k", [b"v1", b"v2"])
        with open(self.store.index_path, "rb") as handle:
            before = handle.read()
        fresh = ContentAddressedStore(self.root)
        fresh._save_index()
        with open(self.store.index_path, "rb") as handle:
            self.assertEqual(handle.read(), before)

    def test_corrupted_version_records_are_reported(self):
        self.enable()
        self.store.put("k", b"v1")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        document["versions"]["default"]["k"]["versions"][0]["sha256"] = "nope"
        with open(self.store.index_path, "wb") as handle:
            handle.write(json.dumps(document).encode("utf-8"))
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertTrue(str(ctx.exception).startswith("corrupted index"))


if __name__ == "__main__":
    unittest.main()

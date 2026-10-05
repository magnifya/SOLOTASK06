"""Tests for per-tenant quota policies and usage accounting."""

import hashlib
import os
import shutil
import tempfile
import unittest

from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


EMPTY_USAGE = {"used_bytes": 0, "used_objects": 0,
               "reserved_bytes": 0, "reserved_sessions": 0}


class QuotaTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-quota-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def enable_versioning(self, tenant="acme"):
        self.store.set_versioning(True, tenant=tenant)


class TestQuotaPolicy(QuotaTestCase):
    def test_default_is_unset(self):
        self.assertIsNone(self.store.get_quota())
        self.assertIsNone(self.store.get_quota(tenant="acme"))

    def test_set_and_get_roundtrip(self):
        self.store.set_quota(max_bytes=100, max_objects=3, tenant="acme")
        self.assertEqual(self.store.get_quota(tenant="acme"),
                         {"max_bytes": 100, "max_objects": 3})
        self.assertIsNone(self.store.get_quota())

    def test_unset_limit_reads_as_none(self):
        self.store.set_quota(max_bytes=100, tenant="acme")
        self.assertEqual(self.store.get_quota(tenant="acme"),
                         {"max_bytes": 100, "max_objects": None})
        self.store.set_quota(max_objects=2, tenant="acme")
        self.assertEqual(self.store.get_quota(tenant="acme"),
                         {"max_bytes": None, "max_objects": 2})

    def test_set_replaces_policy(self):
        self.store.set_quota(max_bytes=100, max_objects=3, tenant="acme")
        self.store.set_quota(max_bytes=50, tenant="acme")
        self.assertEqual(self.store.get_quota(tenant="acme"),
                         {"max_bytes": 50, "max_objects": None})

    def test_invalid_quota(self):
        for kwargs in ({}, {"max_bytes": None, "max_objects": None},
                       {"max_bytes": True}, {"max_objects": False},
                       {"max_bytes": -1}, {"max_objects": -2},
                       {"max_bytes": 1.5}, {"max_objects": "3"},
                       {"max_bytes": []}, {"max_objects": {"n": 1}}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_quota(tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "invalid quota", kwargs)
        self.assertIsNone(self.store.get_quota(tenant="acme"))

    def test_invalid_tenant(self):
        for call in (lambda: self.store.set_quota(max_bytes=1, tenant="BAD!"),
                     lambda: self.store.get_quota(tenant="BAD!"),
                     lambda: self.store.clear_quota(tenant="BAD!"),
                     lambda: self.store.get_usage(tenant="BAD!")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen(self):
        self.store.set_quota(max_bytes=100, max_objects=3, tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_quota(tenant="acme"),
                         {"max_bytes": 100, "max_objects": 3})

    def test_clear_restores_unlimited(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("b", b"2", tenant="acme")
        self.store.clear_quota(tenant="acme")
        self.assertIsNone(self.store.get_quota(tenant="acme"))
        self.store.put("b", b"2", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme")["used_objects"], 2)

    def test_clear_without_policy_is_noop(self):
        self.assertIsNone(self.store.clear_quota(tenant="acme"))
        self.assertIsNone(self.store.get_quota(tenant="acme"))

    def test_quota_io_error_keeps_previous_policy(self):
        self.store.set_quota(max_bytes=100, tenant="acme")
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_quota(max_bytes=50, tenant="acme")
            self.assertEqual(str(ctx.exception), "quota io error")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.clear_quota(tenant="acme")
            self.assertEqual(str(ctx.exception), "quota io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_quota(tenant="acme"),
                         {"max_bytes": 100, "max_objects": None})

    def test_corrupted_quota_in_index(self):
        import json
        for bad in ({"quota": []},
                    {"quota": {"acme": []}},
                    {"quota": {"acme": {}}},
                    {"quota": {"acme": {"max_bytes": -1}}},
                    {"quota": {"acme": {"max_objects": True}}},
                    {"quota": {"BAD TENANT": {"max_bytes": 1}}}):
            document = {"version": 1, "objects": {}}
            document.update(bad)
            with open(self.store.index_path, "w") as handle:
                handle.write(json.dumps(document))
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"), bad)


class TestUsageAccounting(QuotaTestCase):
    def test_empty_usage(self):
        self.assertEqual(self.store.get_usage(), EMPTY_USAGE)
        self.assertEqual(self.store.get_usage(tenant="acme"), EMPTY_USAGE)

    def test_shared_digest_counts_per_key(self):
        self.store.put("a", b"hello", tenant="acme")
        self.store.put("b", b"hello", tenant="acme")
        self.assertEqual(len(self.store.blob_digests()), 1)
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=10, used_objects=2))

    def test_tenants_counted_separately(self):
        self.store.put("k", b"hello", tenant="acme")
        self.store.put("k", b"hello")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=5, used_objects=1))
        self.assertEqual(self.store.get_usage(),
                         dict(EMPTY_USAGE, used_bytes=5, used_objects=1))
        self.assertEqual(self.store.get_usage(tenant="other"), EMPTY_USAGE)

    def test_overwrite_replaces_usage(self):
        self.store.put("k", b"12345", tenant="acme")
        self.store.put("k", b"12345678", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=8, used_objects=1))

    def test_versions_counted_without_double_counting_current(self):
        self.enable_versioning()
        self.store.put("k", b"123", tenant="acme")
        self.store.put("k", b"12345", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=8, used_objects=2))

    def test_diverging_current_pointer_counts_separately(self):
        self.enable_versioning()
        self.store.put("k", b"123", tenant="acme")
        self.store.set_versioning(False, tenant="acme")
        self.store.put("k", b"12345", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=8, used_objects=2))

    def test_delete_releases(self):
        self.store.put("k", b"12345", tenant="acme")
        self.store.delete("k", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"), EMPTY_USAGE)

    def test_versioned_delete_releases_one_version(self):
        self.enable_versioning()
        self.store.put("k", b"123", tenant="acme")
        self.store.put("k", b"12345", tenant="acme")
        self.store.delete("k", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=3, used_objects=1))
        self.store.delete("k", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"), EMPTY_USAGE)

    def test_delete_version_releases(self):
        self.enable_versioning()
        self.store.put("k", b"123", tenant="acme")
        second = self.store.put("k", b"12345", tenant="acme")
        self.store.delete_version("k", second["version_id"], tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=3, used_objects=1))

    def test_purge_releases(self):
        self.enable_versioning()
        self.store.put("k", b"123", tenant="acme")
        self.store.put("k", b"12345", tenant="acme")
        self.store.set_retention(max_versions=1, tenant="acme")
        self.store.purge_retention(dry_run=False, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=5, used_objects=1))

    def test_active_upload_reserves_declared_size(self):
        session = self.store.begin_upload("big", 100, sha(b"x" * 100),
                                          tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, reserved_bytes=100,
                              reserved_sessions=1))
        self.store.append_upload(session, 0, b"x" * 40, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, reserved_bytes=100,
                              reserved_sessions=1))

    def test_reservation_survives_reopen(self):
        self.store.begin_upload("big", 100, sha(b"x" * 100), tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, reserved_bytes=100,
                              reserved_sessions=1))

    def test_complete_releases_reservation(self):
        session = self.store.begin_upload("big", 100, sha(b"x" * 100),
                                          tenant="acme")
        self.store.append_upload(session, 0, b"x" * 100, tenant="acme")
        self.store.complete_upload(session, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=100, used_objects=1))

    def test_abort_releases_reservation(self):
        session = self.store.begin_upload("big", 100, sha(b"x" * 100),
                                          tenant="acme")
        self.store.abort_upload(session, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"), EMPTY_USAGE)

    def test_gc_does_not_change_usage(self):
        self.store.put("a", b"hello", tenant="acme")
        self.store.put("b", b"hello", tenant="acme")
        self.store.delete("b", tenant="acme")
        before = self.store.get_usage(tenant="acme")
        self.store.collect_garbage(dry_run=False)
        self.assertEqual(self.store.get_usage(tenant="acme"), before)


class TestQuotaEnforcement(QuotaTestCase):
    def test_put_within_limit_ok(self):
        self.store.set_quota(max_bytes=10, max_objects=2, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        self.store.put("b", b"1234", tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme")["used_bytes"], 10)

    def test_put_exceeding_max_bytes(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("b", b"12345", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        with self.assertRaises(ObjectStoreError):
            self.store.head("b", tenant="acme")
        self.assertNotIn(sha(b"12345"), self.store.blob_digests())
        self.assertEqual(self.store.get_usage(tenant="acme")["used_bytes"], 6)

    def test_put_exceeding_max_objects(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("b", b"2", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        self.store.put("a", b"1-new", tenant="acme")
        self.assertEqual(self.store.get("a", tenant="acme")[0], b"1-new")

    def test_overwrite_charges_only_the_delta(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("k", b"12345678", tenant="acme")
        self.store.put("k", b"1234567890", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"12345678901", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"1234567890")

    def test_zero_limits(self):
        self.store.set_quota(max_bytes=0, tenant="acme")
        self.store.put("empty", b"", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("byte", b"x", tenant="acme")
        self.store.set_quota(max_objects=0, tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("other", b"", tenant="acme")

    def test_versioned_put_counts_each_version(self):
        self.enable_versioning()
        self.store.set_quota(max_objects=2, tenant="acme")
        self.store.put("k", b"1", tenant="acme")
        self.store.put("k", b"2", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"3", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"2")

    def test_delete_frees_room(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.delete("a", tenant="acme")
        self.store.put("b", b"2", tenant="acme")

    def test_precondition_failure_takes_precedence(self):
        self.store.put("k", b"v1", tenant="acme")
        self.store.set_quota(max_bytes=0, tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"v2", expected_sha256="0" * 64, tenant="acme")
        self.assertEqual(str(ctx.exception), "precondition failed")

    def test_quota_is_per_tenant(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with self.assertRaises(ObjectStoreError):
            self.store.put("b", b"2", tenant="acme")

    def test_quota_enforced_after_reopen(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        with self.assertRaises(ObjectStoreError) as ctx:
            reopened.put("b", b"12345", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")


class TestUploadQuota(QuotaTestCase):
    def test_begin_upload_beyond_max_bytes_creates_no_session(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.begin_upload("big", 5, sha(b"x" * 5), tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=6, used_objects=1))

    def test_reservation_blocks_other_writes(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        session = self.store.begin_upload("big", 4, sha(b"x" * 4),
                                          tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("b", b"1", tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        self.store.append_upload(session, 0, b"x" * 4, tenant="acme")
        self.store.complete_upload(session, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=10, used_objects=2))

    def test_complete_releases_own_reservation_first(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.put("a", b"123456", tenant="acme")
        session = self.store.begin_upload("big", 4, sha(b"x" * 4),
                                          tenant="acme")
        self.store.append_upload(session, 0, b"x" * 4, tenant="acme")
        entry = self.store.complete_upload(session, tenant="acme")
        self.assertEqual(entry["size"], 4)

    def test_complete_checks_object_count(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        session = self.store.begin_upload("b", 2, sha(b"bb"), tenant="acme")
        self.store.append_upload(session, 0, b"bb", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.complete_upload(session, tenant="acme")
        self.assertEqual(str(ctx.exception), "quota exceeded")
        # The session stays active and can still be aborted.
        status = self.store.upload_status(session, tenant="acme")
        self.assertFalse(status["completed"])
        self.store.abort_upload(session, tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, used_bytes=1, used_objects=1))

    def test_failed_complete_publishes_nothing(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        session = self.store.begin_upload("b", 2, sha(b"bb"), tenant="acme")
        self.store.append_upload(session, 0, b"bb", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session, tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.head("b", tenant="acme")
        self.assertNotIn(sha(b"bb"), self.store.blob_digests())

    def test_abort_frees_room_for_new_session(self):
        self.store.set_quota(max_bytes=10, tenant="acme")
        first = self.store.begin_upload("a", 8, sha(b"a" * 8), tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.begin_upload("b", 8, sha(b"b" * 8), tenant="acme")
        self.store.abort_upload(first, tenant="acme")
        self.store.begin_upload("b", 8, sha(b"b" * 8), tenant="acme")

    def test_max_objects_only_does_not_block_begin(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        session = self.store.begin_upload("big", 10 ** 9, sha(b""),
                                          tenant="acme")
        self.assertEqual(self.store.get_usage(tenant="acme"),
                         dict(EMPTY_USAGE, reserved_bytes=10 ** 9,
                              reserved_sessions=1))
        self.store.abort_upload(session, tenant="acme")


if __name__ == "__main__":
    unittest.main()

"""Tests for per-tenant fixed-window rate limiting (Python API)."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.store import (
    ContentAddressedStore,
    ObjectStoreError,
    RateLimitExceeded,
)


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class RateLimitTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-ratelimit-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def assert_rate_limited(self, func, *args, **kwargs):
        with self.assertRaises(ObjectStoreError) as ctx:
            func(*args, **kwargs)
        self.assertEqual(str(ctx.exception), "rate limit exceeded")
        self.assertIsInstance(ctx.exception, RateLimitExceeded)
        self.assertGreaterEqual(ctx.exception.retry_after, 1)


class TestPolicyValidation(RateLimitTestCase):
    def test_defaults_to_none(self):
        self.assertIsNone(self.store.get_rate_limit())
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))

    def test_set_get_roundtrip(self):
        self.store.set_rate_limit(60, max_requests=10, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": None})
        self.store.set_rate_limit(3600, max_bytes=1024, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 3600, "max_requests": None,
                          "max_bytes": 1024})
        self.store.set_rate_limit(1, max_requests=1, max_bytes=2, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 1, "max_requests": 1,
                          "max_bytes": 2})

    def test_invalid_policies(self):
        for kwargs in ({"window_seconds": 0}, {"window_seconds": -1},
                       {"window_seconds": 86401}, {"window_seconds": True},
                       {"window_seconds": "60"}, {"window_seconds": 1.5},
                       {"window_seconds": None},
                       {"window_seconds": 60},
                       {"window_seconds": 60, "max_requests": None,
                        "max_bytes": None},
                       {"window_seconds": 60, "max_requests": -1},
                       {"window_seconds": 60, "max_bytes": -1},
                       {"window_seconds": 60, "max_requests": True},
                       {"window_seconds": 60, "max_bytes": False},
                       {"window_seconds": 60, "max_requests": "5"},
                       {"window_seconds": 60, "max_bytes": 2.5}):
            window = kwargs.pop("window_seconds")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_rate_limit(window, tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "invalid rate limit", kwargs)
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))

    def test_zero_limits_are_valid(self):
        self.store.set_rate_limit(60, max_requests=0, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 0,
                          "max_bytes": None})

    def test_clear_is_idempotent(self):
        self.assertIsNone(self.store.clear_rate_limit(tenant="acme"))
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        self.assertIsNone(self.store.clear_rate_limit(tenant="acme"))
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))
        self.assertIsNone(self.store.get_rate_limit_usage(tenant="acme"))

    def test_invalid_tenant(self):
        calls = (
            lambda: self.store.set_rate_limit(60, max_requests=1,
                                              tenant="Bad Tenant"),
            lambda: self.store.get_rate_limit(tenant="Bad Tenant"),
            lambda: self.store.clear_rate_limit(tenant="Bad Tenant"),
            lambda: self.store.get_rate_limit_usage(tenant="Bad Tenant"),
        )
        for call in calls:
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen_counters_reset(self):
        self.store.set_rate_limit(86400, max_requests=5, tenant="acme")
        self.store.put("k", b"data", tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_rate_limit(tenant="acme"),
                         {"window_seconds": 86400, "max_requests": 5,
                          "max_bytes": None})
        usage = reopened.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 0)
        self.assertEqual(usage["bytes"], 0)

    def test_policy_in_index_document(self):
        self.store.set_rate_limit(60, max_requests=3, tenant="acme")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(document["rate_limit"],
                         {"acme": {"window_seconds": 60, "max_requests": 3}})

    def test_io_error_on_save(self):
        if os.path.exists(self.store.index_path):
            os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_rate_limit(60, max_requests=1, tenant="acme")
            self.assertEqual(str(ctx.exception), "rate limit io error")
            self.assertIsNone(self.store.get_rate_limit(tenant="acme"))
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.clear_rate_limit(tenant="acme")
            self.assertEqual(str(ctx.exception), "rate limit io error")
        finally:
            os.rmdir(self.store.index_path)

    def test_io_error_keeps_previous_policy(self):
        self.store.set_rate_limit(60, max_requests=7, tenant="acme")
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError):
                self.store.set_rate_limit(30, max_requests=1, tenant="acme")
            with self.assertRaises(ObjectStoreError):
                self.store.clear_rate_limit(tenant="acme")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 7,
                          "max_bytes": None})

    def test_corrupted_policy_in_index(self):
        self.store.put("k", b"data")
        with open(self.store.index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        for bad in ({"acme": {"window_seconds": 0, "max_requests": 1}},
                    {"acme": {"window_seconds": 60}},
                    {"acme": {"window_seconds": 60, "max_requests": -1}},
                    {"acme": {"window_seconds": 60, "max_bytes": True}},
                    {"acme": "nope"}):
            document["rate_limit"] = bad
            with open(self.store.index_path, "wb") as handle:
                handle.write(json.dumps(document).encode("utf-8"))
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"),
                            bad)


class TestEnforcement(RateLimitTestCase):
    def test_max_requests_counts_operations(self):
        self.store.set_rate_limit(86400, max_requests=2, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"2", tenant="acme")
        self.assert_rate_limited(self.store.put, "c", b"3", tenant="acme")
        self.assert_rate_limited(self.store.get, "a", tenant="acme")
        self.assert_rate_limited(self.store.list_objects, tenant="acme")
        # Rejected requests stored nothing and counted nothing.
        with self.assertRaises(ObjectStoreError):
            self.store.get("c", tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 2)

    def test_other_tenant_unaffected(self):
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.assert_rate_limited(self.store.put, "b", b"2", tenant="acme")
        self.store.put("a", b"1")
        self.store.put("b", b"2", tenant="other")
        self.assertIsNone(self.store.get_rate_limit_usage(tenant="other"))

    def test_max_bytes_counts_put_bodies(self):
        self.store.set_rate_limit(86400, max_bytes=5, tenant="acme")
        self.store.put("a", b"123", tenant="acme")
        self.assert_rate_limited(self.store.put, "b", b"123", tenant="acme")
        # Reads carry no body bytes and stay allowed.
        data, _ = self.store.get("a", tenant="acme")
        self.assertEqual(data, b"123")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["bytes"], 3)
        self.assertEqual(usage["requests"], 2)

    def test_max_bytes_counts_upload_chunks(self):
        self.store.set_rate_limit(86400, max_bytes=4, tenant="acme")
        session = self.store.begin_upload("big", 6, sha(b"abcdef"),
                                          tenant="acme")
        self.store.append_upload(session, 0, b"ab", tenant="acme")
        self.store.append_upload(session, 2, b"cd", tenant="acme")
        self.assert_rate_limited(self.store.append_upload, session, 4, b"ef",
                                 tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["bytes"], 4)
        # begin/status did not consume byte budget.
        self.assertEqual(usage["requests"], 3)

    def test_management_operations_not_counted(self):
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.store.get_rate_limit(tenant="acme")
        self.store.get_rate_limit_usage(tenant="acme")
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 0)
        self.store.put("a", b"1", tenant="acme")
        self.assert_rate_limited(self.store.put, "b", b"2", tenant="acme")

    def test_validation_failures_not_counted(self):
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        for call in (lambda: self.store.put("bad//key", b"1", tenant="acme"),
                     lambda: self.store.put("k", b"1", tenant="acme",
                                            expected_sha256="nope"),
                     lambda: self.store.put("k", "not-bytes", tenant="acme")):
            with self.assertRaises(ObjectStoreError):
                call()
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 0)

    def test_failed_precondition_not_counted(self):
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.store.put("k", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"2", tenant="acme", expected_sha256=None)
        self.assertEqual(str(ctx.exception), "precondition failed")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)

    def test_signed_read_without_signature_not_counted(self):
        self.store.set_access_policy("signed", secret="0123456789abcdef",
                                     tenant="acme")
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.store.put("k", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("k", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)

    def test_window_switch_resets_counters(self):
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.assert_rate_limited(self.store.put, "b", b"2", tenant="acme")
        # Simulate the window having moved on: the stored window start no
        # longer matches the current fixed window, so counters reset.
        self.store._rate_windows["acme"]["window_start"] -= 60
        self.store.put("b", b"2", tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)

    def test_usage_window_bounds(self):
        self.store.set_rate_limit(60, max_requests=10, tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["reset_at"] - usage["window_start"], 60)
        self.assertEqual(usage["window_start"] % 60, 0)
        self.assertEqual(usage["requests"], 0)
        self.assertEqual(usage["bytes"], 0)

    def test_retry_after_within_window(self):
        self.store.set_rate_limit(60, max_requests=0, tenant="acme")
        with self.assertRaises(RateLimitExceeded) as ctx:
            self.store.put("a", b"1", tenant="acme")
        self.assertGreaterEqual(ctx.exception.retry_after, 1)
        self.assertLessEqual(ctx.exception.retry_after, 60)

    def test_concurrent_requests_never_exceed(self):
        self.store.set_rate_limit(86400, max_requests=5, tenant="acme")
        outcomes = []
        lock = threading.Lock()

        def worker(index):
            try:
                self.store.put("k%d" % index, b"x", tenant="acme")
                outcome = "ok"
            except RateLimitExceeded:
                outcome = "limited"
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("ok"), 5)
        self.assertEqual(outcomes.count("limited"), 15)
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 5)

    def test_blob_counts_against_default_tenant(self):
        self.store.set_rate_limit(86400, max_requests=1)
        digest = sha(b"blob-bytes")
        self.store.put("k", b"blob-bytes")
        self.assert_rate_limited(self.store.blob, digest)

    def test_clear_restores_unlimited(self):
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.assert_rate_limited(self.store.put, "b", b"2", tenant="acme")
        self.store.clear_rate_limit(tenant="acme")
        self.store.put("b", b"2", tenant="acme")
        self.assertIsNone(self.store.get_rate_limit_usage(tenant="acme"))


if __name__ == "__main__":
    unittest.main()

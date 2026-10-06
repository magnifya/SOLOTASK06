"""Tests for per-tenant rate limit policies and request accounting."""

import hashlib
import os
import shutil
import tempfile
import threading
import time
import unittest

from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class RateLimitTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-ratelimit-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)


class TestRateLimitPolicy(RateLimitTestCase):
    def test_default_is_unset(self):
        self.assertIsNone(self.store.get_rate_limit())
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))

    def test_set_and_get_roundtrip(self):
        self.store.set_rate_limit(60, max_requests=10, max_bytes=1000,
                                  tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 1000})
        self.assertIsNone(self.store.get_rate_limit())

    def test_unset_limit_reads_as_none(self):
        self.store.set_rate_limit(60, max_requests=5, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 5,
                          "max_bytes": None})
        self.store.set_rate_limit(60, max_bytes=50, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": None,
                          "max_bytes": 50})

    def test_set_replaces_policy(self):
        self.store.set_rate_limit(60, max_requests=10, max_bytes=100,
                                  tenant="acme")
        self.store.set_rate_limit(30, max_requests=5, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 30, "max_requests": 5,
                          "max_bytes": None})

    def test_window_bounds(self):
        self.store.set_rate_limit(1, max_requests=1, tenant="acme")
        self.store.set_rate_limit(86400, max_requests=1, tenant="acme")
        self.assertEqual(self.store.get_rate_limit(tenant="acme")
                         ["window_seconds"], 86400)

    def test_invalid_rate_limit(self):
        for window in (None, 0, -1, 86401, True, False, "60", 1.5, [60]):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_rate_limit(window, max_requests=1,
                                          tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid rate limit", window)
        for kwargs in ({}, {"max_requests": None, "max_bytes": None},
                       {"max_requests": True}, {"max_bytes": False},
                       {"max_requests": -1}, {"max_bytes": -2},
                       {"max_requests": 1.5}, {"max_bytes": "3"},
                       {"max_requests": []}, {"max_bytes": {"n": 1}}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_rate_limit(60, tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "invalid rate limit", kwargs)
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))

    def test_invalid_tenant(self):
        for call in (lambda: self.store.set_rate_limit(60, max_requests=1,
                                                       tenant="BAD!"),
                     lambda: self.store.get_rate_limit(tenant="BAD!"),
                     lambda: self.store.clear_rate_limit(tenant="BAD!"),
                     lambda: self.store.get_rate_limit_usage(tenant="BAD!")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen(self):
        self.store.set_rate_limit(60, max_requests=10, max_bytes=100,
                                  tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 10,
                          "max_bytes": 100})

    def test_counters_restart_on_reopen(self):
        self.store.set_rate_limit(60, max_requests=10, tenant="acme")
        self.store.put("k", b"1234", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        usage = reopened.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 0)
        self.assertEqual(usage["bytes"], 0)

    def test_clear(self):
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        self.assertIsNone(self.store.clear_rate_limit(tenant="acme"))
        self.assertIsNone(self.store.get_rate_limit(tenant="acme"))
        self.assertIsNone(self.store.get_rate_limit_usage(tenant="acme"))
        # The tenant is unlimited again.
        for index in range(3):
            self.store.put("k%d" % index, b"x", tenant="acme")

    def test_clear_missing_is_noop(self):
        self.assertIsNone(self.store.clear_rate_limit(tenant="acme"))
        self.assertIsNone(self.store.clear_rate_limit())

    def test_io_error_keeps_previous_policy(self):
        self.store.set_rate_limit(60, max_requests=5, tenant="acme")
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_rate_limit(30, max_requests=1, tenant="acme")
            self.assertEqual(str(ctx.exception), "rate limit io error")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.clear_rate_limit(tenant="acme")
            self.assertEqual(str(ctx.exception), "rate limit io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_rate_limit(tenant="acme"),
                         {"window_seconds": 60, "max_requests": 5,
                          "max_bytes": None})


class TestRateLimitEnforcement(RateLimitTestCase):
    def test_max_requests(self):
        self.store.set_rate_limit(60, max_requests=2, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"2", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("c", b"3", tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 2)
        # The rejected write changed nothing.
        reopened = ContentAddressedStore(self.root)
        with self.assertRaises(ObjectStoreError) as ctx:
            reopened.get("c", tenant="acme")
        self.assertTrue(str(ctx.exception).startswith("not found"))

    def test_max_bytes(self):
        self.store.set_rate_limit(60, max_bytes=5, tenant="acme")
        self.store.put("a", b"123", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("b", b"123", tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)
        self.assertEqual(usage["bytes"], 3)

    def test_read_and_list_operations_count(self):
        self.store.set_rate_limit(60, max_requests=3, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.store.get("a", tenant="acme")
        self.store.head("a", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_objects(tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")

    def test_policy_operations_count(self):
        self.store.set_rate_limit(60, max_requests=2, tenant="acme")
        self.store.set_quota(max_bytes=10, tenant="acme")
        self.store.get_quota(tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_usage(tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")

    def test_management_operations_not_counted(self):
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        self.store.get_rate_limit(tenant="acme")
        self.store.get_rate_limit_usage(tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("b", b"2", tenant="acme")
        # Management still works with the window exhausted.
        self.store.get_rate_limit(tenant="acme")
        self.store.get_rate_limit_usage(tenant="acme")
        self.store.clear_rate_limit(tenant="acme")
        self.store.put("b", b"2", tenant="acme")

    def test_validation_failures_not_counted(self):
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        for call in (lambda: self.store.put("bad//key", b"1", tenant="acme"),
                     lambda: self.store.put("k", "not-bytes", tenant="acme"),
                     lambda: self.store.put("k", b"1", tenant="BAD!"),
                     lambda: self.store.put("k", b"1", expected_sha256="zz",
                                            tenant="acme"),
                     lambda: self.store.delete("k", expected_sha256=5,
                                               tenant="acme")):
            with self.assertRaises(ObjectStoreError):
                call()
        # The first valid request still fits the window.
        self.store.put("k", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k2", b"2", tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")

    def test_access_denied_not_counted(self):
        self.store.set_access_policy("signed", secret="0123456789abcdef",
                                     tenant="acme")
        self.store.put("k", b"1", tenant="acme")
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("k", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        # The rejected read was not charged: one signed read still fits.
        expires = int(time.time()) + 60
        signature = self.store.sign_request("GET", "k", expires=expires,
                                            tenant="acme")
        data, _ = self.store.get("k", tenant="acme", expires=expires,
                                 signature=signature)
        self.assertEqual(data, b"1")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("k", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "rate limit exceeded")

    def test_upload_chunks_count_bytes(self):
        self.store.set_rate_limit(60, max_bytes=4, tenant="acme")
        session = self.store.begin_upload("big", 6, sha(b"123456"),
                                          tenant="acme")
        self.store.append_upload(session, 0, b"12", tenant="acme")
        self.store.append_upload(session, 2, b"34", tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.append_upload(session, 4, b"56", tenant="acme")
        self.assertEqual(str(ctx.exception), "rate limit exceeded")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["bytes"], 4)

    def test_session_metadata_does_not_count_bytes(self):
        self.store.set_rate_limit(60, max_bytes=2, tenant="acme")
        session = self.store.begin_upload("big", 2, sha(b"ab"), tenant="acme")
        self.store.upload_status(session, tenant="acme")
        self.store.append_upload(session, 0, b"ab", tenant="acme")
        # The chunk filled the byte budget; requests stay unlimited.
        self.store.upload_status(session, tenant="acme")

    def test_tenants_are_independent(self):
        self.store.set_rate_limit(60, max_requests=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("b", b"2", tenant="acme")
        self.store.put("b", b"2", tenant="other")
        self.store.put("c", b"3")

    def test_window_rollover_resets_counters(self):
        self.store.set_rate_limit(1, max_requests=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.put("b", b"2", tenant="acme")
        start = int(time.time())
        while int(time.time()) == start:
            time.sleep(0.01)
        self.store.put("b", b"2", tenant="acme")

    def test_concurrent_requests_never_exceed(self):
        self.store.set_rate_limit(60, max_requests=20, tenant="acme")
        successes = []
        lock = threading.Lock()

        def worker(index):
            try:
                self.store.put("k%d" % index, b"x", tenant="acme")
            except ObjectStoreError as exc:
                self.assertEqual(str(exc), "rate limit exceeded")
                return
            with lock:
                successes.append(index)

        threads = [threading.Thread(target=worker, args=(index,))
                   for index in range(50)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(successes), 20)
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 20)


class TestRateLimitUsage(RateLimitTestCase):
    def test_usage_is_none_without_policy(self):
        self.assertIsNone(self.store.get_rate_limit_usage())
        self.assertIsNone(self.store.get_rate_limit_usage(tenant="acme"))

    def test_usage_fields_and_window(self):
        self.store.set_rate_limit(60, max_requests=10, tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        now = int(time.time())
        self.assertEqual(usage, {"window_start": now - (now % 60),
                                 "reset_at": now - (now % 60) + 60,
                                 "requests": 0, "bytes": 0})
        self.store.put("a", b"1234", tenant="acme")
        usage = self.store.get_rate_limit_usage(tenant="acme")
        self.assertEqual(usage["requests"], 1)
        self.assertEqual(usage["bytes"], 4)
        self.assertEqual(usage["reset_at"] - usage["window_start"], 60)


if __name__ == "__main__":
    unittest.main()

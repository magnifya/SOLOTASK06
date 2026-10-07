"""Unit tests for the persistent per-tenant audit log."""

import hashlib
import json
import os
import shutil
import tempfile
import unittest

from objstore.store import (
    AUDIT_GLOBAL_TENANT,
    ContentAddressedStore,
    ObjectStoreError,
)


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class AuditTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-audit-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)
        self.audit_path = os.path.join(self.root, "audit.log")

    def read_log(self):
        with open(self.audit_path, "rb") as handle:
            return [json.loads(line)
                    for line in handle.read().decode("utf-8").splitlines()]

    def break_log(self):
        """Make the audit path unwritable as a file (a directory)."""
        if os.path.isfile(self.audit_path):
            os.remove(self.audit_path)
        os.mkdir(self.audit_path)


class TestEventRecording(AuditTestCase):
    def test_object_operations_record_events(self):
        entry = self.store.put("k", b"hello")
        self.store.get("k")
        self.store.head("k")
        self.store.delete("k")
        events = self.read_log()
        self.assertEqual([event["operation"] for event in events],
                         ["put", "get", "head", "delete"])
        self.assertEqual([event["seq"] for event in events], [1, 2, 3, 4])
        for event in events:
            self.assertEqual(event["tenant"], "default")
            self.assertEqual(event["key"], "k")
            self.assertEqual(event["result"], "ok")
            self.assertIn("timestamp", event)
            self.assertIn("prev_hash", event)
            self.assertIn("hash", event)
        put = events[0]
        self.assertEqual(put["sha256"], entry["sha256"])
        self.assertEqual(put["size"], 5)

    def test_failure_events_carry_stable_error(self):
        with self.assertRaises(ObjectStoreError):
            self.store.get("missing")
        with self.assertRaises(ObjectStoreError):
            self.store.get("missing")
        events = self.read_log()
        self.assertEqual(len(events), 2)
        for event in events:
            self.assertEqual(event["result"], "error")
            self.assertEqual(event["error"], "not found: missing")
            self.assertEqual(event["operation"], "get")

    def test_upload_session_events(self):
        payload = b"upload-me"
        session_id = self.store.begin_upload("u", len(payload), sha(payload))
        self.store.append_upload(session_id, 0, payload)
        self.store.upload_status(session_id)
        self.store.complete_upload(session_id)
        events = self.read_log()
        self.assertEqual(
            [event["operation"] for event in events],
            ["begin_upload", "append_upload", "upload_status",
             "complete_upload"])
        for event in events:
            self.assertEqual(event["session"], session_id)
        complete = events[-1]
        self.assertEqual(complete["sha256"], sha(payload))
        self.assertEqual(complete["size"], len(payload))

    def test_version_and_policy_events(self):
        self.store.set_versioning(True)
        self.store.put("v", b"one")
        self.store.put("v", b"two")
        versions = self.store.list_versions("v")["items"]
        self.store.head_version("v", versions[0]["version_id"])
        self.store.set_quota(max_bytes=1000)
        self.store.get_quota()
        self.store.set_retention(max_versions=2)
        self.store.set_lifecycle(60)
        self.store.set_access_policy("signed", secret="s" * 16)
        self.store.set_rate_limit(60, max_requests=100)
        operations = [event["operation"] for event in self.read_log()]
        self.assertEqual(operations, [
            "set_versioning", "put", "put", "list_versions", "head_version",
            "set_quota", "get_quota", "set_retention", "set_lifecycle",
            "set_access_policy", "set_rate_limit",
        ])
        self.assertEqual(operations[4], "head_version")
        event = self.read_log()[4]
        self.assertEqual(event["version"], versions[0]["version_id"])

    def test_blob_and_gc_are_global(self):
        self.store.put("k", b"data")
        digest = sha(b"data")
        self.store.blob(digest)
        self.store.collect_garbage()
        events = [event for event in self.read_log()
                  if event["tenant"] == AUDIT_GLOBAL_TENANT]
        self.assertEqual([event["operation"] for event in events],
                         ["blob", "collect_garbage"])
        self.assertEqual(events[0]["sha256"], digest)
        self.assertEqual(events[0]["size"], 4)
        self.assertEqual([event["seq"] for event in events], [1, 2])

    def test_no_content_or_secret_recorded(self):
        secret = "top-secret-value-1234"
        payload = b"highly confidential payload bytes"
        self.store.set_access_policy("signed", secret=secret)
        self.store.put("doc", payload)
        with open(self.audit_path, "rb") as handle:
            raw = handle.read()
        self.assertNotIn(secret.encode("utf-8"), raw)
        self.assertNotIn(payload, raw)

    def test_audit_queries_record_no_events(self):
        self.store.put("k", b"x")
        before = len(self.read_log())
        self.store.list_audit_events()
        self.store.verify_audit_log()
        self.assertEqual(len(self.read_log()), before)


class TestTenantIsolation(AuditTestCase):
    def test_per_tenant_sequences_and_chains(self):
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"2", tenant="globex")
        self.store.put("c", b"3", tenant="acme")
        acme = self.store.list_audit_events(tenant="acme")["items"]
        globex = self.store.list_audit_events(tenant="globex")["items"]
        self.assertEqual([event["seq"] for event in acme], [1, 2])
        self.assertEqual([event["key"] for event in acme], ["a", "c"])
        self.assertEqual([event["seq"] for event in globex], [1])
        # each tenant chains only its own events
        self.assertEqual(acme[1]["prev_hash"], acme[0]["hash"])
        self.assertEqual(globex[0]["prev_hash"], "0" * 64)

    def test_unknown_tenant_gets_empty_page(self):
        self.store.put("a", b"1")
        page = self.store.list_audit_events(tenant="ghost")
        self.assertEqual(page, {"items": [], "next_after": None})
        result = self.store.verify_audit_log(tenant="ghost")
        self.assertEqual(result, {"ok": True, "events": 0,
                                  "last_seq": None, "head": None})


class TestPagination(AuditTestCase):
    def test_pages_follow_seq_cursor(self):
        for index in range(5):
            self.store.put("k%d" % index, b"x")
        page1 = self.store.list_audit_events(limit=2)
        self.assertEqual([event["seq"] for event in page1["items"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)
        page2 = self.store.list_audit_events(after=page1["next_after"], limit=2)
        self.assertEqual([event["seq"] for event in page2["items"]], [3, 4])
        self.assertEqual(page2["next_after"], 4)
        page3 = self.store.list_audit_events(after=page2["next_after"], limit=2)
        self.assertEqual([event["seq"] for event in page3["items"]], [5])
        self.assertIsNone(page3["next_after"])

    def test_after_zero_and_beyond_end(self):
        self.store.put("k", b"x")
        self.assertEqual(len(self.store.list_audit_events(after=0)["items"]), 1)
        page = self.store.list_audit_events(after=99)
        self.assertEqual(page, {"items": [], "next_after": None})

    def test_invalid_query_parameters(self):
        for kwargs in ({"after": -1}, {"after": "1"}, {"after": 1.5},
                       {"after": True}, {"limit": 0}, {"limit": 1001},
                       {"limit": -3}, {"limit": "2"}, {"limit": True},
                       {"tenant": "BAD TENANT"}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.list_audit_events(**kwargs)
            self.assertEqual(str(ctx.exception), "invalid audit query")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.verify_audit_log(tenant="BAD TENANT")
        self.assertEqual(str(ctx.exception), "invalid audit query")


class TestVerify(AuditTestCase):
    def test_verify_reports_chain_head(self):
        self.assertEqual(self.store.verify_audit_log(),
                         {"ok": True, "events": 0,
                          "last_seq": None, "head": None})
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        events = self.read_log()
        self.assertEqual(self.store.verify_audit_log(),
                         {"ok": True, "events": 2,
                          "last_seq": 2, "head": events[-1]["hash"]})

    def test_verify_is_per_tenant(self):
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"2")
        result = self.store.verify_audit_log(tenant="acme")
        self.assertEqual(result["events"], 1)
        self.assertEqual(result["last_seq"], 1)


class TestPersistence(AuditTestCase):
    def test_restart_continues_sequence(self):
        self.store.put("a", b"1")
        reopened = ContentAddressedStore(self.root)
        reopened.put("b", b"2")
        events = reopened.list_audit_events()["items"]
        self.assertEqual([event["seq"] for event in events], [1, 2])
        self.assertEqual(events[1]["prev_hash"], events[0]["hash"])
        self.assertEqual(reopened.verify_audit_log()["events"], 2)

    def test_directory_without_log_opens_empty(self):
        self.assertFalse(os.path.exists(self.audit_path))
        store = ContentAddressedStore(self.root)
        self.assertEqual(store.verify_audit_log(),
                         {"ok": True, "events": 0,
                          "last_seq": None, "head": None})
        self.assertEqual(store.list_audit_events(),
                         {"items": [], "next_after": None})

    def test_trailing_incomplete_record_is_discarded(self):
        self.store.put("a", b"1")
        with open(self.audit_path, "ab") as handle:
            handle.write(b'{"seq": 2, "ten')
        size_before = os.path.getsize(self.audit_path)
        reopened = ContentAddressedStore(self.root)
        # the partial record was dropped and the file truncated
        self.assertLess(os.path.getsize(self.audit_path), size_before)
        self.assertEqual(reopened.verify_audit_log()["events"], 1)
        reopened.put("b", b"2")
        events = reopened.list_audit_events()["items"]
        self.assertEqual([event["seq"] for event in events], [1, 2])
        self.assertEqual(reopened.verify_audit_log()["events"], 2)

    def test_mid_file_format_error_is_corrupted(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "rb") as handle:
            lines = handle.readlines()
        lines[0] = b"{not json\n"
        with open(self.audit_path, "wb") as handle:
            handle.writelines(lines)
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_sequence_gap_is_corrupted(self):
        for index in range(3):
            self.store.put("k%d" % index, b"x")
        with open(self.audit_path, "rb") as handle:
            lines = handle.readlines()
        del lines[1]
        with open(self.audit_path, "wb") as handle:
            handle.writelines(lines)
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_chain_mismatch_is_corrupted(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "rb") as handle:
            lines = handle.readlines()
        event = json.loads(lines[1])
        event["prev_hash"] = "1" * 64
        # recompute the record hash so only the linkage is broken
        payload = {name: value for name, value in event.items()
                   if name != "hash"}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        event["hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        lines[1] = (json.dumps(event, sort_keys=True, separators=(",", ":"))
                    + "\n").encode("utf-8")
        with open(self.audit_path, "wb") as handle:
            handle.writelines(lines)
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_tampered_record_hash_is_corrupted(self):
        self.store.put("a", b"1")
        with open(self.audit_path, "rb") as handle:
            lines = handle.readlines()
        event = json.loads(lines[0])
        event["operation"] = "delete"  # tamper without rehashing
        lines[0] = (json.dumps(event, sort_keys=True, separators=(",", ":"))
                    + "\n").encode("utf-8")
        with open(self.audit_path, "wb") as handle:
            handle.writelines(lines)
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")


class TestAuditIoError(AuditTestCase):
    def test_failed_write_rolls_back_object(self):
        self.store.put("k", b"old")
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("k", b"new")
        self.assertEqual(str(ctx.exception), "audit io error")
        os.rmdir(self.audit_path)
        self.assertEqual(self.store.get("k")[0], b"old")

    def test_failed_write_rolls_back_new_object_and_quota(self):
        self.store.set_quota(max_objects=1, tenant="acme")
        self.store.put("a", b"1", tenant="acme")
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.put("b", b"2", tenant="acme")
        self.assertEqual(str(ctx.exception), "audit io error")
        os.rmdir(self.audit_path)
        self.assertEqual(self.store.get_usage(tenant="acme")["used_objects"], 1)
        with self.assertRaises(ObjectStoreError):
            self.store.head("b", tenant="acme")

    def test_failed_write_rolls_back_version(self):
        self.store.set_versioning(True)
        self.store.put("v", b"one")
        self.break_log()
        with self.assertRaises(ObjectStoreError):
            self.store.put("v", b"two")
        os.rmdir(self.audit_path)
        self.assertEqual(len(self.store.list_versions("v")["items"]), 1)
        self.assertEqual(self.store.get("v")[0], b"one")

    def test_failed_write_rolls_back_session(self):
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.begin_upload("u", 1, sha(b"x"))
        self.assertEqual(str(ctx.exception), "audit io error")
        os.rmdir(self.audit_path)
        self.assertEqual(self.store._uploads, {})

    def test_failed_write_rolls_back_policy(self):
        self.store.set_quota(max_bytes=100)
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.set_quota(max_bytes=50)
        self.assertEqual(str(ctx.exception), "audit io error")
        os.rmdir(self.audit_path)
        self.assertEqual(self.store.get_quota(),
                         {"max_bytes": 100, "max_objects": None})

    def test_failed_read_returns_no_data(self):
        self.store.put("k", b"data")
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("k")
        self.assertEqual(str(ctx.exception), "audit io error")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head("k")
        self.assertEqual(str(ctx.exception), "audit io error")

    def test_failed_failure_event_hides_original_error(self):
        self.break_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("missing")
        self.assertEqual(str(ctx.exception), "audit io error")


class TestCorruptionDetectedAtQuery(AuditTestCase):
    def test_query_after_external_tampering(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "rb") as handle:
            lines = handle.readlines()
        lines[0] = b"garbage\n"
        with open(self.audit_path, "wb") as handle:
            handle.writelines(lines)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.verify_audit_log()
        self.assertEqual(str(ctx.exception), "audit corrupted")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.list_audit_events()
        self.assertEqual(str(ctx.exception), "audit corrupted")


if __name__ == "__main__":
    unittest.main()

"""Tests for the persistent per-tenant audit log (Python API)."""

import hashlib
import json
import os
import shutil
import tempfile
import unittest

from objstore.store import (
    AUDIT_NAME,
    ContentAddressedStore,
    ObjectStoreError,
)


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class AuditTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-audit-")
        self.store = ContentAddressedStore(self.root)
        self.audit_path = os.path.join(self.root, AUDIT_NAME)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def events(self, tenant="default"):
        return self.store.list_audit_events(tenant=tenant)["items"]

    def operations(self, tenant="default"):
        return [event["operation"] for event in self.events(tenant)]

    def break_audit_log(self):
        if os.path.exists(self.audit_path):
            os.remove(self.audit_path)
        os.mkdir(self.audit_path)

    def restore_audit_log(self):
        os.rmdir(self.audit_path)


class TestAuditEvents(AuditTestCase):
    def test_put_writes_event_with_object_fields(self):
        entry = self.store.put("k", b"payload")
        events = self.events()
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["seq"], 1)
        self.assertEqual(event["tenant"], "default")
        self.assertEqual(event["operation"], "put")
        self.assertEqual(event["key"], "k")
        self.assertEqual(event["result"], "ok")
        self.assertEqual(event["sha256"], entry["sha256"])
        self.assertEqual(event["size"], len(b"payload"))
        self.assertIsNone(event["prev_hash"])
        self.assertTrue(isinstance(event["hash"], str))
        self.assertTrue(isinstance(event["timestamp"], str))

    def test_read_and_delete_operations_are_audited(self):
        self.store.put("k", b"v")
        self.store.get("k")
        self.store.head("k")
        self.store.list_objects()
        self.store.delete("k")
        self.assertEqual(self.operations(),
                         ["put", "get", "head", "list_objects", "delete"])
        self.assertEqual([event["seq"] for event in self.events()],
                         [1, 2, 3, 4, 5])

    def test_failure_event_carries_stable_error(self):
        with self.assertRaises(ObjectStoreError):
            self.store.get("missing")
        (event,) = self.events()
        self.assertEqual(event["operation"], "get")
        self.assertEqual(event["result"], "error")
        self.assertEqual(event["error"], "not found: missing")
        self.assertEqual(event["key"], "missing")
        self.assertNotIn("sha256", event)

    def test_quota_rejection_is_audited(self):
        self.store.set_quota(max_bytes=2)
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"too-long")
        event = self.events()[-1]
        self.assertEqual(event["operation"], "put")
        self.assertEqual(event["result"], "error")
        self.assertEqual(event["error"], "quota exceeded")

    def test_upload_session_operations_are_audited(self):
        digest = sha(b"chunked")
        session = self.store.begin_upload("k", 7, digest)
        self.store.append_upload(session, 0, b"chunked")
        self.store.upload_status(session)
        self.store.complete_upload(session)
        self.assertEqual(
            self.operations(),
            ["begin_upload", "append_upload", "upload_status",
             "complete_upload"])
        events = self.events()
        self.assertEqual(events[0]["session"], session)
        self.assertEqual(events[0]["key"], "k")
        completed = events[-1]
        self.assertEqual(completed["session"], session)
        self.assertEqual(completed["sha256"], digest)
        self.assertEqual(completed["size"], 7)

    def test_abort_is_audited(self):
        session = self.store.begin_upload("k", 1, sha(b"x"))
        self.store.abort_upload(session)
        self.assertEqual(self.operations(), ["begin_upload", "abort_upload"])

    def test_version_and_policy_operations_are_audited(self):
        self.store.set_versioning(True)
        self.store.put("k", b"v1")
        versions = self.store.list_versions("k")
        version_id = versions["items"][0]["version_id"]
        self.store.head_version("k", version_id)
        self.store.get_version("k", version_id)
        self.store.set_retention(max_versions=5)
        self.store.set_quota(max_bytes=100)
        self.store.get_quota()
        self.store.set_lifecycle(60)
        self.store.set_access_policy("signed", secret="s" * 16)
        self.store.clear_access_policy()
        self.store.set_rate_limit(60, max_requests=100)
        self.store.get_rate_limit()
        self.store.delete_version("k", version_id)
        self.assertEqual(
            self.operations(),
            ["set_versioning", "put", "list_versions", "head_version",
             "get_version", "set_retention", "set_quota", "get_quota",
             "set_lifecycle", "set_access_policy", "clear_access_policy",
             "set_rate_limit", "get_rate_limit", "delete_version"])
        event = self.events()[-1]
        self.assertEqual(event["key"], "k")
        self.assertEqual(event["version"], version_id)

    def test_blob_and_gc_are_audited_as_global(self):
        digest = self.store.put("k", b"v")["sha256"]
        self.store.blob(digest)
        self.store.collect_garbage()
        global_ops = self.operations("global")
        self.assertEqual(global_ops, ["blob", "collect_garbage"])
        self.assertNotIn("blob", self.operations())
        self.assertNotIn("collect_garbage", self.operations())
        event = self.events("global")[0]
        self.assertEqual(event["tenant"], "global")
        self.assertEqual(event["sha256"], digest)
        self.assertEqual(event["size"], 1)

    def test_audit_queries_record_no_events(self):
        self.store.put("k", b"v")
        before = len(self.events())
        self.store.list_audit_events()
        self.store.verify_audit_log()
        self.store.list_audit_events(tenant="global")
        self.assertEqual(len(self.events()), before)

    def test_content_and_secrets_are_never_logged(self):
        self.store.set_access_policy("signed", secret="top-secret-value")
        self.store.put("k", b"sensitive-payload-bytes")
        with open(self.audit_path, "rb") as handle:
            raw = handle.read()
        self.assertNotIn(b"top-secret-value", raw)
        self.assertNotIn(b"sensitive-payload-bytes", raw)

    def test_tenants_have_independent_sequences(self):
        self.store.put("a", b"1", tenant="acme")
        self.store.put("b", b"2", tenant="default")
        self.store.put("c", b"3", tenant="acme")
        self.assertEqual([e["seq"] for e in self.events("acme")], [1, 2])
        self.assertEqual([e["seq"] for e in self.events("default")], [1])
        acme = self.events("acme")
        self.assertEqual(acme[1]["prev_hash"], acme[0]["hash"])

    def test_unknown_tenant_gets_empty_page(self):
        self.store.put("k", b"v")
        page = self.store.list_audit_events(tenant="ghost")
        self.assertEqual(page, {"items": [], "next_after": None})


class TestAuditQuery(AuditTestCase):
    def test_pagination_follows_next_after(self):
        for index in range(5):
            self.store.put("k%d" % index, b"v")
        page1 = self.store.list_audit_events(limit=2)
        self.assertEqual([e["seq"] for e in page1["items"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)
        page2 = self.store.list_audit_events(after=page1["next_after"],
                                             limit=2)
        self.assertEqual([e["seq"] for e in page2["items"]], [3, 4])
        page3 = self.store.list_audit_events(after=page2["next_after"],
                                             limit=2)
        self.assertEqual([e["seq"] for e in page3["items"]], [5])
        self.assertIsNone(page3["next_after"])

    def test_after_zero_lists_everything(self):
        self.store.put("k", b"v")
        page = self.store.list_audit_events(after=0)
        self.assertEqual(len(page["items"]), 1)

    def test_invalid_queries_are_rejected(self):
        for kwargs in ({"after": -1}, {"after": "1"}, {"after": 1.5},
                       {"after": True}, {"limit": 0}, {"limit": 1001},
                       {"limit": -1}, {"limit": "10"}, {"limit": None},
                       {"tenant": "BAD TENANT"}, {"tenant": 7}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.list_audit_events(**kwargs)
            self.assertEqual(str(ctx.exception), "invalid audit query",
                             kwargs)

    def test_verify_rejects_invalid_tenant(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.verify_audit_log(tenant="BAD TENANT")
        self.assertEqual(str(ctx.exception), "invalid audit query")


class TestAuditVerify(AuditTestCase):
    def test_empty_log_verifies(self):
        result = self.store.verify_audit_log()
        self.assertEqual(result, {"ok": True, "events": 0,
                                  "last_seq": None, "head": None})

    def test_verify_summarizes_the_chain(self):
        self.store.put("k", b"v")
        self.store.head("k")
        self.store.put("x", b"y", tenant="acme")
        result = self.store.verify_audit_log()
        events = self.events()
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["events"], 2)
        self.assertEqual(result["last_seq"], 2)
        self.assertEqual(result["head"], events[-1]["hash"])
        acme = self.store.verify_audit_log(tenant="acme")
        self.assertEqual(acme["events"], 1)
        self.assertEqual(acme["last_seq"], 1)

    def test_restart_continues_sequence_and_chain(self):
        self.store.put("k", b"v")
        head = self.events()[-1]["hash"]
        reopened = ContentAddressedStore(self.root)
        reopened.put("k2", b"v2")
        events = reopened.list_audit_events()["items"]
        self.assertEqual([e["seq"] for e in events], [1, 2])
        self.assertEqual(events[1]["prev_hash"], head)
        self.assertEqual(reopened.verify_audit_log()["events"], 2)

    def test_trailing_incomplete_record_is_discarded(self):
        self.store.put("k", b"v")
        with open(self.audit_path, "ab") as handle:
            handle.write(b'{"seq": 2, "ten')
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.verify_audit_log()["events"], 1)
        reopened.put("k2", b"v2")
        events = reopened.list_audit_events()["items"]
        self.assertEqual([e["seq"] for e in events], [1, 2])
        # The partial bytes were truncated away, not kept as a gap.
        self.assertEqual(ContentAddressedStore(self.root)
                         .verify_audit_log()["events"], 2)

    def test_directory_without_log_opens_empty(self):
        fresh = os.path.join(self.root, "fresh")
        store = ContentAddressedStore(fresh)
        self.assertEqual(store.verify_audit_log()["events"], 0)
        self.assertEqual(store.list_audit_events(),
                         {"items": [], "next_after": None})

    def test_mid_log_format_error_is_corrupted(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "ab") as handle:
            handle.write(b"not json at all\n")
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_sequence_gap_is_corrupted(self):
        for index in range(3):
            self.store.put("k%d" % index, b"v")
        with open(self.audit_path, "rb") as handle:
            lines = handle.read().splitlines(keepends=True)
        with open(self.audit_path, "wb") as handle:
            handle.write(b"".join([lines[0], lines[2]]))
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_chain_mismatch_is_corrupted(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "rb") as handle:
            lines = handle.read().splitlines(keepends=True)
        event = json.loads(lines[0])
        event["operation"] = "delete"
        lines[0] = (json.dumps(event, sort_keys=True) + "\n").encode("utf-8")
        with open(self.audit_path, "wb") as handle:
            handle.write(b"".join(lines))
        with self.assertRaises(ObjectStoreError) as ctx:
            ContentAddressedStore(self.root)
        self.assertEqual(str(ctx.exception), "audit corrupted")

    def test_verify_detects_tampering_after_open(self):
        self.store.put("a", b"1")
        self.store.put("b", b"2")
        with open(self.audit_path, "rb") as handle:
            lines = handle.read().splitlines(keepends=True)
        with open(self.audit_path, "wb") as handle:
            handle.write(b"".join([lines[1], lines[0]]))
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.verify_audit_log()
        self.assertEqual(str(ctx.exception), "audit corrupted")


class TestAuditIoError(AuditTestCase):
    def test_failed_audit_write_rolls_back_put(self):
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.put("k", b"v")
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head("k")
        self.assertEqual(str(ctx.exception), "not found: k")
        self.assertEqual(self.store.get_usage()["used_bytes"], 0)

    def test_failed_audit_write_rolls_back_delete(self):
        self.store.put("k", b"v")
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.delete("k")
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v"))

    def test_failed_audit_write_rolls_back_policy(self):
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_quota(max_bytes=1)
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        self.assertIsNone(self.store.get_quota())
        self.store.put("k", b"much-longer-than-one-byte")

    def test_failed_audit_write_rolls_back_session_begin(self):
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.begin_upload("k", 1, sha(b"x"))
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        self.assertEqual(self.store.get_usage()["reserved_sessions"], 0)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_usage()["reserved_sessions"], 0)

    def test_failed_audit_write_rolls_back_complete(self):
        session = self.store.begin_upload("k", 1, sha(b"x"))
        self.store.append_upload(session, 0, b"x")
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.complete_upload(session)
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")
        # The session is still active and can be completed again.
        entry = self.store.complete_upload(session)
        self.assertEqual(entry["sha256"], sha(b"x"))

    def test_failed_audit_write_rolls_back_abort(self):
        session = self.store.begin_upload("k", 2, sha(b"xy"))
        self.store.append_upload(session, 0, b"xy")
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.abort_upload(session)
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        status = self.store.upload_status(session)
        self.assertEqual(status["completed"], False)
        self.assertEqual(status["offset"], 2)
        self.assertEqual(self.store.complete_upload(session)["sha256"],
                         sha(b"xy"))

    def test_failed_audit_write_hides_read_results(self):
        self.store.put("k", b"v")
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.get("k")
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()
        self.assertEqual(self.store.get("k")[0], b"v")

    def test_failed_failure_event_becomes_audit_io_error(self):
        self.break_audit_log()
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.get("missing")
            self.assertEqual(str(ctx.exception), "audit io error")
        finally:
            self.restore_audit_log()


if __name__ == "__main__":
    unittest.main()

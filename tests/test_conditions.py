"""Unit tests for conditional writes (expected_sha256) in objstore.store."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class ConditionTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-cond-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)


class TestPutPreconditions(ConditionTestCase):
    def test_omitted_argument_stays_unconditional(self):
        self.store.put("k", b"v1")
        # no expected_sha256 at all: overwrites regardless of the old state
        entry = self.store.put("k", b"v2")
        self.assertEqual(entry["sha256"], sha(b"v2"))
        self.assertEqual(self.store.put("brand-new", b"x")["sha256"], sha(b"x"))

    def test_none_requires_absence(self):
        entry = self.store.put("k", b"v1", expected_sha256=None)
        self.assertEqual(entry["sha256"], sha(b"v1"))
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"v2", expected_sha256=None)
        # a different key being present is irrelevant
        self.assertEqual(
            self.store.put("other", b"x", expected_sha256=None)["sha256"],
            sha(b"x"))

    def test_digest_requires_existing_match(self):
        old = self.store.put("k", b"v1")
        # matching digest updates the object
        entry = self.store.put("k", b"v2", expected_sha256=old["sha256"])
        self.assertEqual(entry["sha256"], sha(b"v2"))
        # stale digest is rejected
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"v3", expected_sha256=old["sha256"])
        # a digest of content living under another key does not satisfy it
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"v4", expected_sha256=sha(b"elsewhere"))
        # missing key cannot match any digest
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("absent", b"x", expected_sha256=old["sha256"])

    def test_condition_compares_only_this_keys_digest(self):
        # content type is irrelevant: the same bytes with a new type still
        # satisfy a digest condition
        old = self.store.put("k", b"v1", content_type="text/a")
        entry = self.store.put("k", b"v1", content_type="text/b",
                               expected_sha256=old["sha256"])
        self.assertEqual(entry["content_type"], "text/b")
        # once the bytes change, the old digest condition no longer matches
        self.store.put("k", b"v2")
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"v3", content_type="text/b",
                           expected_sha256=old["sha256"])

    def test_failed_condition_changes_nothing(self):
        old = self.store.put("k", b"v1")
        self.store.put("other", b"untouched")
        rejected = b"brand-new-bytes"
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", rejected, expected_sha256=sha(b"nope"))
        self.assertEqual(self.store.head("k"), old)
        self.assertEqual(self.store.get("k")[0], b"v1")
        # the rejected bytes never reached the blob directory
        self.assertNotIn(sha(rejected), self.store.blob_digests())
        self.assertEqual(self.store.head("other")["sha256"], sha(b"untouched"))

    def test_invalid_precondition_values(self):
        for bad in ("", "xyz", "A" * 64, "0" * 63, "0" * 65,
                    12, 0, 1.5, True, False, b"0" * 64, [], {}):
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z",
                                        msg=repr(bad)):
                self.store.put("k", b"x", expected_sha256=bad)

    def test_invalid_precondition_is_reported_without_writing(self):
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"v", expected_sha256="not-a-digest")
        self.assertEqual(self.store.blob_digests(), [])
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")


class TestDeletePreconditions(ConditionTestCase):
    def test_omitted_argument_stays_unconditional(self):
        self.store.put("k", b"v")
        self.store.delete("k")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_none_on_existing_key_fails_and_keeps_object(self):
        self.store.put("k", b"v")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=None)
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v"))

    def test_none_on_absent_key_still_reports_not_found(self):
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found: ghost\Z"):
            self.store.delete("ghost", expected_sha256=None)

    def test_digest_match_deletes(self):
        self.store.put("k", b"v")
        self.store.delete("k", expected_sha256=sha(b"v"))
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_digest_mismatch_fails_and_keeps_object(self):
        self.store.put("k", b"v")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=sha(b"other"))
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v"))

    def test_digest_on_absent_key_is_not_found_not_failed(self):
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found: ghost\Z"):
            self.store.delete("ghost", expected_sha256=sha(b"v"))

    def test_invalid_precondition_value(self):
        self.store.put("k", b"v")
        for bad in ("", "xyz", "A" * 64, 7, True):
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z"):
                self.store.delete("k", expected_sha256=bad)
        # object survives the rejected calls
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v"))


class TestConditionalUploadSessions(ConditionTestCase):
    def begin(self, key, data, expected):
        return self.store.begin_upload(key, len(data), sha(data),
                                       expected_sha256=expected)

    def test_condition_is_saved_but_never_checked_at_begin_or_append(self):
        self.store.put("k", b"already-here")
        # a require-absent session is created even though the key exists now
        session_id = self.begin("k", b"new-bytes", None)
        self.assertEqual(self.store.append_upload(session_id, 0, b"new"), 3)
        self.assertEqual(self.store.append_upload(session_id, 3, b"-bytes"), 9)
        # the existing object is untouched while the session is in progress
        self.assertEqual(self.store.get("k")[0], b"already-here")

    def test_complete_with_none_publishes_only_when_absent(self):
        session_id = self.begin("k", b"ab", None)
        self.store.append_upload(session_id, 0, b"ab")
        self.assertEqual(self.store.complete_upload(session_id)["sha256"],
                         sha(b"ab"))
        # a second require-absent session loses: the key now exists
        second = self.begin("k", b"cd", None)
        self.store.append_upload(second, 0, b"cd")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(second)

    def test_complete_with_digest_publishes_only_on_match(self):
        self.store.put("k", b"old")
        good = self.begin("k", b"new", sha(b"old"))
        self.store.append_upload(good, 0, b"new")
        self.assertEqual(self.store.complete_upload(good)["sha256"], sha(b"new"))
        stale = self.begin("k", b"newer", sha(b"old"))
        self.store.append_upload(stale, 0, b"newer")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(stale)
        # a digest condition cannot match a missing key either
        ghost = self.store.begin_upload("g", 1, sha(b"x"),
                                        expected_sha256=sha(b"nope"))
        self.store.append_upload(ghost, 0, b"x")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(ghost)

    def test_failed_complete_keeps_progress_object_and_blob_state(self):
        self.store.put("k", b"old")
        session_id = self.begin("k", b"new-bytes", None)
        self.store.append_upload(session_id, 0, b"new-bytes")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session_id)
        # object and old blob untouched, published blob never written
        self.assertEqual(self.store.get("k")[0], b"old")
        self.assertEqual(self.store.blob_digests(), [sha(b"old")])
        # session stays incomplete with all confirmed bytes
        status = self.store.upload_status(session_id)
        self.assertEqual(status["offset"], 9)
        self.assertFalse(status["completed"])
        # once the condition can be met, completing again publishes
        self.store.delete("k")
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(b"new-bytes"))

    def test_failed_complete_can_be_aborted(self):
        self.store.put("k", b"old")
        session_id = self.begin("k", b"new", None)
        self.store.append_upload(session_id, 0, b"new")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session_id)
        self.assertIsNone(self.store.abort_upload(session_id))
        self.assertEqual(self.store.get("k")[0], b"old")

    def test_length_and_hash_are_checked_before_the_condition(self):
        self.store.put("k", b"old")
        incomplete = self.begin("k", b"full", None)
        self.store.append_upload(incomplete, 0, b"fu")
        with self.assertRaisesRegex(ObjectStoreError, r"incomplete"):
            self.store.complete_upload(incomplete)
        bad_hash = self.store.begin_upload("k", 3, sha(b"zzz"),
                                           expected_sha256=None)
        self.store.append_upload(bad_hash, 0, b"abc")
        with self.assertRaisesRegex(ObjectStoreError, r"hash mismatch"):
            self.store.complete_upload(bad_hash)

    def test_repeated_complete_does_not_recheck_the_condition(self):
        session_id = self.begin("k", b"ab", None)
        self.store.append_upload(session_id, 0, b"ab")
        first = self.store.complete_upload(session_id)
        self.store.put("k", b"overwritten")
        self.store.delete("k")
        # later writes and deletes are neither blocked nor overwritten
        self.assertEqual(self.store.complete_upload(session_id), first)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_begin_validates_the_condition(self):
        for bad in ("", "ZZ", "0" * 63, 9, True):
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z"):
                self.store.begin_upload("k", 1, sha(b"x"),
                                        expected_sha256=bad)
        self.assertFalse(os.path.exists(self.store.uploads_dir))


class TestConditionalUploadPersistence(ConditionTestCase):
    def test_condition_survives_reopen(self):
        session_id = self.store.begin_upload("k", 2, sha(b"ab"),
                                             expected_sha256=None)
        self.store.append_upload(session_id, 0, b"ab")
        reopened = ContentAddressedStore(self.root)
        # block the key only through the reopened instance
        reopened.put("k", b"blocker")
        with self.assertRaises(ObjectStoreError):
            reopened.complete_upload(session_id)
        reopened.delete("k")
        self.assertEqual(reopened.complete_upload(session_id)["sha256"],
                         sha(b"ab"))

    def test_completed_conditional_publish_and_flag_agree_after_reopen(self):
        session_id = self.store.begin_upload("k", 2, sha(b"ab"),
                                             expected_sha256=None)
        self.store.append_upload(session_id, 0, b"ab")
        entry = self.store.complete_upload(session_id)
        reopened = ContentAddressedStore(self.root)
        self.assertTrue(reopened.upload_status(session_id)["completed"])
        self.assertEqual(reopened.complete_upload(session_id), entry)
        self.assertEqual(reopened.get("k"), (b"ab", entry))

    def test_old_session_file_without_condition_is_unconditional(self):
        # sessions written by older versions carry no expected_sha256 field
        self.store.put("legacy", b"existing")
        session_id = self.store.begin_upload("legacy", 1, sha(b"l"))
        self.store.append_upload(session_id, 0, b"l")
        path = self.store._session_meta_path(session_id)
        with open(path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertNotIn("expected_sha256", document)
        reopened = ContentAddressedStore(self.root)
        # unconditional publish overwrites the existing object
        self.assertEqual(reopened.complete_upload(session_id)["sha256"],
                         sha(b"l"))

    def test_corrupt_condition_field_is_reported(self):
        session_id = self.store.begin_upload("k", 1, sha(b"l"),
                                             expected_sha256=None)
        path = self.store._session_meta_path(session_id)
        with open(path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        document["expected_sha256"] = "not-a-digest"
        with open(path, "wb") as handle:
            handle.write(json.dumps(document).encode("utf-8"))
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)

    def test_interrupted_conditional_complete_is_all_or_nothing(self):
        import objstore.store as store_module
        self.store.put("k", b"old")
        data = b"new-content"
        session_id = self.store.begin_upload(
            "k", len(data), sha(data), expected_sha256=sha(b"old"))
        self.store.append_upload(session_id, 0, data)
        real_atomic_write = store_module._atomic_write

        def boom(path, written):
            if os.path.basename(path) == "index.json":
                raise OSError("simulated crash")
            return real_atomic_write(path, written)

        store_module._atomic_write = boom
        try:
            with self.assertRaisesRegex(ObjectStoreError, r"\Aupload io error\Z"):
                self.store.complete_upload(session_id)
        finally:
            store_module._atomic_write = real_atomic_write
        reopened = ContentAddressedStore(self.root)
        # either fully pre-publish or fully published: here it is pre-publish
        self.assertEqual(reopened.get("k")[0], b"old")
        self.assertFalse(reopened.upload_status(session_id)["completed"])
        # the condition still applies and now succeeds against the old digest
        entry = reopened.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(data))
        again = ContentAddressedStore(self.root)
        self.assertEqual(again.get("k")[0], data)
        self.assertTrue(again.upload_status(session_id)["completed"])


class TestConditionalGarbageCollection(ConditionTestCase):
    def test_completion_record_keeps_no_blob_even_with_a_condition(self):
        data = b"collect-me"
        session_id = self.store.begin_upload("k", len(data), sha(data),
                                             expected_sha256=None)
        self.store.append_upload(session_id, 0, data)
        entry = self.store.complete_upload(session_id)
        self.store.delete("k")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], [entry["sha256"]])

    def test_failed_conditional_publish_leaves_no_orphan_blob(self):
        self.store.put("k", b"old")
        session_id = self.store.begin_upload("k", 3, sha(b"new"),
                                             expected_sha256=None)
        self.store.append_upload(session_id, 0, b"new")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session_id)
        # the session part file is not a blob; GC still finds nothing extra
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"], [])
        self.assertEqual(self.store.blob_digests(), [sha(b"old")])


class TestConditionalConcurrency(ConditionTestCase):
    def test_competing_same_digest_puts_have_one_winner(self):
        self.store.put("k", b"v0")
        old = sha(b"v0")
        workers = 8
        barrier = threading.Barrier(workers)
        outcomes = []
        lock = threading.Lock()

        def worker(worker_id):
            payload = ("v-worker-%d" % worker_id).encode("utf-8")
            barrier.wait()
            try:
                self.store.put("k", payload, expected_sha256=old)
                with lock:
                    outcomes.append(("ok", sha(payload)))
            except ObjectStoreError as exc:
                self.assertEqual(str(exc), "precondition failed")
                with lock:
                    outcomes.append(("fail", sha(payload)))

        threads = [threading.Thread(target=worker, args=(w,))
                   for w in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        winners = [digest for result, digest in outcomes if result == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(outcomes), workers)
        # the stored object is exactly the single winner's content
        final = self.store.head("k")["sha256"]
        self.assertEqual(final, winners[0])
        on_disk = self.store.blob_digests()
        # every losing payload's blob is absent from the blob directory
        for result, digest in outcomes:
            if result == "fail":
                self.assertNotIn(digest, on_disk)

    def test_conditional_put_and_conditional_complete_are_mutually_exclusive(self):
        self.store.put("k", b"v0")
        old = sha(b"v0")
        upload_bytes = b"from-upload"
        session_id = self.store.begin_upload("k", len(upload_bytes),
                                             sha(upload_bytes),
                                             expected_sha256=old)
        self.store.append_upload(session_id, 0, upload_bytes)
        put_bytes = b"from-put"
        barrier = threading.Barrier(2)
        results = {}

        def do_put():
            barrier.wait()
            try:
                self.store.put("k", put_bytes, expected_sha256=old)
                results["put"] = "ok"
            except ObjectStoreError:
                results["put"] = "failed"

        def do_complete():
            barrier.wait()
            try:
                self.store.complete_upload(session_id)
                results["complete"] = "ok"
            except ObjectStoreError:
                results["complete"] = "failed"

        threads = [threading.Thread(target=do_put),
                   threading.Thread(target=do_complete)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(sorted(results), ["complete", "put"])
        self.assertEqual(len([v for v in results.values() if v == "ok"]), 1)
        final = self.store.head("k")["sha256"]
        self.assertIn(final, (sha(put_bytes), sha(upload_bytes)))
        self.assertEqual(final, sha(put_bytes) if results["put"] == "ok"
                         else sha(upload_bytes))


if __name__ == "__main__":
    unittest.main()

"""Unit tests for conditional writes (expected_sha256 preconditions)."""

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


class PreconditionTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-precond-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)


class TestPutPreconditions(PreconditionTestCase):
    def test_omitted_precondition_keeps_unconditional_behaviour(self):
        self.store.put("k", b"one")
        entry = self.store.put("k", b"two")
        self.assertEqual(entry["sha256"], sha(b"two"))
        self.assertEqual(self.store.get("k")[0], b"two")

    def test_none_requires_absent_key(self):
        entry = self.store.put("k", b"new", expected_sha256=None)
        self.assertEqual(entry["sha256"], sha(b"new"))
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"other", expected_sha256=None)
        self.assertEqual(self.store.get("k")[0], b"new")

    def test_digest_requires_matching_key(self):
        self.store.put("k", b"v1")
        entry = self.store.put("k", b"v2", expected_sha256=sha(b"v1"))
        self.assertEqual(entry["sha256"], sha(b"v2"))
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"v3", expected_sha256=sha(b"v1"))
        self.assertEqual(self.store.get("k")[0], b"v2")

    def test_digest_on_missing_key_fails(self):
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("k", b"v1", expected_sha256=sha(b"v1"))
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_condition_compares_only_the_key_digest(self):
        # a different content_type does not break a matching digest
        self.store.put("k", b"data", content_type="text/plain")
        entry = self.store.put("k", b"next", expected_sha256=sha(b"data"),
                               content_type="application/json")
        self.assertEqual(entry["content_type"], "application/json")
        # another key holding the digest does not satisfy the condition
        self.store.put("other", b"shared")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.put("fresh", b"x", expected_sha256=sha(b"shared"))

    def test_failed_precondition_changes_no_object_or_blob(self):
        self.store.put("k", b"keep")
        before = self.store.head("k")
        blobs_before = self.store.blob_digests()
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"replacement", expected_sha256=sha(b"other"))
        with self.assertRaises(ObjectStoreError):
            self.store.put("k", b"replacement", expected_sha256=None)
        self.assertEqual(self.store.head("k"), before)
        self.assertEqual(self.store.blob_digests(), blobs_before)

    def test_invalid_precondition_values(self):
        for bad in ["", "A" * 64, "0" * 63, "0" * 65, 0, True, 12, b"0" * 64,
                    ["0" * 64]]:
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z"):
                self.store.put("k", b"x", expected_sha256=bad)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_empty_content_digest_is_a_valid_precondition(self):
        self.store.put("k", b"")
        entry = self.store.put("k", b"full", expected_sha256=sha(b""))
        self.assertEqual(entry["sha256"], sha(b"full"))


class TestDeletePreconditions(PreconditionTestCase):
    def test_matching_digest_deletes(self):
        self.store.put("k", b"data")
        self.store.delete("k", expected_sha256=sha(b"data"))
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_mismatched_digest_keeps_key(self):
        self.store.put("k", b"data")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=sha(b"other"))
        self.assertEqual(self.store.get("k")[0], b"data")

    def test_none_requires_absent_key(self):
        self.store.put("k", b"data")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=None)
        self.assertEqual(self.store.get("k")[0], b"data")

    def test_satisfied_absence_still_reports_not_found(self):
        with self.assertRaisesRegex(ObjectStoreError, r"\Anot found: k\Z"):
            self.store.delete("k", expected_sha256=None)

    def test_digest_on_missing_key_fails_precondition(self):
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.delete("k", expected_sha256=sha(b"data"))

    def test_invalid_precondition_values(self):
        self.store.put("k", b"data")
        for bad in ["", "G" * 64, 7, False, None.__class__]:
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z"):
                self.store.delete("k", expected_sha256=bad)
        self.assertEqual(self.store.get("k")[0], b"data")

    def test_omitted_precondition_keeps_unconditional_behaviour(self):
        self.store.put("k", b"data")
        self.store.delete("k")
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")


class TestUploadPreconditions(PreconditionTestCase):
    def begin(self, key="k", data=b"hello world", expected=None,
              with_precondition=False):
        kwargs = {"expected_sha256": expected} if with_precondition else {}
        return self.store.begin_upload(key, len(data), sha(data), **kwargs)

    def fill(self, session_id, data=b"hello world"):
        self.store.append_upload(session_id, 0, data)

    def test_invalid_precondition_rejected_at_begin(self):
        for bad in ["", "A" * 64, "0" * 63, 3, True]:
            with self.assertRaisesRegex(ObjectStoreError,
                                        r"\Ainvalid precondition\Z"):
                self.store.begin_upload("k", 1, sha(b"x"), expected_sha256=bad)
        self.assertFalse(os.path.exists(self.store.uploads_dir))

    def test_precondition_not_checked_at_begin_or_append(self):
        self.store.put("k", b"existing")
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        self.assertEqual(self.store.upload_status(session_id)["offset"], 11)
        self.assertEqual(self.store.get("k")[0], b"existing")

    def test_absent_condition_publishes_when_key_missing(self):
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(b"hello world"))
        self.assertEqual(self.store.get("k")[0], b"hello world")

    def test_absent_condition_fails_when_key_exists(self):
        self.store.put("k", b"existing")
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(session_id)
        # object, progress and blob are untouched; the session stays active
        self.assertEqual(self.store.get("k")[0], b"existing")
        status = self.store.upload_status(session_id)
        self.assertEqual((status["offset"], status["completed"]), (11, False))
        self.assertNotIn(sha(b"hello world"), self.store.blob_digests())
        # the session can still be aborted
        self.assertIsNone(self.store.abort_upload(session_id))

    def test_failed_complete_can_be_retried(self):
        self.store.put("k", b"v1")
        session_id = self.begin("k", expected=sha(b"v1"), with_precondition=True)
        self.fill(session_id)
        self.store.put("k", b"v2")
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(session_id)
        # once the key matches the condition again, completion succeeds
        self.store.delete("k")
        self.store.put("k", b"v1")
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry["sha256"], sha(b"hello world"))

    def test_condition_uses_object_state_at_publish_time(self):
        self.store.put("k", b"v1")
        session_id = self.begin("k", expected=sha(b"v1"), with_precondition=True)
        self.fill(session_id)
        self.store.put("k", b"v2")  # digest changed after the session began
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            self.store.complete_upload(session_id)
        self.assertEqual(self.store.get("k")[0], b"v2")

    def test_matching_digest_publishes(self):
        self.store.put("k", b"v1")
        session_id = self.begin("k", expected=sha(b"v1"), with_precondition=True)
        self.fill(session_id)
        entry = self.store.complete_upload(session_id)
        self.assertEqual(self.store.get("k"), (b"hello world", entry))

    def test_precondition_survives_reopen(self):
        self.store.put("k", b"v1")
        session_id = self.begin("k", expected=sha(b"v1"), with_precondition=True)
        self.fill(session_id)
        self.store.put("k", b"v2")
        reopened = ContentAddressedStore(self.root)
        with self.assertRaisesRegex(ObjectStoreError, r"\Aprecondition failed\Z"):
            reopened.complete_upload(session_id)
        reopened.delete("k")
        reopened.put("k", b"v1")
        entry = reopened.complete_upload(session_id)
        self.assertEqual(ContentAddressedStore(self.root).get("k"),
                         (b"hello world", entry))

    def test_session_without_precondition_stays_unconditional(self):
        self.store.put("k", b"v1")
        session_id = self.begin("k")  # no precondition stored
        self.fill(session_id)
        entry = self.store.complete_upload(session_id)
        self.assertEqual(self.store.get("k"), (b"hello world", entry))

    def test_repeated_complete_does_not_recheck_precondition(self):
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        first = self.store.complete_upload(session_id)
        # later writes and deletes are not overwritten and no recheck happens
        self.store.put("k", b"newer")
        self.assertEqual(self.store.complete_upload(session_id), first)
        self.assertEqual(self.store.get("k")[0], b"newer")
        self.store.delete("k")
        self.assertEqual(self.store.complete_upload(session_id), first)

    def test_completed_result_survives_reopen_with_precondition(self):
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        entry = self.store.complete_upload(session_id)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.complete_upload(session_id), entry)
        self.assertTrue(reopened.upload_status(session_id)["completed"])

    def test_gc_still_ignores_completion_records(self):
        session_id = self.begin("k", expected=None, with_precondition=True)
        self.fill(session_id)
        entry = self.store.complete_upload(session_id)
        self.store.delete("k")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], [entry["sha256"]])

    def test_corrupted_precondition_in_session_file_is_reported(self):
        session_id = self.begin("k", expected=None, with_precondition=True)
        path = self.store._session_meta_path(session_id)
        with open(path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        document["expected_sha256"] = "not-a-digest"
        with open(path, "wb") as handle:
            handle.write(json.dumps(document).encode("utf-8"))
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)


class TestPreconditionConcurrency(PreconditionTestCase):
    def test_only_one_writer_with_same_old_digest_succeeds(self):
        self.store.put("k", b"base")
        old = sha(b"base")
        barrier = threading.Barrier(2)
        outcomes = []

        def writer(payload):
            barrier.wait(timeout=10)
            try:
                self.store.put("k", payload, expected_sha256=old)
            except ObjectStoreError as exc:
                outcomes.append(("error", str(exc)))
            else:
                outcomes.append(("ok", payload))

        threads = [threading.Thread(target=writer, args=(b"first",)),
                   threading.Thread(target=writer, args=(b"second",))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        outcomes.sort()
        self.assertEqual([kind for kind, _ in outcomes], ["error", "ok"])
        self.assertEqual(outcomes[0][1], "precondition failed")
        winner = outcomes[1][1]
        self.assertEqual(self.store.get("k")[0], winner)
        self.assertEqual(self.store.head("k")["sha256"], sha(winner))

    def test_concurrent_complete_and_put_with_same_digest(self):
        self.store.put("k", b"base")
        old = sha(b"base")
        session_id = self.store.begin_upload("k", 7, sha(b"session"),
                                             expected_sha256=old)
        self.store.append_upload(session_id, 0, b"session")
        barrier = threading.Barrier(2)
        outcomes = []

        def completer():
            barrier.wait(timeout=10)
            try:
                self.store.complete_upload(session_id)
                outcomes.append("complete")
            except ObjectStoreError:
                outcomes.append("complete-failed")

        def putter():
            barrier.wait(timeout=10)
            try:
                self.store.put("k", b"put-win", expected_sha256=old)
                outcomes.append("put")
            except ObjectStoreError:
                outcomes.append("put-failed")

        threads = [threading.Thread(target=completer),
                   threading.Thread(target=putter)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(sorted(outcomes),
                         ["complete", "put-failed"] if "complete" in outcomes
                         else ["complete-failed", "put"])
        final = self.store.get("k")[0]
        self.assertIn(final, (b"session", b"put-win"))


if __name__ == "__main__":
    unittest.main()

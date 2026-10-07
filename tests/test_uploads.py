"""Unit tests for resumable uploads in objstore.store."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest

import objstore.store as store_module
from objstore.store import ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class UploadTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-upload-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)

    def begin(self, key="k", data=b"hello world", content_type=None):
        return self.store.begin_upload(key, len(data), sha(data),
                                       content_type=content_type)


class TestBeginUpload(UploadTestCase):
    def test_begin_returns_unique_string_ids(self):
        first, second = self.begin(), self.begin()
        self.assertIsInstance(first, str)
        self.assertIsInstance(second, str)
        self.assertNotEqual(first, second)

    def test_begin_does_not_create_or_change_objects(self):
        self.store.put("other", b"untouched")
        before = self.store.head("other")
        session_id = self.begin("k", b"data")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 0)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")
        self.assertEqual(self.store.head("other"), before)
        self.assertEqual(self.store.blob_digests(), [sha(b"untouched")])

    def test_begin_validates_arguments(self):
        for bad_key in ["", "/a", "a//b", "a/../b", None, 5]:
            with self.assertRaises(ObjectStoreError):
                self.store.begin_upload(bad_key, 1, sha(b"x"))
        for bad_size in [-1, True, False, 1.5, "3", None]:
            with self.assertRaises(ObjectStoreError):
                self.store.begin_upload("k", bad_size, sha(b"x"))
        for bad_sha in ["", "A" * 64, "0" * 63, None, 12]:
            with self.assertRaises(ObjectStoreError):
                self.store.begin_upload("k", 1, bad_sha)
        with self.assertRaises(ObjectStoreError):
            self.store.begin_upload("k", 1, sha(b"x"), content_type=7)
        # nothing was persisted for the rejected attempts
        self.assertFalse(os.path.exists(self.store.uploads_dir))

    def test_zero_size_declaration_is_allowed(self):
        session_id = self.begin("empty", b"")
        self.assertEqual(self.store.upload_status(session_id)["size"], 0)


class TestUploadStatus(UploadTestCase):
    def test_status_reports_declaration_and_progress(self):
        session_id = self.store.begin_upload("docs/a.txt", 11, sha(b"hello world"),
                                             content_type="text/plain")
        status = self.store.upload_status(session_id)
        self.assertEqual(status, {"key": "docs/a.txt", "size": 11,
                                  "sha256": sha(b"hello world"),
                                  "content_type": "text/plain",
                                  "offset": 0, "completed": False})
        self.store.append_upload(session_id, 0, b"hello ")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 6)

    def test_unknown_or_invalid_session_ids_are_rejected(self):
        for bad in [None, 5, "", "../../../../etc/passwd", "z" * 32]:
            with self.assertRaises(ObjectStoreError):
                self.store.upload_status(bad)
        with self.assertRaises(ObjectStoreError):
            self.store.upload_status("a" * 32)


class TestAppendUpload(UploadTestCase):
    def test_chunks_append_in_order_and_return_received_length(self):
        session_id = self.begin()
        self.assertEqual(self.store.append_upload(session_id, 0, b"hello "), 6)
        self.assertEqual(self.store.append_upload(session_id, 6, b"world"), 11)

    def test_payload_kinds_match_put(self):
        session_id = self.begin(data=b"abc")
        self.assertEqual(self.store.append_upload(session_id, 0, bytearray(b"ab")), 2)
        self.assertEqual(self.store.append_upload(session_id, 2, memoryview(b"c")), 3)

    def test_identical_resend_inside_received_range_is_a_noop(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello ")
        self.store.append_upload(session_id, 6, b"world")
        self.assertEqual(self.store.append_upload(session_id, 0, b"hello"), 11)
        self.assertEqual(self.store.append_upload(session_id, 3, b"lo wo"), 11)
        self.assertEqual(self.store.append_upload(session_id, 6, b"world"), 11)
        self.assertEqual(self.store.upload_status(session_id)["offset"], 11)

    def test_conflicting_resend_fails(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello ")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, b"HELL")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 4, b"ox")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 6)

    def test_overlap_past_received_end_fails(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello ")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 4, b"o wor")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 6)

    def test_gap_fails(self):
        session_id = self.begin()
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 2, b"llo")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 0)

    def test_chunk_may_not_exceed_declared_size(self):
        session_id = self.begin(data=b"abc")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, b"abcd")
        self.store.append_upload(session_id, 0, b"ab")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 2, b"cd")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 2)

    def test_empty_chunk_only_at_received_end(self):
        session_id = self.begin()
        self.assertEqual(self.store.append_upload(session_id, 0, b""), 0)
        self.store.append_upload(session_id, 0, b"hello ")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, b"")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 3, b"")
        self.assertEqual(self.store.append_upload(session_id, 6, b""), 6)
        self.assertEqual(self.store.upload_status(session_id)["offset"], 6)

    def test_invalid_append_arguments(self):
        session_id = self.begin()
        for bad_offset in [-1, True, 1.5, "0", None]:
            with self.assertRaises(ObjectStoreError):
                self.store.append_upload(session_id, bad_offset, b"x")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, "not bytes")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload("f" * 32, 0, b"x")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 0)


class TestCompleteUpload(UploadTestCase):
    def test_complete_publishes_object_and_returns_put_metadata(self):
        self.store.put("k", b"old-bytes")
        session_id = self.begin("k", b"hello world", content_type="text/plain")
        self.store.append_upload(session_id, 0, b"hello ")
        # the old object stays readable until completion
        self.assertEqual(self.store.get("k")[0], b"old-bytes")
        self.store.append_upload(session_id, 6, b"world")
        entry = self.store.complete_upload(session_id)
        self.assertEqual(entry, {"sha256": sha(b"hello world"), "size": 11,
                                 "content_type": "text/plain"})
        self.assertEqual(self.store.get("k"), (b"hello world", entry))
        status = self.store.upload_status(session_id)
        self.assertEqual(status["completed"], True)
        self.assertEqual(status["offset"], 11)

    def test_incomplete_content_is_never_published(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session_id)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")
        with self.assertRaises(ObjectStoreError):
            self.store.blob(sha(b"hello world"))
        self.assertEqual(self.store.blob_digests(), [])

    def test_hash_mismatch_fails_and_keeps_progress(self):
        session_id = self.store.begin_upload("k", 3, sha(b"xyz"))
        self.store.append_upload(session_id, 0, b"abc")
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(session_id)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")
        self.assertEqual(self.store.upload_status(session_id)["offset"], 3)
        self.assertEqual(self.store.upload_status(session_id)["completed"], False)

    def test_zero_byte_object_still_requires_matching_hash(self):
        bad = self.store.begin_upload("k", 0, sha(b"x"))
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload(bad)
        good = self.begin("k", b"")
        entry = self.store.complete_upload(good)
        self.assertEqual(entry, {"sha256": sha(b""), "size": 0, "content_type": None})
        self.assertEqual(self.store.get("k"), (b"", entry))

    def test_append_after_complete_fails(self):
        session_id = self.begin(data=b"ab")
        self.store.append_upload(session_id, 0, b"ab")
        self.store.complete_upload(session_id)
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 2, b"cd")
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, b"ab")

    def test_repeated_complete_returns_first_result_without_republishing(self):
        session_id = self.begin("k", b"ab", content_type="text/plain")
        self.store.append_upload(session_id, 0, b"ab")
        first = self.store.complete_upload(session_id)
        # later writes and deletes of the key are not overwritten
        self.store.put("k", b"newer")
        self.assertEqual(self.store.complete_upload(session_id), first)
        self.assertEqual(self.store.get("k")[0], b"newer")
        self.store.delete("k")
        self.assertEqual(self.store.complete_upload(session_id), first)
        with self.assertRaises(ObjectStoreError):
            self.store.head("k")

    def test_unknown_session_cannot_complete(self):
        with self.assertRaises(ObjectStoreError):
            self.store.complete_upload("0" * 32)

    def test_completed_upload_shares_blobs_with_put(self):
        self.store.put("twin", b"same-bytes")
        session_id = self.begin("k", b"same-bytes")
        self.store.append_upload(session_id, 0, b"same-bytes")
        self.store.complete_upload(session_id)
        self.assertEqual(self.store.blob_digests(), [sha(b"same-bytes")])
        self.assertEqual(self.store.head("k")["sha256"], sha(b"same-bytes"))


class TestAbortUpload(UploadTestCase):
    def test_abort_removes_session_and_returns_none(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello")
        self.assertIsNone(self.store.abort_upload(session_id))
        for call in (self.store.upload_status, self.store.complete_upload,
                     self.store.abort_upload):
            with self.assertRaises(ObjectStoreError):
                call(session_id)
        with self.assertRaises(ObjectStoreError):
            self.store.append_upload(session_id, 0, b"x")
        self.assertFalse(os.path.exists(self.store.uploads_dir))

    def test_abort_of_completed_session_keeps_the_published_object(self):
        session_id = self.begin("k", b"ab")
        self.store.append_upload(session_id, 0, b"ab")
        entry = self.store.complete_upload(session_id)
        self.assertIsNone(self.store.abort_upload(session_id))
        self.assertEqual(self.store.get("k"), (b"ab", entry))
        with self.assertRaises(ObjectStoreError):
            self.store.upload_status(session_id)

    def test_abort_unknown_session_fails(self):
        with self.assertRaises(ObjectStoreError):
            self.store.abort_upload("1" * 32)


class TestUploadPersistence(UploadTestCase):
    def test_session_and_progress_survive_reopen(self):
        session_id = self.store.begin_upload("k", 11, sha(b"hello world"),
                                             content_type="text/plain")
        self.store.append_upload(session_id, 0, b"hello ")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.upload_status(session_id),
                         {"key": "k", "size": 11, "sha256": sha(b"hello world"),
                          "content_type": "text/plain", "offset": 6,
                          "completed": False})
        reopened.append_upload(session_id, 6, b"world")
        entry = reopened.complete_upload(session_id)
        self.assertEqual(ContentAddressedStore(self.root).get("k"),
                         (b"hello world", entry))

    def test_completed_status_and_result_survive_reopen(self):
        session_id = self.begin("k", b"ab")
        self.store.append_upload(session_id, 0, b"ab")
        entry = self.store.complete_upload(session_id)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.upload_status(session_id)["completed"], True)
        self.assertEqual(reopened.complete_upload(session_id), entry)
        self.assertEqual(reopened.get("k"), (b"ab", entry))

    def test_aborted_session_stays_unknown_after_reopen(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello")
        self.store.abort_upload(session_id)
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root).upload_status(session_id)

    def test_interrupted_append_leaves_old_or_whole_chunk_progress(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello ")
        real_save = self.store._save_session

        def boom(sid, session):
            raise ObjectStoreError("upload io error")

        self.store._save_session = boom
        try:
            with self.assertRaises(ObjectStoreError):
                self.store.append_upload(session_id, 6, b"world")
        finally:
            self.store._save_session = real_save
        # confirmed progress is unchanged and the unconfirmed tail is dropped
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.upload_status(session_id)["offset"], 6)
        self.assertEqual(reopened.append_upload(session_id, 6, b"world"), 11)
        self.assertEqual(reopened.complete_upload(session_id)["sha256"],
                         sha(b"hello world"))

    def test_interrupted_complete_leaves_old_object_or_full_new_object(self):
        self.store.put("k", b"old")
        session_id = self.begin("k", b"new-content")
        self.store.append_upload(session_id, 0, b"new-content")
        real_atomic_write = store_module._atomic_write

        def boom(path, data):
            if os.path.basename(path) == "index.json":
                raise OSError("simulated crash")
            return real_atomic_write(path, data)

        store_module._atomic_write = boom
        try:
            with self.assertRaises(ObjectStoreError):
                self.store.complete_upload(session_id)
        finally:
            store_module._atomic_write = real_atomic_write
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("k")[0], b"old")
        self.assertEqual(reopened.upload_status(session_id)["completed"], False)
        # the session can be completed after recovery
        entry = reopened.complete_upload(session_id)
        self.assertEqual(ContentAddressedStore(self.root).get("k"),
                         (b"new-content", entry))
        self.assertEqual(ContentAddressedStore(self.root)
                         .upload_status(session_id)["completed"], True)

    def test_no_session_files_left_after_complete(self):
        session_id = self.begin("k", b"ab")
        self.store.append_upload(session_id, 0, b"ab")
        self.store.complete_upload(session_id)
        self.assertFalse(os.path.exists(self.store.uploads_dir))
        self.assertEqual(sorted(os.listdir(self.root)),
                         ["audit.log", "blobs", "index.json"])

    def test_corrupted_session_files_are_reported(self):
        session_id = self.begin()
        self.store.append_upload(session_id, 0, b"hello")
        with open(self.store._session_meta_path(session_id), "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)


class TestUploadsAndGarbageCollection(UploadTestCase):
    def test_gc_keeps_incomplete_session_data(self):
        session_id = self.begin("k", b"session-bytes")
        self.store.append_upload(session_id, 0, b"session-")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [], "bytes": 0, "dry_run": False})
        self.assertEqual(self.store.upload_status(session_id)["offset"], 8)
        self.store.append_upload(session_id, 8, b"bytes")
        self.assertEqual(self.store.complete_upload(session_id)["sha256"],
                         sha(b"session-bytes"))

    def test_gc_collects_completed_upload_blob_once_unreferenced(self):
        session_id = self.begin("k", b"collect-me")
        self.store.append_upload(session_id, 0, b"collect-me")
        entry = self.store.complete_upload(session_id)
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"], [])
        self.store.delete("k")
        # the completion record does not keep the blob alive
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [entry["sha256"]],
                                  "bytes": len(b"collect-me"), "dry_run": False})

    def test_gc_after_abort_has_nothing_extra(self):
        session_id = self.begin("k", b"aborted")
        self.store.append_upload(session_id, 0, b"abor")
        self.store.abort_upload(session_id)
        self.assertEqual(self.store.collect_garbage(dry_run=False),
                         {"digests": [], "bytes": 0, "dry_run": False})


class TestUploadConcurrency(UploadTestCase):
    def test_same_key_sessions_publish_in_completion_order(self):
        first = self.begin("k", b"from-first")
        second = self.begin("k", b"from-second!")
        self.store.append_upload(first, 0, b"from-first")
        self.store.append_upload(second, 0, b"from-second!")
        self.store.complete_upload(first)
        self.assertEqual(self.store.get("k")[0], b"from-first")
        self.store.complete_upload(second)
        self.assertEqual(self.store.get("k")[0], b"from-second!")

    def test_concurrent_sessions_and_reads_stay_consistent(self):
        errors = []

        def uploader(worker):
            try:
                for index in range(10):
                    data = ("w%d-%d" % (worker, index)).encode("utf-8") * 3
                    session_id = self.store.begin_upload("w%d/k%d" % (worker, index),
                                                         len(data), sha(data))
                    third = len(data) // 3
                    self.store.append_upload(session_id, 0, data[:third])
                    self.store.append_upload(session_id, third, data[third:2 * third])
                    self.store.append_upload(session_id, 2 * third, data[2 * third:])
                    self.store.complete_upload(session_id)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=uploader, args=(w,)) for w in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        for worker in range(4):
            for index in range(10):
                data = ("w%d-%d" % (worker, index)).encode("utf-8") * 3
                self.assertEqual(
                    self.store.get("w%d/k%d" % (worker, index))[0], data)


if __name__ == "__main__":
    unittest.main()

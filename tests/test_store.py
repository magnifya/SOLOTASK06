"""Unit tests for objstore.store (run from the repository root)."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.store import MAX_LIMIT, ContentAddressedStore, ObjectStoreError


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-store-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)


class TestPutGetHead(StoreTestCase):
    def test_roundtrip(self):
        entry = self.store.put("a.txt", b"hello", content_type="text/plain")
        self.assertEqual(entry, {"sha256": sha(b"hello"), "size": 5, "content_type": "text/plain"})
        self.assertEqual(sorted(entry), ["content_type", "sha256", "size"])
        self.assertEqual(self.store.get("a.txt"), (b"hello", entry))
        self.assertEqual(self.store.head("a.txt"), entry)

    def test_content_type_defaults_to_none(self):
        self.store.put("a.txt", b"hello")
        self.assertIsNone(self.store.head("a.txt")["content_type"])

    def test_head_returns_a_copy(self):
        self.store.put("a.txt", b"hello")
        head = self.store.head("a.txt")
        head["size"] = 999
        self.assertEqual(self.store.head("a.txt")["size"], 5)

    def test_payload_kinds(self):
        binary = bytes(range(256)) * 2
        self.assertEqual(self.store.put("bin", binary)["size"], 512)
        self.assertEqual(self.store.get("bin")[0], binary)
        self.assertEqual(self.store.put("empty", b"")["size"], 0)
        self.assertEqual(self.store.get("empty")[0], b"")
        self.assertEqual(self.store.put("ba", bytearray(b"abc"))["sha256"], sha(b"abc"))
        self.assertEqual(self.store.put("mv", memoryview(b"abc"))["sha256"], sha(b"abc"))

    def test_put_rejects_bad_arguments(self):
        with self.assertRaises(ObjectStoreError):
            self.store.put("a.txt", "not bytes")
        with self.assertRaises(ObjectStoreError):
            self.store.put("a.txt", b"x", content_type=7)


class TestDedup(StoreTestCase):
    def test_identical_bytes_share_one_blob(self):
        first = self.store.put("a.txt", b"same")
        second = self.store.put("nested/b.txt", b"same")
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(self.store.blob_digests(), [sha(b"same")])

    def test_reput_identical_bytes_returns_same_entry(self):
        self.assertEqual(self.store.put("a.txt", b"hello"), self.store.put("a.txt", b"hello"))

    def test_reput_new_bytes_replaces_entry(self):
        self.store.put("a.txt", b"hello")
        entry = self.store.put("a.txt", b"hello!")
        self.assertEqual(self.store.get("a.txt")[0], b"hello!")
        self.assertEqual(self.store.head("a.txt"), entry)
        self.assertEqual(len(self.store.blob_digests()), 2)

    def test_delete_keeps_blob_shared_by_other_keys(self):
        entry = self.store.put("a.txt", b"shared")
        self.store.put("b.txt", b"shared")
        self.store.delete("a.txt")
        self.assertEqual(self.store.blob(entry["sha256"]), b"shared")
        self.assertEqual(self.store.get("b.txt")[0], b"shared")


class TestBlobAccess(StoreTestCase):
    def test_fetch_by_digest(self):
        entry = self.store.put("a.txt", b"content")
        self.assertEqual(self.store.blob(entry["sha256"]), b"content")

    def test_invalid_digest_is_rejected(self):
        for bad in ["", "xyz", "A" * 64, "0" * 63, "0" * 65, None, 12]:
            with self.assertRaises(ObjectStoreError):
                self.store.blob(bad)

    def test_unknown_digest_is_not_found(self):
        with self.assertRaises(ObjectStoreError):
            self.store.blob("f" * 64)


class TestMissingAndInvalidKeys(StoreTestCase):
    def test_missing_key_operations_fail(self):
        for call in (self.store.get, self.store.head, self.store.delete):
            with self.assertRaises(ObjectStoreError):
                call("nope")

    def test_invalid_keys_are_rejected(self):
        for bad in ["", "/leading", "trailing/", "a//b", "a/./b", "a/../b", "a\x00b", None, 5]:
            with self.assertRaises(ObjectStoreError):
                self.store.get(bad)
        with self.assertRaises(ObjectStoreError):
            self.store.put("../escape", b"x")


class TestPersistence(StoreTestCase):
    def test_restart_sees_same_objects(self):
        self.store.put("docs/a.txt", b"alpha", content_type="text/plain")
        self.store.put("docs/b.txt", b"beta")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("docs/a.txt"), (b"alpha", self.store.head("docs/a.txt")))
        self.assertEqual(reopened.list_objects()["items"][0]["key"], "docs/a.txt")

    def test_delete_survives_restart(self):
        self.store.put("a.txt", b"x")
        self.store.delete("a.txt")
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root).head("a.txt")

    def test_index_file_is_deterministic_json(self):
        self.store.put("a.txt", b"x")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(sorted(document), ["objects", "version"])
        self.assertEqual(sorted(document["objects"]), ["a.txt"])
        self.assertEqual(document["objects"]["a.txt"]["sha256"], sha(b"x"))

    def test_corrupted_index_documents_are_reported(self):
        path = os.path.join(self.root, "index.json")
        broken = [
            b"{not json",
            json.dumps({"version": 1}).encode("utf-8"),
            json.dumps({"version": 1, "objects": []}).encode("utf-8"),
            json.dumps({"version": 1, "objects": {"a": {"sha256": "nope", "size": 1}}}).encode("utf-8"),
            json.dumps({"version": 1, "objects": {"a": {"sha256": "0" * 64, "size": -1}}}).encode("utf-8"),
            json.dumps({"version": 1, "objects": {"a": {"sha256": "0" * 64, "size": 1, "content_type": 5}}}).encode("utf-8"),
            json.dumps({"version": 1, "objects": {"a": "not-an-object"}}).encode("utf-8"),
        ]
        for payload in broken:
            with open(path, "wb") as handle:
                handle.write(payload)
            with self.assertRaises(ObjectStoreError):
                ContentAddressedStore(self.root)

    def test_no_temporary_files_left_behind(self):
        for index in range(5):
            self.store.put("k%d" % index, b"payload-%d" % index)
        self.assertEqual(sorted(os.listdir(self.root)), ["blobs", "index.json"])
        self.assertEqual(len(self.store.blob_digests()), 5)
        self.assertTrue(all(len(name) == 64 for name in os.listdir(self.store.blobs_dir)))


class TestListing(StoreTestCase):
    def seed(self):
        self.store.put("docs/b.txt", b"b")
        self.store.put("docs/a.txt", b"a", content_type="text/plain")
        self.store.put("img/c.png", b"c")

    def test_sorted_items_and_shape(self):
        self.seed()
        page = self.store.list_objects()
        self.assertEqual([item["key"] for item in page["items"]],
                         ["docs/a.txt", "docs/b.txt", "img/c.png"])
        self.assertIsNone(page["next_after"])
        self.assertEqual(sorted(page["items"][0]),
                         ["content_type", "key", "sha256", "size"])
        self.assertEqual(page["items"][0]["sha256"], sha(b"a"))

    def test_prefix_filter(self):
        self.seed()
        self.assertEqual([item["key"] for item in self.store.list_objects(prefix="docs/")["items"]],
                         ["docs/a.txt", "docs/b.txt"])
        self.assertEqual(self.store.list_objects(prefix="zzz")["items"], [])

    def test_pagination(self):
        self.seed()
        first = self.store.list_objects(limit=2)
        self.assertEqual([item["key"] for item in first["items"]], ["docs/a.txt", "docs/b.txt"])
        self.assertEqual(first["next_after"], "docs/b.txt")
        second = self.store.list_objects(after=first["next_after"], limit=2)
        self.assertEqual([item["key"] for item in second["items"]], ["img/c.png"])
        self.assertIsNone(second["next_after"])

    def test_empty_store(self):
        self.assertEqual(self.store.list_objects(), {"items": [], "next_after": None})

    def test_invalid_arguments(self):
        for bad in [0, -1, MAX_LIMIT + 1, "10", None, True, 1.5]:
            with self.assertRaises(ObjectStoreError):
                self.store.list_objects(limit=bad)
        with self.assertRaises(ObjectStoreError):
            self.store.list_objects(prefix=5)
        with self.assertRaises(ObjectStoreError):
            self.store.list_objects(after=5)


class TestConcurrency(StoreTestCase):
    def test_concurrent_puts_are_all_recorded(self):
        workers, per_worker, errors = 8, 15, []

        def work(worker):
            try:
                for index in range(per_worker):
                    self.store.put("w%d/k%02d" % (worker, index), b"v%d-%d" % (worker, index))
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=work, args=(worker,)) for worker in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [])
        self.assertEqual(len(self.store.list_objects(limit=MAX_LIMIT)["items"]), workers * per_worker)
        self.assertEqual(len(self.store.blob_digests()), workers * per_worker)
        self.assertEqual(
            len(ContentAddressedStore(self.root).list_objects(limit=MAX_LIMIT)["items"]),
            workers * per_worker,
        )

    def test_reads_stay_consistent_during_writes(self):
        self.store.put("stable", b"stable")
        stop, errors = threading.Event(), []

        def reader():
            try:
                while not stop.is_set():
                    self.assertEqual(self.store.get("stable")[0], b"stable")
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for index in range(40):
                self.store.put("churn/%02d" % index, b"c%d" % index)
        finally:
            stop.set()
            thread.join(timeout=30)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()

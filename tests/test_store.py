"""Unit tests for objstore.store (run from the repository root)."""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest

from objstore.store import MAX_LIMIT, ContentAddressedStore, ObjectStoreError


def sha256_of(payload):
    return hashlib.sha256(payload).hexdigest()


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-store-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)


class TestPutGetHead(StoreTestCase):
    def test_roundtrip_returns_payload_and_entry(self):
        entry = self.store.put("a.txt", b"hello", content_type="text/plain")
        self.assertEqual(entry["sha256"], sha256_of(b"hello"))
        self.assertEqual(entry["size"], 5)
        self.assertEqual(entry["content_type"], "text/plain")
        payload, head = self.store.get("a.txt")
        self.assertEqual(payload, b"hello")
        self.assertEqual(head, entry)

    def test_head_returns_metadata_only(self):
        self.store.put("a.txt", b"hello")
        head = self.store.head("a.txt")
        self.assertEqual(sorted(head), ["content_type", "sha256", "size"])
        self.assertIsNone(head["content_type"])

    def test_head_returns_a_copy(self):
        self.store.put("a.txt", b"hello")
        head = self.store.head("a.txt")
        head["size"] = 999
        self.assertEqual(self.store.head("a.txt")["size"], 5)

    def test_empty_payload_is_allowed(self):
        entry = self.store.put("empty.bin", b"")
        self.assertEqual(entry["size"], 0)
        self.assertEqual(self.store.get("empty.bin")[0], b"")

    def test_binary_payload_roundtrips(self):
        payload = bytes(range(256)) * 3
        self.store.put("blob.bin", payload)
        self.assertEqual(self.store.get("blob.bin")[0], payload)

    def test_put_accepts_bytearray(self):
        entry = self.store.put("ba.bin", bytearray(b"abc"))
        self.assertEqual(entry["sha256"], sha256_of(b"abc"))

    def test_put_rejects_non_bytes(self):
        with self.assertRaises(ObjectStoreError):
            self.store.put("a.txt", "not bytes")

    def test_put_rejects_non_string_content_type(self):
        with self.assertRaises(ObjectStoreError):
            self.store.put("a.txt", b"x", content_type=7)


class TestDedup(StoreTestCase):
    def test_identical_bytes_share_one_blob(self):
        first = self.store.put("a.txt", b"same")
        second = self.store.put("nested/b.txt", b"same")
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(self.store.blob_digests(), [sha256_of(b"same")])

    def test_reput_identical_bytes_returns_same_sha(self):
        first = self.store.put("a.txt", b"hello")
        second = self.store.put("a.txt", b"hello")
        self.assertEqual(first, second)

    def test_reput_different_bytes_changes_sha(self):
        first = self.store.put("a.txt", b"hello")
        second = self.store.put("a.txt", b"hello!")
        self.assertNotEqual(first["sha256"], second["sha256"])
        self.assertEqual(self.store.get("a.txt")[0], b"hello!")
        self.assertEqual(len(self.store.blob_digests()), 2)

    def test_delete_keeps_shared_blob(self):
        entry = self.store.put("a.txt", b"shared")
        self.store.put("b.txt", b"shared")
        self.store.delete("a.txt")
        self.assertEqual(self.store.blob(entry["sha256"]), b"shared")
        self.assertEqual(self.store.get("b.txt")[0], b"shared")


class TestBlobAccess(StoreTestCase):
    def test_blob_fetch_by_digest(self):
        entry = self.store.put("a.txt", b"content")
        self.assertEqual(self.store.blob(entry["sha256"]), b"content")
        self.assertTrue(self.store.has_blob(entry["sha256"]))

    def test_invalid_sha_is_rejected(self):
        for bad in ["", "xyz", "A" * 64, "0" * 63, "0" * 65, None, 12]:
            with self.assertRaises(ObjectStoreError):
                self.store.blob(bad)

    def test_unknown_sha_reports_not_found(self):
        with self.assertRaises(ObjectStoreError):
            self.store.blob("f" * 64)


class TestMissingAndInvalid(StoreTestCase):
    def test_get_missing_key(self):
        with self.assertRaises(ObjectStoreError):
            self.store.get("nope")

    def test_head_missing_key(self):
        with self.assertRaises(ObjectStoreError):
            self.store.head("nope")

    def test_delete_missing_key(self):
        with self.assertRaises(ObjectStoreError):
            self.store.delete("nope")

    def test_get_rejects_invalid_key(self):
        for bad in ["", "/leading", "trailing/", "a//b", "a/./b", "a/../b", None, 5]:
            with self.assertRaises(ObjectStoreError):
                self.store.get(bad)

    def test_put_rejects_invalid_key(self):
        with self.assertRaises(ObjectStoreError):
            self.store.put("../escape", b"x")


class TestIndexPersistence(StoreTestCase):
    def test_restart_sees_same_objects(self):
        self.store.put("docs/a.txt", b"alpha", content_type="text/plain")
        self.store.put("docs/b.txt", b"beta")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("docs/a.txt")[0], b"alpha")
        self.assertEqual(reopened.head("docs/a.txt")["content_type"], "text/plain")
        self.assertEqual(reopened.list_objects()["items"][0]["key"], "docs/a.txt")

    def test_deletes_survive_restart(self):
        self.store.put("a.txt", b"x")
        self.store.delete("a.txt")
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root).head("a.txt")

    def test_index_file_is_valid_json(self):
        self.store.put("a.txt", b"x")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(sorted(document["objects"]), ["a.txt"])

    def test_corrupted_index_json_is_reported(self):
        with open(os.path.join(self.root, "index.json"), "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)

    def test_corrupted_index_entry_is_reported(self):
        document = {"version": 1, "objects": {"a.txt": {"sha256": "nope", "size": 1}}}
        with open(os.path.join(self.root, "index.json"), "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)

    def test_missing_objects_map_is_reported(self):
        with open(os.path.join(self.root, "index.json"), "w", encoding="utf-8") as handle:
            json.dump({"version": 1}, handle)
        with self.assertRaises(ObjectStoreError):
            ContentAddressedStore(self.root)

    def test_no_temporary_files_left_behind(self):
        for index in range(5):
            self.store.put("k%d" % index, b"payload-%d" % index)
        leftovers = [
            name
            for name in os.listdir(self.root)
            if name != "index.json" and name != "blobs"
        ]
        self.assertEqual(leftovers, [])
        for name in os.listdir(self.store.blobs_dir):
            self.assertEqual(len(name), 64)


class TestListing(StoreTestCase):
    def seed(self):
        self.store.put("docs/b.txt", b"b")
        self.store.put("docs/a.txt", b"a")
        self.store.put("img/c.png", b"c")

    def test_sorted_and_complete(self):
        self.seed()
        page = self.store.list_objects()
        self.assertEqual([item["key"] for item in page["items"]], ["docs/a.txt", "docs/b.txt", "img/c.png"])
        self.assertIsNone(page["next_after"])

    def test_item_shape(self):
        self.store.put("docs/a.txt", b"a", content_type="text/plain")
        item = self.store.list_objects()["items"][0]
        self.assertEqual(sorted(item), ["content_type", "key", "sha256", "size"])
        self.assertEqual(item["sha256"], sha256_of(b"a"))

    def test_prefix_filter(self):
        self.seed()
        page = self.store.list_objects(prefix="docs/")
        self.assertEqual([item["key"] for item in page["items"]], ["docs/a.txt", "docs/b.txt"])
        self.assertEqual(self.store.list_objects(prefix="zzz")["items"], [])

    def test_pagination_with_limit_and_after(self):
        self.seed()
        first = self.store.list_objects(limit=2)
        self.assertEqual([item["key"] for item in first["items"]], ["docs/a.txt", "docs/b.txt"])
        self.assertEqual(first["next_after"], "docs/b.txt")
        second = self.store.list_objects(after=first["next_after"], limit=2)
        self.assertEqual([item["key"] for item in second["items"]], ["img/c.png"])
        self.assertIsNone(second["next_after"])

    def test_empty_store_listing(self):
        self.assertEqual(self.store.list_objects(), {"items": [], "next_after": None})

    def test_invalid_limits(self):
        for bad in [0, -1, MAX_LIMIT + 1, "10", None, True, 1.5]:
            with self.assertRaises(ObjectStoreError):
                self.store.list_objects(limit=bad)

    def test_invalid_prefix_and_after(self):
        with self.assertRaises(ObjectStoreError):
            self.store.list_objects(prefix=5)
        with self.assertRaises(ObjectStoreError):
            self.store.list_objects(after=5)


class TestConcurrency(StoreTestCase):
    def test_concurrent_puts_are_all_recorded(self):
        workers = 8
        per_worker = 15
        errors = []

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
        page = self.store.list_objects(limit=MAX_LIMIT)
        self.assertEqual(len(page["items"]), workers * per_worker)
        self.assertEqual(len(self.store.blob_digests()), workers * per_worker)
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(len(reopened.list_objects(limit=MAX_LIMIT)["items"]), workers * per_worker)

    def test_reads_are_consistent_during_writes(self):
        self.store.put("stable", b"stable")
        stop = threading.Event()
        errors = []

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

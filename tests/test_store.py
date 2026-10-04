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


class TestBlobIntegrity(StoreTestCase):
    def test_tampering_shapes_are_rejected(self):
        payload = b"integrity-check-payload"
        digest = self.store.put("k", payload)["sha256"]
        mutations = {
            "truncated": payload[:-3],
            "appended": payload + b"extra",
            "same-length": payload[:-1] + bytes([payload[-1] ^ 0xFF]),
        }
        for label, broken in mutations.items():
            with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
                handle.write(broken)
            with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
                self.store.blob(digest)
            with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
                self.store.get("k")
            # failed reads neither remove the file nor change metadata
            self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, digest)))
            self.assertEqual(self.store.head("k")["size"], len(payload))
            with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
                handle.write(payload)
            self.assertEqual(self.store.blob(digest), payload)

    def test_every_read_rechecks_disk(self):
        digest = self.store.put("k", b"verified-now-corrupt-later")["sha256"]
        self.assertEqual(self.store.blob(digest), b"verified-now-corrupt-later")
        with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
            handle.write(b"verified-now-corrupt-later!!")
        with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
            self.store.blob(digest)

    def test_tampered_empty_blob_is_rejected(self):
        digest = self.store.put("empty", b"")["sha256"]
        with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
            handle.write(b"x")
        with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
            self.store.blob(digest)
        with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
            self.store.get("empty")

    def test_wrong_metadata_size_fails_get_but_not_blob(self):
        payload = b"size-mismatch-case"
        digest = self.store.put("m", payload)["sha256"]
        index_path = os.path.join(self.root, "index.json")
        with open(index_path, "rb") as handle:
            document = json.loads(handle.read().decode("utf-8"))
        document["objects"]["m"]["size"] = len(payload) + 2
        with open(index_path, "wb") as handle:
            handle.write(json.dumps(document).encode("utf-8"))
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.blob(digest), payload)
        with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
            reopened.get("m")
        # metadata-only operations are unaffected
        self.assertEqual(reopened.head("m")["sha256"], digest)

    def test_unreadable_existing_blob_is_io_error(self):
        digest = self.store.put("io", b"io-test")["sha256"]
        os.chmod(os.path.join(self.store.blobs_dir, digest), 0)
        try:
            if os.geteuid() == 0:
                self.skipTest("root bypasses file permissions")
            with self.assertRaisesRegex(ObjectStoreError, r"\Ablob io error\Z"):
                self.store.blob(digest)
        finally:
            os.chmod(os.path.join(self.store.blobs_dir, digest), 0o644)


class TestRepairOnPut(StoreTestCase):
    def test_reput_repairs_corrupted_shared_blob(self):
        payload = b"shared-content-for-repair"
        digest = self.store.put("a", payload)["sha256"]
        self.store.put("b", payload)
        self.store.put("c", payload)
        before_b, before_c = self.store.head("b"), self.store.head("c")
        with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
            handle.write(payload[:5])
        with self.assertRaises(ObjectStoreError):
            self.store.get("b")
        entry = self.store.put("a", payload, content_type="text/plain")
        self.assertEqual(entry,
                         {"sha256": digest, "size": len(payload),
                          "content_type": "text/plain"})
        self.assertEqual(self.store.blob(digest), payload)
        # sibling metadata is untouched and the keys become readable again
        self.assertEqual(self.store.head("b"), before_b)
        self.assertEqual(self.store.head("c"), before_c)
        self.assertEqual(self.store.get("b"), (payload, before_b))
        self.assertEqual(self.store.get("c"), (payload, before_c))
        self.assertEqual(self.store.blob_digests(), [digest])
        # the repair itself survives reopening the data directory
        self.assertEqual(ContentAddressedStore(self.root).get("b"), (payload, before_b))

    def test_intact_shared_blob_is_not_rewritten(self):
        payload = b"leave-intact-shared"
        digest = self.store.put("a", payload)["sha256"]
        path = os.path.join(self.store.blobs_dir, digest)
        before = os.stat(path)
        self.store.put("b", payload)
        self.store.put("a", payload)
        after = os.stat(path)
        self.assertEqual((after.st_ino, after.st_mtime_ns),
                         (before.st_ino, before.st_mtime_ns))
        self.assertEqual(self.store.blob_digests(), [digest])

    def test_failed_repair_write_leaves_mapping_and_bytes_unchanged(self):
        import objstore.store as store_module
        payload = b"repair-io-victim"
        digest = self.store.put("v", payload)["sha256"]
        with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
            handle.write(b"bad")
        entry_before = self.store.head("v")
        real_atomic_write = store_module._atomic_write

        def boom(path, data):
            if os.path.basename(path) == digest:
                raise OSError("simulated disk failure")
            return real_atomic_write(path, data)

        store_module._atomic_write = boom
        try:
            with self.assertRaisesRegex(ObjectStoreError, r"\Ablob io error\Z"):
                self.store.put("v", payload)
        finally:
            store_module._atomic_write = real_atomic_write
        self.assertEqual(self.store.head("v"), entry_before)
        with open(os.path.join(self.store.blobs_dir, digest), "rb") as handle:
            self.assertEqual(handle.read(), b"bad")
        # a retry with healthy I/O repairs cleanly
        self.store.put("v", payload)
        self.assertEqual(self.store.get("v")[0], payload)

    def test_leftover_temp_file_does_not_block_reopen(self):
        payload = b"interrupted-repair"
        digest = self.store.put("v", payload)["sha256"]
        leftover = os.path.join(self.store.blobs_dir, "%s.tmp.999.999" % digest)
        with open(leftover, "wb") as handle:
            handle.write(b"partial-new-content")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("v")[0], payload)
        self.assertEqual(reopened.blob_digests(), [digest])


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


class TestGarbageCollection(StoreTestCase):
    def raw_blob(self, digest, data):
        with open(os.path.join(self.store.blobs_dir, digest), "wb") as handle:
            handle.write(data)

    def test_preview_reports_candidates_without_touching_storage(self):
        old = self.store.put("k", b"v1")["sha256"]
        self.store.put("k", b"v2-overwritten")
        gone = self.store.put("g", b"gone-soon")["sha256"]
        self.store.delete("g")
        result = self.store.collect_garbage()
        self.assertEqual(result, {
            "digests": sorted([old, gone]),
            "bytes": len(b"v1") + len(b"gone-soon"),
            "dry_run": True,
        })
        # a preview neither deletes blobs nor changes metadata
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, old)))
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, gone)))
        self.assertEqual(self.store.head("k")["sha256"], sha(b"v2-overwritten"))
        # the identical preview is reproducible
        self.assertEqual(self.store.collect_garbage(), result)

    def test_execute_deletes_only_unreferenced_blobs(self):
        keep = self.store.put("keep", b"still-here")["sha256"]
        old = self.store.put("k", b"v1")["sha256"]
        self.store.put("k", b"v2")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [old], "bytes": 2, "dry_run": False})
        self.assertFalse(os.path.exists(os.path.join(self.store.blobs_dir, old)))
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, keep)))
        self.assertEqual(self.store.get("keep"),
                         (b"still-here", self.store.head("keep")))
        # a second run has nothing left to collect
        self.assertEqual(self.store.collect_garbage(dry_run=False),
                         {"digests": [], "bytes": 0, "dry_run": False})

    def test_empty_store_has_no_candidates(self):
        fresh = ContentAddressedStore(tempfile.mkdtemp(prefix="objstore-gc-empty-"))
        self.assertEqual(fresh.collect_garbage(),
                         {"digests": [], "bytes": 0, "dry_run": True})
        self.assertEqual(fresh.collect_garbage(dry_run=False),
                         {"digests": [], "bytes": 0, "dry_run": False})

    def test_shared_content_is_preserved_until_the_last_reference_is_gone(self):
        entry = self.store.put("a", b"shared")
        self.store.put("b", b"shared")
        self.store.put("c", b"shared")
        self.store.delete("a")
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"], [])
        self.store.delete("b")
        self.assertEqual(self.store.collect_garbage(dry_run=False)["digests"], [])
        self.assertEqual(self.store.blob(entry["sha256"]), b"shared")
        self.store.delete("c")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [entry["sha256"]], "bytes": 6,
                                  "dry_run": False})
        self.assertFalse(os.path.exists(
            os.path.join(self.store.blobs_dir, entry["sha256"])))

    def test_unreferenced_blobs_are_collected_without_content_validation(self):
        complete = sha(b"never-indexed")
        self.raw_blob(complete, b"never-indexed")
        wrong_bytes = b"totally different bytes on disk"
        wrong_content = sha(b"expected")
        self.raw_blob(wrong_content, wrong_bytes)
        empty = sha(b"")
        self.raw_blob(empty, b"")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result["digests"], sorted([complete, empty, wrong_content]))
        self.assertEqual(result["bytes"], len(b"never-indexed") + len(wrong_bytes))
        for digest in (complete, empty, wrong_content):
            self.assertFalse(os.path.exists(
                os.path.join(self.store.blobs_dir, digest)))

    def test_candidates_are_sorted_and_sized_by_actual_file_bytes(self):
        names = ["f" * 64, "0" * 64, "a" * 64, "9" * 64]
        sizes = {}
        for index, name in enumerate(names):
            data = b"x" * (index + 1)
            sizes[name] = len(data)
            self.raw_blob(name, data)
        result = self.store.collect_garbage()
        self.assertEqual(result["digests"], sorted(names))
        self.assertEqual(result["bytes"], sum(sizes.values()))
        self.assertTrue(result["dry_run"])

    def test_non_canonical_names_subdirs_and_symlinks_are_never_touched(self):
        outside = os.path.join(self.root, "link-target")
        with open(outside, "wb") as handle:
            handle.write(b"link target bytes")
        link = os.path.join(self.store.blobs_dir, "e" * 64)
        os.symlink(outside, link)
        dangling = os.path.join(self.store.blobs_dir, "d" * 64)
        os.symlink(os.path.join(self.root, "missing-target"), dangling)
        directory = os.path.join(self.store.blobs_dir, "c" * 64)
        os.makedirs(os.path.join(directory, "nested"))
        with open(os.path.join(directory, "nested", "f.bin"), "wb") as handle:
            handle.write(b"inside a subdirectory")
        for name in ("tmp-file", "%s.tmp.123.456" % ("b" * 64), "B" * 64,
                     "a" * 63, "a" * 65, "readme"):
            self.raw_blob(name, b"preserved")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [], "bytes": 0, "dry_run": False})
        kept = sorted(os.listdir(self.store.blobs_dir))
        self.assertEqual(kept, sorted(
            ["tmp-file", "%s.tmp.123.456" % ("b" * 64), "B" * 64,
             "a" * 63, "a" * 65, "readme", "e" * 64, "d" * 64, "c" * 64]))
        # symlink targets (including contents behind the link) stay intact
        self.assertTrue(os.path.islink(link))
        self.assertTrue(os.path.islink(dangling))
        with open(outside, "rb") as handle:
            self.assertEqual(handle.read(), b"link target bytes")
        with open(os.path.join(directory, "nested", "f.bin"), "rb") as handle:
            self.assertEqual(handle.read(), b"inside a subdirectory")

    def test_referenced_missing_or_corrupted_blob_does_not_block_other_candidates(self):
        referenced = self.store.put("k", b"referred-bytes")
        missing = self.store.put("m", b"vanished")["sha256"]
        os.remove(os.path.join(self.store.blobs_dir, missing))
        with open(os.path.join(self.store.blobs_dir, referenced["sha256"]),
                  "wb") as handle:
            handle.write(b"corruption")
        candidate = sha(b"orphan")
        self.raw_blob(candidate, b"orphan")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [candidate], "bytes": 6,
                                  "dry_run": False})
        self.assertFalse(os.path.exists(
            os.path.join(self.store.blobs_dir, candidate)))
        # the corrupted, still-referenced blob and metadata are untouched
        with open(os.path.join(self.store.blobs_dir, referenced["sha256"]),
                  "rb") as handle:
            self.assertEqual(handle.read(), b"corruption")
        with self.assertRaisesRegex(ObjectStoreError, r"\Acorrupted blob\Z"):
            self.store.blob(referenced["sha256"])
        self.assertEqual(self.store.head("k")["sha256"], referenced["sha256"])

    def test_invalid_dry_run_is_rejected_and_changes_nothing(self):
        candidate = sha(b"orphan")
        self.raw_blob(candidate, b"orphan")
        referenced = self.store.put("k", b"kept")["sha256"]
        for bad in (1, 0, "true", None, [], 1.0):
            with self.assertRaisesRegex(ObjectStoreError, r"\Ainvalid dry_run\Z"):
                self.store.collect_garbage(dry_run=bad)
        self.assertTrue(os.path.exists(
            os.path.join(self.store.blobs_dir, candidate)))
        self.assertTrue(os.path.exists(
            os.path.join(self.store.blobs_dir, referenced)))

    def test_scan_failure_deletes_nothing(self):
        first = self.store.put("k", b"v1")["sha256"]
        self.store.put("k", b"v2")
        try:
            os.chmod(self.store.blobs_dir, 0)
            if os.geteuid() == 0:
                self.skipTest("root bypasses directory permissions")
            with self.assertRaisesRegex(ObjectStoreError, r"\Agc io error\Z"):
                self.store.collect_garbage()
            with self.assertRaisesRegex(ObjectStoreError, r"\Agc io error\Z"):
                self.store.collect_garbage(dry_run=False)
        finally:
            os.chmod(self.store.blobs_dir, 0o755)
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, first)))

    def test_file_vanishing_during_scan_or_delete_is_skipped(self):
        import objstore.store as store_module
        a, b = "0" * 64, "1" * 64
        self.raw_blob(a, b"aa"), self.raw_blob(b, b"bb")
        real_lstat = store_module.os.lstat

        def missing_lstat(path, *pos, **kw):
            if os.path.basename(path) == a:
                raise FileNotFoundError(path)
            return real_lstat(path, *pos, **kw)

        store_module.os.lstat = missing_lstat
        try:
            self.assertEqual(self.store.collect_garbage(),
                             {"digests": [b], "bytes": 2, "dry_run": True})
        finally:
            store_module.os.lstat = real_lstat

        real_remove = store_module.os.remove

        def removed_elsewhere(path, *pos, **kw):
            if os.path.basename(path) == a:
                real_remove(path)
                raise FileNotFoundError(path)
            return real_remove(path, *pos, **kw)

        store_module.os.remove = removed_elsewhere
        try:
            result = self.store.collect_garbage(dry_run=False)
        finally:
            store_module.os.remove = real_remove
        self.assertEqual(result, {"digests": [b], "bytes": 2, "dry_run": False})
        self.assertFalse(os.path.exists(os.path.join(self.store.blobs_dir, a)))
        self.assertFalse(os.path.exists(os.path.join(self.store.blobs_dir, b)))

    def test_delete_failure_keeps_completed_work_and_retry_finishes(self):
        import errno
        import objstore.store as store_module
        a, b = "0" * 64, "1" * 64
        self.raw_blob(a, b"aa"), self.raw_blob(b, b"bb")
        real_remove = store_module.os.remove

        def fail_on_b(path, *pos, **kw):
            if os.path.basename(path) == b:
                raise OSError(errno.EPERM, "simulated delete failure")
            return real_remove(path, *pos, **kw)

        store_module.os.remove = fail_on_b
        try:
            with self.assertRaisesRegex(ObjectStoreError, r"\Agc io error\Z"):
                self.store.collect_garbage(dry_run=False)
        finally:
            store_module.os.remove = real_remove
        self.assertFalse(os.path.exists(os.path.join(self.store.blobs_dir, a)))
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, b)))
        self.assertEqual(self.store.collect_garbage(dry_run=False),
                         {"digests": [b], "bytes": 2, "dry_run": False})

    def test_preview_then_new_reference_then_execute_keeps_content(self):
        old = self.store.put("k", b"once-orphaned")["sha256"]
        self.store.put("k", b"current")
        self.assertEqual(self.store.collect_garbage()["digests"], [old])
        self.store.put("k2", b"once-orphaned")
        result = self.store.collect_garbage(dry_run=False)
        self.assertEqual(result, {"digests": [], "bytes": 0, "dry_run": False})
        self.assertEqual(self.store.get("k2")[0], b"once-orphaned")
        self.assertTrue(os.path.exists(os.path.join(self.store.blobs_dir, old)))

    def test_concurrent_gc_and_puts_keep_every_referenced_blob(self):
        self.store.put("shared", b"shared-bytes")
        for index in range(6):
            self.store.put("s%d" % index, b"shared-bytes")
        stop, errors = threading.Event(), []

        def collector():
            try:
                while not stop.is_set():
                    self.store.collect_garbage(dry_run=False)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        def writer(worker):
            for index in range(30):
                self.store.put("w%d/k%02d" % (worker, index),
                               b"v%d-%d" % (worker, index))
                if index % 4 == 0:
                    self.store.put("w%d/k%02d" % (worker, index),
                                   b"v%d-%d!" % (worker, index))

        threads = [threading.Thread(target=collector)]
        threads += [threading.Thread(target=writer, args=(w,)) for w in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads[1:]:
            thread.join(timeout=30)
        stop.set()
        threads[0].join(timeout=30)
        self.assertEqual(errors, [])
        page = self.store.list_objects(limit=MAX_LIMIT)
        self.assertIsNone(page["next_after"])
        for item in page["items"]:
            data, head = self.store.get(item["key"])
            self.assertEqual(head, {
                "sha256": item["sha256"], "size": item["size"],
                "content_type": item["content_type"],
            })
            self.assertEqual(hashlib.sha256(data).hexdigest(), item["sha256"])
        self.assertEqual(
            self.store.blob_digests(),
            sorted({item["sha256"] for item in page["items"]}),
        )


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

"""Integrity verification and self-healing tests for objstore.store and the CLI.

A blob is tampered with by rewriting ``<root>/blobs/<sha256>`` directly; reads
must then fail with ``corrupted blob`` (or ``blob io error`` for unreadable
files), and a re-put of the original bytes must atomically repair the shared
blob without disturbing other keys' metadata.
"""

import contextlib
import hashlib
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from objstore import cli
from objstore.store import (
    BLOB_IO_ERROR,
    CORRUPTED_BLOB,
    ContentAddressedStore,
    ObjectStoreError,
)


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class IntegrityTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-integrity-")
        self.addCleanup(shutil.rmtree, self.root, True)
        self.store = ContentAddressedStore(self.root)

    def blob_path(self, digest):
        return os.path.join(self.store.blobs_dir, digest)

    def overwrite_blob(self, digest, data):
        with open(self.blob_path(digest), "wb") as handle:
            handle.write(data)

    def assert_corrupted(self, callable_, *args):
        with self.assertRaises(ObjectStoreError) as caught:
            callable_(*args)
        self.assertEqual(str(caught.exception), CORRUPTED_BLOB)


class TestReadVerification(IntegrityTestCase):
    def test_intact_blob_reads(self):
        entry = self.store.put("a", b"hello")
        self.assertEqual(self.store.blob(entry["sha256"]), b"hello")

    def test_empty_and_binary_still_read(self):
        self.assertEqual(self.store.put("empty", b"") and self.store.get("empty")[0], b"")
        binary = bytes(range(256)) * 3
        self.store.put("bin", binary)
        self.assertEqual(self.store.get("bin")[0], binary)
        self.assertEqual(self.store.blob(sha(binary)), binary)

    def test_truncated_blob_is_rejected(self):
        entry = self.store.put("a", b"hello world")
        self.overwrite_blob(entry["sha256"], b"hello")
        self.assert_corrupted(self.store.blob, entry["sha256"])
        self.assert_corrupted(self.store.get, "a")

    def test_appended_blob_is_rejected(self):
        entry = self.store.put("a", b"hello")
        self.overwrite_blob(entry["sha256"], b"hello\x00extra")
        self.assert_corrupted(self.store.blob, entry["sha256"])
        self.assert_corrupted(self.store.get, "a")

    def test_same_length_tampering_is_rejected(self):
        entry = self.store.put("a", b"abcde")
        self.overwrite_blob(entry["sha256"], b"abcdf")  # one byte flipped, same size
        self.assert_corrupted(self.store.blob, entry["sha256"])
        self.assert_corrupted(self.store.get, "a")

    def test_every_read_rechecks_disk(self):
        entry = self.store.put("a", b"hello")
        self.assertEqual(self.store.get("a")[0], b"hello")
        self.assertEqual(self.store.blob(entry["sha256"]), b"hello")
        self.overwrite_blob(entry["sha256"], b"HELLO")  # same length, corrupt
        # No success caching: the corruption is visible on the next read.
        self.assert_corrupted(self.store.blob, entry["sha256"])
        self.assert_corrupted(self.store.get, "a")

    def test_metadata_size_wrong_only_fails_get_but_not_blob(self):
        entry = self.store.put("a", b"hello")
        self.store._index["objects"]["a"]["size"] = 4
        self.store._save_index()
        reopened = ContentAddressedStore(self.root)
        # Blob bytes are intact, so digest access still works...
        self.assertEqual(reopened.blob(entry["sha256"]), b"hello")
        # ...but get additionally enforces the metadata size.
        self.assert_corrupted(reopened.get, "a")
        # head stays pure metadata.
        self.assertEqual(reopened.head("a")["size"], 4)

    def test_head_listing_and_digests_skip_content_checks(self):
        entry = self.store.put("a", b"hello")
        self.overwrite_blob(entry["sha256"], b"tampered!")
        self.assertEqual(self.store.head("a")["sha256"], entry["sha256"])
        self.assertEqual(self.store.list_objects()["items"][0]["key"], "a")
        self.assertEqual(self.store.blob_digests(), [entry["sha256"]])

    def test_missing_blob_keeps_not_found_semantics(self):
        entry = self.store.put("a", b"hello")
        os.remove(self.blob_path(entry["sha256"]))
        with self.assertRaises(ObjectStoreError) as caught:
            self.store.blob(entry["sha256"])
        self.assertTrue(str(caught.exception).startswith("not found: blob "))
        with self.assertRaises(ObjectStoreError):
            self.store.get("a")

    def test_unreadable_existing_blob_is_io_error(self):
        entry = self.store.put("a", b"hello")
        os.replace(self.blob_path(entry["sha256"]), self.blob_path(entry["sha256"]) + ".keep")
        os.mkdir(self.blob_path(entry["sha256"]))  # opening a directory fails
        try:
            with self.assertRaises(ObjectStoreError) as caught:
                self.store.blob(entry["sha256"])
            self.assertEqual(str(caught.exception), BLOB_IO_ERROR)
            with self.assertRaises(ObjectStoreError) as caught:
                self.store.get("a")
            self.assertEqual(str(caught.exception), BLOB_IO_ERROR)
        finally:
            os.rmdir(self.blob_path(entry["sha256"]))
            os.replace(self.blob_path(entry["sha256"]) + ".keep", self.blob_path(entry["sha256"]))


class TestRepairOnPut(IntegrityTestCase):
    def test_reput_repairs_corrupt_blob(self):
        entry = self.store.put("a", b"shared content")
        self.overwrite_blob(entry["sha256"], b"broken")
        self.assert_corrupted(self.store.get, "a")
        repaired = self.store.put("a", b"shared content")
        self.assertEqual(repaired, entry)
        self.assertEqual(self.store.get("a"), (b"shared content", entry))
        self.assertEqual(self.store.blob(entry["sha256"]), b"shared content")

    def test_repair_restores_other_sharing_keys_and_keeps_their_metadata(self):
        first = self.store.put("a", b"shared content", content_type="text/plain")
        self.store.put("nested/b", b"shared content", content_type=None)
        # Give one sharing key a wrong metadata size; it must stay wrong.
        self.store._index["objects"]["nested/b"]["size"] = 99
        self.store._save_index()
        self.overwrite_blob(first["sha256"], b"corrupt bytes")

        # Re-putting the original content under a new key repairs the blob.
        entry_c = self.store.put("c", b"shared content")
        self.assertEqual(entry_c["sha256"], first["sha256"])

        # The correctly-sized key reads again; its metadata is untouched.
        self.assertEqual(self.store.get("a"),
                         (b"shared content",
                          {"sha256": first["sha256"], "size": 14,
                           "content_type": "text/plain"}))
        self.assertEqual(self.store.head("nested/b")["size"], 99)
        self.assert_corrupted(self.store.get, "nested/b")
        # Still deduplicated: exactly one blob file for this digest.
        self.assertEqual(self.store.blob_digests(), [first["sha256"]])

    def test_intact_shared_blob_still_deduplicated(self):
        first = self.store.put("a", b"same")
        self.store.put("b", b"same")
        inode_before = os.stat(self.blob_path(first["sha256"])).st_ino
        mtime_before = os.stat(self.blob_path(first["sha256"])).st_mtime_ns
        self.store.put("c", b"same")
        self.assertEqual(os.stat(self.blob_path(first["sha256"])).st_ino, inode_before)
        self.assertEqual(os.stat(self.blob_path(first["sha256"])).st_mtime_ns, mtime_before)
        self.assertEqual(self.store.blob_digests(), [first["sha256"]])

    def test_repair_survives_reopen(self):
        entry = self.store.put("a", b"survive me")
        self.overwrite_blob(entry["sha256"], b"old corrupt")
        self.store.put("a", b"survive me")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get("a"), (b"survive me", entry))

    def test_repair_write_failure_changes_nothing(self):
        entry = self.store.put("a", b"payload")
        self.overwrite_blob(entry["sha256"], b"corrupt!")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            index_before = handle.read()
        with mock.patch("objstore.store._atomic_write", side_effect=OSError("disk full")):
            with self.assertRaises(ObjectStoreError) as caught:
                self.store.put("a", b"payload")
        self.assertEqual(str(caught.exception), BLOB_IO_ERROR)
        # The corrupt blob and the object mapping are both left as they were.
        with open(self.blob_path(entry["sha256"]), "rb") as handle:
            self.assertEqual(handle.read(), b"corrupt!")
        with open(os.path.join(self.root, "index.json"), "rb") as handle:
            self.assertEqual(handle.read(), index_before)
        self.assertEqual(self.store.head("a"), entry)
        self.assert_corrupted(self.store.get, "a")

    def test_repair_replaces_whole_file_atomically(self):
        # A shorter corrupt payload must not leave a tail of the old bytes.
        payload = bytes(range(256)) * 4  # 1024 bytes
        entry = self.store.put("a", payload)
        self.overwrite_blob(entry["sha256"], b"x")  # drastically shorter
        self.store.put("a", payload)
        with open(self.blob_path(entry["sha256"]), "rb") as handle:
            on_disk = handle.read()
        self.assertEqual(on_disk, payload)
        self.assertEqual(len(on_disk), entry["size"])

    def test_put_against_unreadable_existing_blob_is_io_error(self):
        entry = self.store.put("a", b"hello")
        os.replace(self.blob_path(entry["sha256"]), self.blob_path(entry["sha256"]) + ".keep")
        os.mkdir(self.blob_path(entry["sha256"]))
        try:
            with self.assertRaises(ObjectStoreError) as caught:
                self.store.put("a", b"hello")
            self.assertEqual(str(caught.exception), BLOB_IO_ERROR)
        finally:
            os.rmdir(self.blob_path(entry["sha256"]))
            os.replace(self.blob_path(entry["sha256"]) + ".keep", self.blob_path(entry["sha256"]))


class TestCLIGetIntegrity(IntegrityTestCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["--data-dir", self.root, *argv])
        return code, out.getvalue(), err.getvalue()

    def test_get_corrupt_blob_fails_without_touching_outputs(self):
        entry = self.store.put("a", b"hello")
        existing = os.path.join(self.root, "existing.out")
        with open(existing, "wb") as handle:
            handle.write(b"keep me")
        missing = os.path.join(self.root, "never-created.out")

        self.overwrite_blob(entry["sha256"], b"HELLO")
        code, stdout, stderr = self.run_cli("get", "--key", "a", "--out", existing)
        self.assertEqual(code, cli.EXIT_FAILURE)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr.strip()),
                         {"ok": False, "error": CORRUPTED_BLOB})
        with open(existing, "rb") as handle:
            self.assertEqual(handle.read(), b"keep me")
        self.assertFalse(os.path.exists(missing))

        code, stdout, stderr = self.run_cli("get", "--key", "a", "--out", missing)
        self.assertEqual(code, cli.EXIT_FAILURE)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr.strip()),
                         {"ok": False, "error": CORRUPTED_BLOB})
        self.assertFalse(os.path.exists(missing))

    def test_get_after_repair_succeeds(self):
        entry = self.store.put("a", b"hello")
        self.overwrite_blob(entry["sha256"], b"HELLO")
        code, _, stderr = self.run_cli("put", "--key", "a",
                                       "--file", self._write_input(b"hello"))
        self.assertEqual(code, 0, stderr)
        out = os.path.join(self.root, "copy.out")
        code, stdout, stderr = self.run_cli("get", "--key", "a", "--out", out)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(stdout.strip(),
                         json.dumps({"ok": True, "key": "a",
                                     "sha256": entry["sha256"], "size": 5,
                                     "out": os.path.abspath(out)},
                                    sort_keys=True, separators=(",", ":")))
        with open(out, "rb") as handle:
            self.assertEqual(handle.read(), b"hello")

    def _write_input(self, data):
        path = os.path.join(self.root, "input.bin")
        with open(path, "wb") as handle:
            handle.write(data)
        return path


if __name__ == "__main__":
    unittest.main()

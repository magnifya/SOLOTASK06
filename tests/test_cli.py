"""End-to-end tests for the ``python -m objstore`` command line interface."""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


class CLITestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="objstore-cli-")
        self.data_dir = os.path.join(self.tmp, "data")
        os.makedirs(self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "objstore", "--data-dir", self.data_dir, *args],
            capture_output=True, cwd=REPO_ROOT,
        )

    def write_blob(self, digest, data):
        with open(os.path.join(self.data_dir, "blobs", digest), "wb") as handle:
            handle.write(data)


class TestGetIntegrity(CLITestCase):
    def test_corrupted_get_fails_without_touching_outputs(self):
        payload = b"cli-corrupt-payload"
        digest = sha(payload)
        source = os.path.join(self.tmp, "src.bin")
        with open(source, "wb") as handle:
            handle.write(payload)
        self.assertEqual(self.run_cli("put", "--key", "k", "--file", source).returncode, 0)
        self.write_blob(digest, payload[:-1])

        existing = os.path.join(self.tmp, "existing.bin")
        with open(existing, "wb") as handle:
            handle.write(b"previous-output")
        missing = os.path.join(self.tmp, "sub", "new.bin")

        for out in (existing, missing):
            proc = self.run_cli("get", "--key", "k", "--out", out)
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(proc.stdout, b"")
            self.assertEqual(proc.stderr.count(b"\n"), 1)
            self.assertEqual(json.loads(proc.stderr.decode("utf-8")),
                             {"ok": False, "error": "corrupted blob"})

        with open(existing, "rb") as handle:
            self.assertEqual(handle.read(), b"previous-output")
        self.assertFalse(os.path.exists(missing))

    def test_reput_repairs_and_get_succeeds_across_reopen(self):
        payload = b"cli-repair-payload"
        digest = sha(payload)
        source = os.path.join(self.tmp, "src.bin")
        with open(source, "wb") as handle:
            handle.write(payload)
        self.assertEqual(self.run_cli("put", "--key", "k", "--file", source).returncode, 0)
        self.write_blob(digest, b"garbage")

        proc = self.run_cli("put", "--key", "k", "--file", source)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        out = os.path.join(self.tmp, "restored.bin")
        proc = self.run_cli("get", "--key", "k", "--out", out)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(out, "rb") as handle:
            self.assertEqual(handle.read(), payload)


class TestGarbageCollection(CLITestCase):
    def seed_orphan(self, key="k", payload=b"old"):
        source = os.path.join(self.tmp, "src.bin")
        with open(source, "wb") as handle:
            handle.write(payload)
        self.assertEqual(self.run_cli("put", "--key", key, "--file", source).returncode, 0)
        old_digest = sha(payload)
        source2 = os.path.join(self.tmp, "src2.bin")
        with open(source2, "wb") as handle:
            handle.write(payload + b"-new")
        self.assertEqual(self.run_cli("put", "--key", key, "--file", source2).returncode, 0)
        return old_digest, len(payload)

    def test_gc_defaults_to_preview(self):
        old_digest, size = self.seed_orphan()
        proc = self.run_cli("gc")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(proc.stderr, b"")
        self.assertEqual(proc.stdout.count(b"\n"), 1)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [old_digest], "bytes": size,
                          "dry_run": True})
        self.assertTrue(os.path.exists(os.path.join(self.data_dir, "blobs", old_digest)))

    def test_gc_execute_deletes_candidates(self):
        old_digest, size = self.seed_orphan()
        proc = self.run_cli("gc", "--execute")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [old_digest], "bytes": size,
                          "dry_run": False})
        self.assertFalse(os.path.exists(os.path.join(self.data_dir, "blobs", old_digest)))

        proc = self.run_cli("gc")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [], "bytes": 0, "dry_run": True})

    def test_gc_keeps_referenced_and_non_blob_files(self):
        source = os.path.join(self.tmp, "src.bin")
        with open(source, "wb") as handle:
            handle.write(b"referenced")
        self.assertEqual(self.run_cli("put", "--key", "live", "--file", source).returncode, 0)
        with open(os.path.join(self.data_dir, "blobs", "stray.tmp.1.2"), "wb") as handle:
            handle.write(b"temp")
        proc = self.run_cli("gc", "--execute")
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8"))["digests"], [])
        self.assertTrue(os.path.exists(os.path.join(self.data_dir, "blobs", sha(b"referenced"))))
        self.assertTrue(os.path.exists(os.path.join(self.data_dir, "blobs", "stray.tmp.1.2")))


if __name__ == "__main__":
    unittest.main()

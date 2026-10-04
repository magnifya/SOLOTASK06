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
    def blob_path(self, digest):
        return os.path.join(self.data_dir, "blobs", digest)

    def test_preview_on_empty_store(self):
        proc = self.run_cli("gc")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stderr, b"")
        self.assertEqual(proc.stdout.count(b"\n"), 1)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [], "bytes": 0, "dry_run": True})

    def test_preview_then_execute(self):
        source = os.path.join(self.tmp, "a.bin")
        with open(source, "wb") as handle:
            handle.write(b"gc-cli-payload")
        self.assertEqual(self.run_cli("put", "--key", "a", "--file", source).returncode, 0)
        digest = sha(b"gc-cli-payload")
        self.assertEqual(self.run_cli("delete", "--key", "a").returncode, 0)

        proc = self.run_cli("gc")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [digest],
                          "bytes": len(b"gc-cli-payload"), "dry_run": True})
        self.assertTrue(os.path.exists(self.blob_path(digest)))

        proc = self.run_cli("gc", "--execute")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [digest],
                          "bytes": len(b"gc-cli-payload"), "dry_run": False})
        self.assertFalse(os.path.exists(self.blob_path(digest)))

        proc = self.run_cli("gc", "--execute")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8")),
                         {"ok": True, "digests": [], "bytes": 0, "dry_run": False})

    def test_execute_keeps_referenced_shared_and_non_blob_names(self):
        source = os.path.join(self.tmp, "s.bin")
        with open(source, "wb") as handle:
            handle.write(b"shared")
        self.assertEqual(self.run_cli("put", "--key", "a", "--file", source).returncode, 0)
        self.assertEqual(self.run_cli("put", "--key", "b", "--file", source).returncode, 0)
        digest = sha(b"shared")
        orphan = sha(b"orphan")
        self.write_blob(orphan, b"orphan")
        with open(os.path.join(self.data_dir, "blobs", "%s.tmp.1.2" % orphan),
                  "wb") as handle:
            handle.write(b"partial")
        with open(os.path.join(self.data_dir, "blobs", "notes.txt"), "wb") as handle:
            handle.write(b"keep me")

        proc = self.run_cli("gc", "--execute")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout.decode("utf-8"))["digests"], [orphan])
        self.assertTrue(os.path.exists(self.blob_path(digest)))
        self.assertTrue(os.path.exists(
            os.path.join(self.data_dir, "blobs", "%s.tmp.1.2" % orphan)))
        self.assertTrue(os.path.exists(
            os.path.join(self.data_dir, "blobs", "notes.txt")))
        self.assertFalse(os.path.exists(self.blob_path(orphan)))

    def test_io_failure_is_one_line_of_json_and_nonzero_exit(self):
        if os.geteuid() == 0:
            self.skipTest("root bypasses directory permissions")
        blobs = os.path.join(self.data_dir, "blobs")
        os.chmod(blobs, 0)
        try:
            proc = self.run_cli("gc", "--execute")
        finally:
            os.chmod(blobs, 0o755)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, b"")
        self.assertEqual(proc.stderr.count(b"\n"), 1)
        self.assertEqual(json.loads(proc.stderr.decode("utf-8")),
                         {"ok": False, "error": "gc io error"})


if __name__ == "__main__":
    unittest.main()

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


if __name__ == "__main__":
    unittest.main()

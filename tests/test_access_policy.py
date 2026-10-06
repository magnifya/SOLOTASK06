"""Tests for per-tenant access policies and signed reads."""

import hashlib
import hmac
import json
import os
import shutil
import tempfile
import time
import unittest

from objstore.store import ContentAddressedStore, ObjectStoreError

SECRET = "0123456789abcdef"
OTHER_SECRET = "fedcba9876543210"


def sha(payload):
    return hashlib.sha256(payload).hexdigest()


def future(seconds=3600):
    return int(time.time()) + seconds


def expected_signature(tenant, secret, method, key, version_id, expires):
    fields = (tenant, method, key, version_id or "", str(expires))
    return hmac.new(secret.encode("utf-8"), "\n".join(fields).encode("utf-8"),
                    hashlib.sha256).hexdigest()


class AccessPolicyTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-access-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def make_signed(self, tenant="acme", secret=SECRET):
        self.store.set_access_policy("signed", secret=secret, tenant=tenant)

    def sign(self, method, key, version_id=None, expires=None,
             tenant="acme"):
        return self.store.sign_request(method, key, version_id=version_id,
                                       expires=expires or future(),
                                       tenant=tenant)


class TestAccessPolicy(AccessPolicyTestCase):
    def test_default_is_public(self):
        self.assertEqual(self.store.get_access_policy(), {"mode": "public"})
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_set_and_get_roundtrip(self):
        self.make_signed()
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "signed"})
        # The secret is never exposed through the getter.
        self.assertNotIn("secret", self.store.get_access_policy(tenant="acme"))
        self.assertEqual(self.store.get_access_policy(), {"mode": "public"})

    def test_policy_is_per_tenant(self):
        self.make_signed("acme")
        self.assertEqual(self.store.get_access_policy(tenant="other"),
                         {"mode": "public"})

    def test_set_public_restores_default(self):
        self.make_signed()
        self.store.set_access_policy("public", tenant="acme")
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_secret_length_bounds(self):
        self.store.set_access_policy("signed", secret="x" * 16, tenant="acme")
        self.store.set_access_policy("signed", secret="x" * 256, tenant="acme")
        for secret in ("x" * 15, "x" * 257, "", 1234567890123456, None,
                       b"bytes-not-text!!"):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_access_policy("signed", secret=secret,
                                             tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid access policy",
                             repr(secret))

    def test_invalid_mode(self):
        for mode in ("private", "SIGNED", "Signed", "", None, True, 1, []):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_access_policy(mode, tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid access policy",
                             repr(mode))
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_public_mode_rejects_secret(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.set_access_policy("public", secret=SECRET,
                                         tenant="acme")
        self.assertEqual(str(ctx.exception), "invalid access policy")

    def test_invalid_tenant(self):
        for call in (lambda: self.store.set_access_policy("signed",
                                                          secret=SECRET,
                                                          tenant="BAD!"),
                     lambda: self.store.get_access_policy(tenant="BAD!"),
                     lambda: self.store.clear_access_policy(tenant="BAD!"),
                     lambda: self.store.sign_request("GET", "k",
                                                     expires=future(),
                                                     tenant="BAD!")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen(self):
        self.make_signed()
        self.store.put("k", b"data", tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_access_policy(tenant="acme"),
                         {"mode": "signed"})
        with self.assertRaises(ObjectStoreError) as ctx:
            reopened.get("k", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = expected_signature("acme", SECRET, "GET", "k", None,
                                       expires)
        self.assertEqual(reopened.get("k", tenant="acme", expires=expires,
                                      signature=signature)[0], b"data")

    def test_clear_restores_public(self):
        self.make_signed()
        self.store.put("k", b"data", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.get("k", tenant="acme")
        self.store.clear_access_policy(tenant="acme")
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"data")

    def test_clear_without_policy_is_noop(self):
        self.assertIsNone(self.store.clear_access_policy(tenant="acme"))
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_io_error_keeps_previous_policy(self):
        self.make_signed()
        # Replacing the index path with a directory makes the atomic
        # temp-write-plus-replace fail with an OSError on every platform.
        os.remove(self.store.index_path)
        os.mkdir(self.store.index_path)
        try:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_access_policy("signed", secret=OTHER_SECRET,
                                             tenant="acme")
            self.assertEqual(str(ctx.exception), "access policy io error")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_access_policy("public", tenant="acme")
            self.assertEqual(str(ctx.exception), "access policy io error")
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.clear_access_policy(tenant="acme")
            self.assertEqual(str(ctx.exception), "access policy io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "signed"})
        # The old secret still signs reads.
        expires = future()
        self.store.put("k", b"data", tenant="acme")
        signature = expected_signature("acme", SECRET, "GET", "k", None,
                                       expires)
        self.assertEqual(self.store.get("k", tenant="acme", expires=expires,
                                        signature=signature)[0], b"data")

    def test_corrupted_access_in_index(self):
        for bad in ({"access": []},
                    {"access": {"acme": []}},
                    {"access": {"acme": {}}},
                    {"access": {"acme": {"mode": "private"}}},
                    {"access": {"acme": {"mode": "signed"}}},
                    {"access": {"acme": {"mode": "signed", "secret": "short"}}},
                    {"access": {"acme": {"mode": "signed", "secret": 42}}},
                    {"access": {"BAD TENANT": {"mode": "public"}}}):
            document = {"version": 1, "objects": {}}
            document.update(bad)
            with open(self.store.index_path, "w") as handle:
                handle.write(json.dumps(document))
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"),
                            bad)

    def test_old_index_without_access_map_loads_public(self):
        document = {"version": 1,
                    "objects": {"k": {"sha256": sha(b"data"), "size": 4,
                                      "content_type": None}}}
        with open(self.store.index_path, "w") as handle:
            handle.write(json.dumps(document))
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_access_policy(), {"mode": "public"})


class TestSignRequest(AccessPolicyTestCase):
    def test_signature_matches_manual_hmac(self):
        self.make_signed()
        expires = future()
        for method in ("GET", "HEAD"):
            signature = self.store.sign_request(method, "docs/a.txt",
                                                expires=expires,
                                                tenant="acme")
            self.assertEqual(signature,
                             expected_signature("acme", SECRET, method,
                                                "docs/a.txt", None, expires))
            self.assertTrue(signature.islower() or signature.isdigit())

    def test_signature_binds_tenant_key_method_and_expiry(self):
        self.make_signed()
        expires = future()
        base = self.sign("GET", "k", expires=expires)
        self.assertNotEqual(base, self.sign("HEAD", "k", expires=expires))
        self.assertNotEqual(base, self.sign("GET", "other", expires=expires))
        self.assertNotEqual(base, self.sign("GET", "k", expires=expires + 1))
        self.make_signed("other")
        self.assertNotEqual(base, self.sign("GET", "k", expires=expires,
                                            tenant="other"))

    def test_secret_change_invalidates_old_signatures(self):
        self.make_signed()
        expires = future()
        old = self.sign("GET", "k", expires=expires)
        self.make_signed(secret=OTHER_SECRET)
        self.assertNotEqual(old, self.sign("GET", "k", expires=expires))

    def test_invalid_method(self):
        self.make_signed()
        for method in ("POST", "PUT", "DELETE", "get", "", None):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.sign_request(method, "k", expires=future(),
                                        tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid signature",
                             repr(method))

    def test_invalid_expires(self):
        self.make_signed()
        now = int(time.time())
        for expires in (None, now - 1, now, True, "123", 1.5, []):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.sign_request("GET", "k", expires=expires,
                                        tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid signature",
                             repr(expires))

    def test_invalid_version_id(self):
        self.make_signed()
        for version_id in ("not-a-version", 123, "0" * 31):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.sign_request("GET", "k", version_id=version_id,
                                        expires=future(), tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid signature",
                             repr(version_id))

    def test_sign_without_signed_policy(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.sign_request("GET", "k", expires=future(),
                                    tenant="acme")
        self.assertEqual(str(ctx.exception), "invalid signature")


class TestSignedReads(AccessPolicyTestCase):
    def test_public_reads_ignore_signature_params(self):
        self.store.put("k", b"data", tenant="acme")
        self.assertEqual(self.store.get("k", tenant="acme", expires=1,
                                        signature="junk")[0], b"data")
        self.assertEqual(self.store.head("k", tenant="acme")["size"], 4)

    def test_signed_get_requires_signature(self):
        self.store.put("k", b"data", tenant="acme")
        self.make_signed()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("k", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = self.sign("GET", "k", expires=expires)
        data, entry = self.store.get("k", tenant="acme", expires=expires,
                                     signature=signature)
        self.assertEqual(data, b"data")
        self.assertEqual(entry["sha256"], sha(b"data"))

    def test_signed_head_requires_signature(self):
        self.store.put("k", b"data", tenant="acme")
        self.make_signed()
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head("k", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = self.sign("HEAD", "k", expires=expires)
        self.assertEqual(self.store.head("k", tenant="acme", expires=expires,
                                         signature=signature)["size"], 4)

    def test_get_signature_does_not_authorize_head(self):
        self.store.put("k", b"data", tenant="acme")
        self.make_signed()
        expires = future()
        get_signature = self.sign("GET", "k", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head("k", tenant="acme", expires=expires,
                            signature=get_signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_denied_read_variants(self):
        self.store.put("k", b"data", tenant="acme")
        self.make_signed()
        expires = future()
        good = self.sign("GET", "k", expires=expires)
        cases = (
            # missing one or both parameters
            {}, {"expires": expires}, {"signature": good},
            # expired or malformed expiry
            {"expires": int(time.time()) - 1, "signature": good},
            {"expires": True, "signature": good},
            {"expires": "soon", "signature": good},
            # wrong key, wrong secret, truncated or non-text signature
            {"expires": expires, "signature": self.sign("GET", "other",
                                                        expires=expires)},
            {"expires": expires,
             "signature": expected_signature("acme", OTHER_SECRET, "GET", "k",
                                             None, expires)},
            {"expires": expires, "signature": good[:-1]},
            {"expires": expires, "signature": good.upper()},
            {"expires": expires, "signature": "é" * 64},
            {"expires": expires, "signature": None},
            {"expires": expires, "signature": 42},
        )
        for kwargs in cases:
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.get("k", tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "access denied", kwargs)

    def test_authorization_precedes_lookup(self):
        self.make_signed()
        # An unknown key with a bad signature is denied, not "not found".
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("missing", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        # A validly signed read of an unknown key is a normal not found.
        expires = future()
        signature = self.sign("GET", "missing", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("missing", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "not found: missing")

    def test_signed_policy_does_not_gate_writes_or_lists(self):
        self.make_signed()
        self.store.put("k", b"data", tenant="acme")
        self.assertEqual(self.store.list_objects(tenant="acme")["items"][0]
                         ["key"], "k")
        self.store.delete("k", tenant="acme")
        self.assertEqual(self.store.list_objects(tenant="acme")["items"], [])

    def test_other_tenant_stays_public(self):
        self.store.put("k", b"data", tenant="acme")
        self.store.put("k", b"data")
        self.make_signed()
        self.assertEqual(self.store.get("k")[0], b"data")
        with self.assertRaises(ObjectStoreError):
            self.store.get("k", tenant="acme")


class TestSignedVersionReads(AccessPolicyTestCase):
    def setUp(self):
        super().setUp()
        self.store.set_versioning(True, tenant="acme")
        self.store.put("k", b"v1", tenant="acme")
        self.second = self.store.put("k", b"v2", tenant="acme")

    def test_signed_version_get_and_head(self):
        self.make_signed()
        version_id = self.second["version_id"]
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", version_id, tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head_version("k", version_id, tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = self.sign("GET", "k", version_id=version_id,
                              expires=expires)
        data, entry = self.store.get_version("k", version_id, tenant="acme",
                                             expires=expires,
                                             signature=signature)
        self.assertEqual(data, b"v2")
        self.assertEqual(entry["version_id"], version_id)
        head_signature = self.sign("HEAD", "k", version_id=version_id,
                                   expires=expires)
        entry = self.store.head_version("k", version_id, tenant="acme",
                                        expires=expires,
                                        signature=head_signature)
        self.assertEqual(entry["size"], 2)

    def test_object_signature_does_not_authorize_version(self):
        self.make_signed()
        version_id = self.second["version_id"]
        expires = future()
        object_signature = self.sign("GET", "k", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("k", version_id, tenant="acme",
                                   expires=expires,
                                   signature=object_signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_version_authorization_precedes_lookup(self):
        self.make_signed()
        version_id = self.second["version_id"]
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("missing", version_id, tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = self.sign("GET", "missing", version_id=version_id,
                              expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("missing", version_id, tenant="acme",
                                   expires=expires, signature=signature)
        self.assertEqual(str(ctx.exception), "not found: missing")

    def test_version_reads_stay_public_without_policy(self):
        version_id = self.second["version_id"]
        data, _ = self.store.get_version("k", version_id, tenant="acme")
        self.assertEqual(data, b"v2")


if __name__ == "__main__":
    unittest.main()

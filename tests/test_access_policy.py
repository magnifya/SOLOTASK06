"""Tests for per-tenant read access policies and request signing."""

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


def sign(secret, tenant, method, key, version_id, expires):
    payload = "\n".join((tenant, method, key, version_id or "", str(expires)))
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"),
                    hashlib.sha256).hexdigest()


def future():
    return int(time.time()) + 3600


class AccessPolicyTestCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="objstore-access-")
        self.store = ContentAddressedStore(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def sign(self, method, key, version_id=None, expires=None, tenant="acme"):
        return self.store.sign_request(method, key, version_id=version_id,
                                       expires=expires or future(),
                                       tenant=tenant)


class TestAccessPolicy(AccessPolicyTestCase):
    def test_default_is_public(self):
        self.assertEqual(self.store.get_access_policy(), {"mode": "public"})
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_set_and_get_roundtrip(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "signed", "secret": SECRET})
        self.assertEqual(self.store.get_access_policy(), {"mode": "public"})

    def test_set_replaces_secret(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
        self.store.set_access_policy("signed", secret=OTHER_SECRET,
                                     tenant="acme")
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "signed", "secret": OTHER_SECRET})

    def test_set_public_clears(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
        self.store.set_access_policy("public", tenant="acme")
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_invalid_policies(self):
        for mode, secret in (("private", SECRET), ("SIGNED", SECRET),
                             ("", SECRET), (None, SECRET), (True, SECRET),
                             (["signed"], SECRET),
                             ("signed", None), ("signed", ""),
                             ("signed", "x" * 15), ("signed", "x" * 257),
                             ("signed", 16), ("signed", b"x" * 16),
                             ("signed", ["x" * 16])):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.set_access_policy(mode, secret=secret, tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid access policy",
                             (mode, secret))
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_secret_length_boundaries(self):
        self.store.set_access_policy("signed", secret="x" * 16, tenant="acme")
        self.store.set_access_policy("signed", secret="x" * 256, tenant="acme")
        self.store.set_access_policy("signed", secret="秘密密钥" * 4,
                                     tenant="acme")

    def test_invalid_tenant(self):
        for call in (lambda: self.store.set_access_policy("signed", secret=SECRET, tenant="BAD!"),
                     lambda: self.store.get_access_policy(tenant="BAD!"),
                     lambda: self.store.clear_access_policy(tenant="BAD!"),
                     lambda: self.store.sign_request("GET", "k", expires=future(), tenant="BAD!")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertTrue(str(ctx.exception).startswith("invalid tenant"))

    def test_policy_survives_reopen(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
        reopened = ContentAddressedStore(self.root)
        self.assertEqual(reopened.get_access_policy(tenant="acme"),
                         {"mode": "signed", "secret": SECRET})

    def test_clear_restores_public(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
        self.store.put("k", b"data", tenant="acme")
        with self.assertRaises(ObjectStoreError):
            self.store.get("k", tenant="acme")
        self.assertIsNone(self.store.clear_access_policy(tenant="acme"))
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})
        self.assertEqual(self.store.get("k", tenant="acme")[0], b"data")

    def test_clear_without_policy_is_noop(self):
        self.assertIsNone(self.store.clear_access_policy(tenant="acme"))
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "public"})

    def test_io_error_keeps_previous_policy(self):
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")
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
                self.store.clear_access_policy(tenant="acme")
            self.assertEqual(str(ctx.exception), "access policy io error")
        finally:
            os.rmdir(self.store.index_path)
        self.assertEqual(self.store.get_access_policy(tenant="acme"),
                         {"mode": "signed", "secret": SECRET})

    def test_corrupted_access_in_index(self):
        for bad in ({"access": []},
                    {"access": {"acme": []}},
                    {"access": {"acme": {}}},
                    {"access": {"acme": {"mode": "public"}}},
                    {"access": {"acme": {"mode": "signed"}}},
                    {"access": {"acme": {"mode": "signed", "secret": "short"}}},
                    {"access": {"acme": {"mode": "signed", "secret": 42}}},
                    {"access": {"BAD TENANT": {"mode": "signed", "secret": SECRET}}}):
            document = {"version": 1, "objects": {}}
            document.update(bad)
            with open(self.store.index_path, "w") as handle:
                handle.write(json.dumps(document))
            with self.assertRaises(ObjectStoreError) as ctx:
                ContentAddressedStore(self.root)
            self.assertTrue(str(ctx.exception).startswith("corrupted index"), bad)


class TestSignRequest(AccessPolicyTestCase):
    def setUp(self):
        super().setUp()
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")

    def test_matches_manual_hmac(self):
        expires = future()
        signature = self.store.sign_request("GET", "a/b", expires=expires,
                                            tenant="acme")
        self.assertEqual(signature,
                         sign(SECRET, "acme", "GET", "a/b", None, expires))

    def test_version_id_joined_as_empty_when_none(self):
        expires = future()
        with_none = self.store.sign_request("GET", "k", version_id=None,
                                            expires=expires, tenant="acme")
        explicit = self.store.sign_request("GET", "k", version_id="",
                                           expires=expires, tenant="acme")
        self.assertEqual(with_none, explicit)
        versioned = self.store.sign_request("GET", "k", version_id="v" * 32,
                                            expires=expires, tenant="acme")
        self.assertNotEqual(with_none, versioned)

    def test_method_and_tenant_are_signed(self):
        expires = future()
        get_sig = self.store.sign_request("GET", "k", expires=expires,
                                          tenant="acme")
        head_sig = self.store.sign_request("HEAD", "k", expires=expires,
                                           tenant="acme")
        self.assertNotEqual(get_sig, head_sig)
        self.store.set_access_policy("signed", secret=SECRET, tenant="other")
        other_sig = self.store.sign_request("GET", "k", expires=expires,
                                            tenant="other")
        self.assertNotEqual(get_sig, other_sig)

    def test_invalid_method(self):
        for method in ("POST", "PUT", "DELETE", "get", "", None, 42):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.sign_request(method, "k", expires=future(),
                                        tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid signature", method)

    def test_invalid_expires(self):
        for expires in (None, True, "9999999999", 1.5,
                        int(time.time()) - 1, int(time.time()) - 3600):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.sign_request("GET", "k", expires=expires,
                                        tenant="acme")
            self.assertEqual(str(ctx.exception), "invalid signature", expires)

    def test_unsigned_tenant_cannot_sign(self):
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.sign_request("GET", "k", expires=future())
        self.assertEqual(str(ctx.exception), "invalid signature")


class TestSignedReads(AccessPolicyTestCase):
    def setUp(self):
        super().setUp()
        self.store.put("doc", b"payload", tenant="acme")
        self.store.set_versioning(True, tenant="acme")
        self.first = self.store.put("doc", b"v1", tenant="acme")
        self.second = self.store.put("doc", b"v2", tenant="acme")
        self.store.set_access_policy("signed", secret=SECRET, tenant="acme")

    def test_public_tenant_unaffected(self):
        self.store.put("doc", b"payload")
        self.assertEqual(self.store.get("doc")[0], b"payload")
        self.assertEqual(self.store.head("doc")["size"], 7)

    def test_unsigned_reads_denied(self):
        for call in (lambda: self.store.get("doc", tenant="acme"),
                     lambda: self.store.head("doc", tenant="acme"),
                     lambda: self.store.get_version(
                         "doc", self.first["version_id"], tenant="acme"),
                     lambda: self.store.head_version(
                         "doc", self.first["version_id"], tenant="acme")):
            with self.assertRaises(ObjectStoreError) as ctx:
                call()
            self.assertEqual(str(ctx.exception), "access denied")

    def test_signed_object_reads(self):
        expires = future()
        signature = self.sign("GET", "doc", expires=expires)
        data, entry = self.store.get("doc", tenant="acme", expires=expires,
                                     signature=signature)
        self.assertEqual(data, b"v2")
        self.assertEqual(entry["sha256"], hashlib.sha256(b"v2").hexdigest())
        signature = self.sign("HEAD", "doc", expires=expires)
        entry = self.store.head("doc", tenant="acme", expires=expires,
                                signature=signature)
        self.assertEqual(entry["size"], 2)

    def test_signed_version_reads(self):
        expires = future()
        version_id = self.first["version_id"]
        signature = self.sign("GET", "doc", version_id=version_id,
                              expires=expires)
        data, entry = self.store.get_version(
            "doc", version_id, tenant="acme", expires=expires,
            signature=signature)
        self.assertEqual(data, b"v1")
        self.assertEqual(entry["version_id"], version_id)
        signature = self.sign("HEAD", "doc", version_id=version_id,
                              expires=expires)
        entry = self.store.head_version(
            "doc", version_id, tenant="acme", expires=expires,
            signature=signature)
        self.assertEqual(entry["version_id"], version_id)

    def test_get_signature_does_not_authorize_head(self):
        expires = future()
        signature = self.sign("GET", "doc", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head("doc", tenant="acme", expires=expires,
                            signature=signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_expired_signature_denied(self):
        expires = int(time.time()) - 10
        signature = sign(SECRET, "acme", "GET", "doc", None, expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("doc", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_malformed_credentials_denied(self):
        good = future()
        for kwargs in ({"expires": good, "signature": None},
                       {"expires": good, "signature": "z" * 64},
                       {"expires": good, "signature": "0" * 63},
                       {"expires": good, "signature": 42},
                       {"expires": None, "signature": "0" * 64},
                       {"expires": "soon", "signature": "0" * 64},
                       {"expires": True, "signature": "0" * 64}):
            with self.assertRaises(ObjectStoreError) as ctx:
                self.store.get("doc", tenant="acme", **kwargs)
            self.assertEqual(str(ctx.exception), "access denied", kwargs)

    def test_wrong_secret_denied(self):
        expires = future()
        signature = sign(OTHER_SECRET, "acme", "GET", "doc", None, expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("doc", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_signature_for_other_key_denied(self):
        expires = future()
        signature = self.sign("GET", "other", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("doc", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_check_precedes_key_lookup(self):
        # No credentials on an unknown key: access denied, not not found.
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("missing", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.head_version("missing", "0" * 32, tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")

    def test_valid_signature_unknown_key_is_not_found(self):
        expires = future()
        signature = self.sign("GET", "missing", expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("missing", tenant="acme", expires=expires,
                           signature=signature)
        self.assertTrue(str(ctx.exception).startswith("not found"))

    def test_valid_signature_unknown_version_is_not_found(self):
        expires = future()
        version_id = "1" * 32
        signature = self.sign("GET", "doc", version_id=version_id,
                              expires=expires)
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get_version("doc", version_id, tenant="acme",
                                   expires=expires, signature=signature)
        self.assertTrue(str(ctx.exception).startswith("not found"))

    def test_secret_rotation_invalidates_old_signatures(self):
        expires = future()
        signature = self.sign("GET", "doc", expires=expires)
        self.store.set_access_policy("signed", secret=OTHER_SECRET,
                                     tenant="acme")
        with self.assertRaises(ObjectStoreError) as ctx:
            self.store.get("doc", tenant="acme", expires=expires,
                           signature=signature)
        self.assertEqual(str(ctx.exception), "access denied")

    def test_policy_enforced_after_reopen(self):
        reopened = ContentAddressedStore(self.root)
        with self.assertRaises(ObjectStoreError) as ctx:
            reopened.get("doc", tenant="acme")
        self.assertEqual(str(ctx.exception), "access denied")
        expires = future()
        signature = sign(SECRET, "acme", "GET", "doc", None, expires)
        self.assertEqual(reopened.get("doc", tenant="acme", expires=expires,
                                      signature=signature)[0], b"v2")


if __name__ == "__main__":
    unittest.main()

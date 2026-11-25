import os
import unittest
from unittest.mock import patch

from app.credentials import (
    CredentialEncryptionError,
    decrypt_secret,
    encrypt_secret,
)


class CredentialEncryptionTests(unittest.TestCase):
    def test_encrypts_and_decrypts_secret(self):
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}):
            encrypted = encrypt_secret("provider-secret-value")
            self.assertNotIn(b"provider-secret-value", encrypted)
            self.assertEqual(decrypt_secret(encrypted), "provider-secret-value")

    def test_random_nonce_produces_distinct_ciphertexts(self):
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}):
            first = encrypt_secret("same-secret")
            second = encrypt_secret("same-secret")
            self.assertNotEqual(first, second)
            self.assertEqual(decrypt_secret(first), decrypt_secret(second))

    def test_tampering_is_rejected(self):
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}):
            encrypted = bytearray(encrypt_secret("secret-value"))
            encrypted[-1] ^= 1
            with self.assertRaises(CredentialEncryptionError):
                decrypt_secret(bytes(encrypted))

    def test_invalid_base64_characters_are_rejected(self):
        with patch.dict(os.environ, {"NEXUS_CREDENTIAL_ENCRYPTION_KEY": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY$"}):
            with self.assertRaises(CredentialEncryptionError):
                encrypt_secret("secret-value")

    def test_missing_encryption_key_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(CredentialEncryptionError):
                encrypt_secret("secret-value")


if __name__ == "__main__":
    unittest.main()

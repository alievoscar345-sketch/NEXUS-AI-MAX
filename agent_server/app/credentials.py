from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CredentialEncryptionError(RuntimeError):
    """Raised when the server-side credential encryption key is missing or invalid."""


_AAD = b"NEXUS_PROVIDER_CREDENTIAL_V1"


def _encryption_key() -> bytes:
    encoded = os.getenv("NEXUS_CREDENTIAL_ENCRYPTION_KEY", "").strip()
    if not encoded:
        raise CredentialEncryptionError("Credential encryption is not configured")
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        key = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise CredentialEncryptionError("Credential encryption key is invalid") from exc
    if len(key) != 32:
        raise CredentialEncryptionError("Credential encryption key must decode to exactly 32 bytes")
    return key


def encrypt_secret(secret: str) -> bytes:
    if not isinstance(secret, str) or not secret:
        raise CredentialEncryptionError("Secret must be a non-empty string")
    try:
        nonce = os.urandom(12)
        ciphertext = AESGCM(_encryption_key()).encrypt(nonce, secret.encode("utf-8"), _AAD)
        return nonce + ciphertext
    except CredentialEncryptionError:
        raise
    except Exception as exc:
        raise CredentialEncryptionError("Unable to encrypt provider credential") from exc


def decrypt_secret(payload: bytes) -> str:
    if not isinstance(payload, bytes) or len(payload) < 29:
        raise CredentialEncryptionError("Encrypted provider credential is malformed")
    try:
        plaintext = AESGCM(_encryption_key()).decrypt(payload[:12], payload[12:], _AAD)
        return plaintext.decode("utf-8")
    except CredentialEncryptionError:
        raise
    except Exception as exc:
        raise CredentialEncryptionError("Unable to decrypt provider credential") from exc

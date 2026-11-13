"""
Security and Cryptography module for Interaction Ledger.
Provides:
- Deterministic HMAC-SHA256 blind indexing for phone numbers.
- Authenticated AES-256-GCM encryption for PII fields at rest.
- Phone number masking (+91******XXXX).
"""

from __future__ import annotations

import base64
import hmac
import hashlib
import os
import re
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag

from .config import Config


class SecurityError(Exception):
    """Raised when an encryption, decryption, or authentication error occurs."""
    pass


def hash_phone(canonical_phone: str, workflow_id: str, hmac_salt: bytes) -> str:
    """
    Generate a deterministic blind index for a lead's phone number scoped to a workflow.
    
    Formula: HMAC-SHA256(hmac_salt, f"{workflow_id}:{canonical_phone}")
    
    Args:
        canonical_phone: Canonical phone number (+91XXXXXXXXXX).
        workflow_id: Workflow identifier (e.g. 'motor_renewal').
        hmac_salt: Secret pepper/salt key bytes.
        
    Returns:
        Hexadecimal HMAC-SHA256 digest string.
    """
    if not canonical_phone:
        raise ValueError("canonical_phone must not be empty")
    if not workflow_id:
        raise ValueError("workflow_id must not be empty")

    message = f"{workflow_id}:{canonical_phone}".encode("utf-8")
    return hmac.new(hmac_salt, message, hashlib.sha256).hexdigest()


def mask_phone(phone: str) -> str:
    """
    Mask a phone number according to the specification:
    '+91******' followed by the last 4 digits (e.g., '+91******5036').
    
    Args:
        phone: Canonical or raw phone string.
        
    Returns:
        Masked phone string.
    """
    digits = re.sub(r'\D', '', phone)
    if len(digits) >= 4:
        last4 = digits[-4:]
    else:
        last4 = digits.rjust(4, '*')
    return f"+91******{last4}"


def encrypt_field(plaintext: Optional[str], key: bytes) -> Optional[str]:
    """
    Encrypt a plaintext string using AES-256-GCM authenticated encryption.
    
    Structure: Base64(12-byte nonce + ciphertext + 16-byte authentication tag).
    
    Args:
        plaintext: The string to encrypt (returns None if input is None).
        key: 32-byte AES key.
        
    Returns:
        URL-safe Base64 encoded string containing the nonce and ciphertext.
    """
    if plaintext is None:
        return None

    try:
        aesgcm = AESGCM(key)
        nonce = os.urandom(12)  # 96-bit unique nonce
        data = plaintext.encode("utf-8")
        ciphertext = aesgcm.encrypt(nonce, data, None)
        combined = nonce + ciphertext
        return base64.b64encode(combined).decode("ascii")
    except Exception as e:
        raise SecurityError(f"Encryption failed: {e}") from e


def decrypt_field(ciphertext_b64: Optional[str], key: bytes) -> Optional[str]:
    """
    Decrypt an AES-256-GCM encrypted string.
    
    Args:
        ciphertext_b64: Base64 encoded string containing nonce + ciphertext + tag.
        key: 32-byte AES key.
        
    Returns:
        Decrypted plaintext string, or None if input was None.
        
    Raises:
        SecurityError: If ciphertext is corrupt, tampered, or decryption key is incorrect.
    """
    if ciphertext_b64 is None:
        return None

    try:
        combined = base64.b64decode(ciphertext_b64.encode("ascii"))
        if len(combined) < 28:  # 12 nonce + 16 tag minimum
            raise SecurityError("Ciphertext payload too short")

        nonce = combined[:12]
        ciphertext = combined[12:]
        aesgcm = AESGCM(key)
        decrypted_bytes = aesgcm.decrypt(nonce, ciphertext, None)
        return decrypted_bytes.decode("utf-8")
    except InvalidTag as e:
        raise SecurityError("Decryption failed: invalid authentication tag or wrong key") from e
    except Exception as e:
        raise SecurityError(f"Decryption failed: {e}") from e


class SecurityManager:
    """Convenience wrapper managing encryption and hashing using an active Config."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def hash_phone(self, canonical_phone: str, workflow_id: str) -> str:
        """Hash a phone number using the configured HMAC salt."""
        return hash_phone(canonical_phone, workflow_id, self.config.hmac_salt)

    def mask_phone(self, phone: str) -> str:
        """Mask a phone number."""
        return mask_phone(phone)

    def encrypt(self, plaintext: Optional[str]) -> Optional[str]:
        """Encrypt a PII string using the configured AES-256 key."""
        return encrypt_field(plaintext, self.config.encryption_key)

    def decrypt(self, ciphertext_b64: Optional[str]) -> Optional[str]:
        """Decrypt an encrypted string using the configured AES-256 key."""
        return decrypt_field(ciphertext_b64, self.config.encryption_key)


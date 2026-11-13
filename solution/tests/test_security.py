"""
Unit tests for security, cryptography, and configuration.
"""

import os
import secrets
import pytest

from ledger.config import Config, ConfigurationError
from ledger.security import (
    SecurityError,
    SecurityManager,
    decrypt_field,
    encrypt_field,
    hash_phone,
    mask_phone,
)


@pytest.fixture
def test_keys():
    """Generate temporary test cryptographic keys."""
    enc_key = secrets.token_bytes(32)
    salt = secrets.token_bytes(32)
    return enc_key, salt


@pytest.fixture
def security_manager(test_keys):
    """Instantiate SecurityManager with test configuration."""
    enc_key, salt = test_keys
    config = Config(
        encryption_key=enc_key,
        hmac_salt=salt,
        db_path=":memory:",
    )
    return SecurityManager(config)


def test_deterministic_phone_hashing(security_manager):
    """Test that phone hashing is deterministic for the same phone and workflow."""
    phone = "+919820012345"
    wf = "motor_renewal"
    h1 = security_manager.hash_phone(phone, wf)
    h2 = security_manager.hash_phone(phone, wf)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex digest length


def test_workflow_scoped_phone_hashing(security_manager):
    """Test that different workflows produce different hashes for the same phone number."""
    phone = "+919820012345"
    h_motor = security_manager.hash_phone(phone, "motor_renewal")
    h_feedback = security_manager.hash_phone(phone, "policy_feedback")
    assert h_motor != h_feedback


def test_different_phones_produce_different_hashes(security_manager):
    """Test that distinct phone numbers produce distinct hashes."""
    p1 = "+919820012345"
    p2 = "+919820012346"
    wf = "motor_renewal"
    assert security_manager.hash_phone(p1, wf) != security_manager.hash_phone(p2, wf)


def test_encryption_decryption_round_trip(security_manager):
    """Test AES-256-GCM encryption and decryption round-trip."""
    test_strings = [
        "Aarav Sharma",
        "+919820012345",
        "Diya Reddy-Iyer",
        "Unicode: தமிழ் / हिन्दी",
        "",
    ]
    for text in test_strings:
        enc = security_manager.encrypt(text)
        assert enc != text
        dec = security_manager.decrypt(enc)
        assert dec == text


def test_encryption_none_handling(security_manager):
    """Test that encrypting or decrypting None returns None."""
    assert security_manager.encrypt(None) is None
    assert security_manager.decrypt(None) is None


def test_nonce_uniqueness(security_manager):
    """Test that encrypting the same text twice produces distinct ciphertexts (random IV)."""
    text = "Secret Customer Name"
    enc1 = security_manager.encrypt(text)
    enc2 = security_manager.encrypt(text)
    assert enc1 != enc2
    assert security_manager.decrypt(enc1) == text
    assert security_manager.decrypt(enc2) == text


def test_incorrect_encryption_key_rejection(test_keys):
    """Test that attempting decryption with an incorrect key raises SecurityError."""
    enc_key1, _ = test_keys
    enc_key2 = secrets.token_bytes(32)
    text = "Confidential Data"

    ciphertext = encrypt_field(text, enc_key1)
    with pytest.raises(SecurityError, match="invalid authentication tag or wrong key"):
        decrypt_field(ciphertext, enc_key2)


def test_tampered_ciphertext_rejection(test_keys):
    """Test that tampered ciphertext payload is rejected."""
    enc_key, _ = test_keys
    ciphertext = encrypt_field("Sensitive Info", enc_key)

    # Corrupt last character of base64
    tampered = ciphertext[:-2] + ("A" if ciphertext[-2] != "A" else "B") + ciphertext[-1]
    with pytest.raises(SecurityError):
        decrypt_field(tampered, enc_key)


def test_phone_masking():
    """Test phone masking according to the +91******XXXX specification."""
    assert mask_phone("+919820012345") == "+91******2345"
    assert mask_phone("9820012345") == "+91******2345"
    assert mask_phone("91-98200-12345") == "+91******2345"
    assert mask_phone("09820012345") == "+91******2345"
    assert mask_phone("(+91) 98200 12345") == "+91******2345"


def test_config_missing_keys_fails_safely(monkeypatch, tmp_path):
    """Test that Config.from_env() raises ConfigurationError if keys are missing."""
    monkeypatch.delenv("LEDGER_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("LEDGER_HMAC_SALT", raising=False)
    empty_env = tmp_path / "empty.env"
    empty_env.touch()

    with pytest.raises(ConfigurationError, match="LEDGER_ENCRYPTION_KEY"):
        Config.from_env(env_file=empty_env)


def test_config_invalid_key_length_fails(monkeypatch):
    """Test that an invalid key length raises ConfigurationError."""
    monkeypatch.setenv("LEDGER_ENCRYPTION_KEY", "abcd")  # Too short
    monkeypatch.setenv("LEDGER_HMAC_SALT", "saltsaltsalt")

    with pytest.raises(ConfigurationError, match="64-character hexadecimal"):
        Config.from_env()


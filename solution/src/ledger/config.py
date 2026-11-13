"""
Configuration management for the Interaction Ledger.
Loads encryption keys and environment settings securely.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ConfigurationError(Exception):
    """Raised when required configuration or encryption keys are missing or invalid."""
    pass


def _load_dotenv(dotenv_path: Path) -> None:
    """Minimal zero-dependency .env loader that populates os.environ if not set."""
    if not dotenv_path.is_file():
        return
    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass


@dataclass(frozen=True)
class Config:
    """Configuration container holding encryption keys and database paths."""
    encryption_key: bytes
    hmac_salt: bytes
    db_path: Path

    @classmethod
    def from_env(
        cls,
        env_file: Optional[Path | str] = None,
        db_path_override: Optional[Path | str] = None,
        allow_auto_generate: bool = False,
    ) -> Config:
        """
        Load configuration from environment variables (and optional .env file).
        
        Raises:
            ConfigurationError: If required keys are missing or invalid.
        """
        if env_file:
            _load_dotenv(Path(env_file))
        else:
            default_env = Path("solution/.env")
            if default_env.is_file():
                _load_dotenv(default_env)
            elif Path(".env").is_file():
                _load_dotenv(Path(".env"))

        # 1. Encryption Key (AES-256 requires 32 bytes)
        raw_key = os.environ.get("LEDGER_ENCRYPTION_KEY", "").strip()
        raw_salt = os.environ.get("LEDGER_HMAC_SALT", "").strip()

        # If keys are missing and auto-generation allowed (e.g. from CLI on clean clone)
        if allow_auto_generate and (not raw_key or not raw_salt):
            env_target = Path("solution/.env")
            if not env_target.is_file():
                import secrets
                auto_key = secrets.token_hex(32)
                auto_salt = secrets.token_hex(32)
                env_target.parent.mkdir(parents=True, exist_ok=True)
                with open(env_target, "w", encoding="utf-8") as f:
                    f.write(f"# Auto-generated local cryptographic keys\n")
                    f.write(f"LEDGER_ENCRYPTION_KEY={auto_key}\n")
                    f.write(f"LEDGER_HMAC_SALT={auto_salt}\n")
                    f.write(f"LEDGER_DB_PATH=solution/ledger.db\n")
            _load_dotenv(env_target)
            raw_key = os.environ.get("LEDGER_ENCRYPTION_KEY", "").strip()
            raw_salt = os.environ.get("LEDGER_HMAC_SALT", "").strip()

        if not raw_key:
            raise ConfigurationError(
                "Missing required environment variable: LEDGER_ENCRYPTION_KEY. "
                "Provide a 32-byte hexadecimal key (64 characters)."
            )
        try:
            key_bytes = bytes.fromhex(raw_key)
            if len(key_bytes) != 32:
                raise ValueError
        except Exception as e:
            raise ConfigurationError(
                f"LEDGER_ENCRYPTION_KEY must be a 64-character hexadecimal string (32 bytes). Error: {e}"
            ) from e

        # 2. HMAC Salt for phone blind index
        raw_salt = os.environ.get("LEDGER_HMAC_SALT", "").strip()
        if not raw_salt:
            raise ConfigurationError(
                "Missing required environment variable: LEDGER_HMAC_SALT. "
                "Provide a secure salt string or hex for HMAC-SHA256."
            )
        try:
            # Prefer hex decode if 64 chars, else encode utf-8
            if len(raw_salt) >= 32 and all(c in "0123456789abcdefABCDEF" for c in raw_salt):
                salt_bytes = bytes.fromhex(raw_salt)
            else:
                salt_bytes = raw_salt.encode("utf-8")
        except Exception:
            salt_bytes = raw_salt.encode("utf-8")

        # 3. Database Path
        if db_path_override:
            db_path = Path(db_path_override)
        else:
            db_env = os.environ.get("LEDGER_DB_PATH", "solution/ledger.db").strip()
            db_path = Path(db_env)

        return cls(
            encryption_key=key_bytes,
            hmac_salt=salt_bytes,
            db_path=db_path,
        )


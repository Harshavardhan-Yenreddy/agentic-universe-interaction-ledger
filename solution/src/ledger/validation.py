"""
Field validation and normalization rules per assignment specification.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple


def normalize_phone(raw_value: Optional[str]) -> Optional[str]:
    """
    Validate and normalize an Indian mobile number.
    Rules:
    - Strip every non-digit.
    - If 12 digits starting with '91', drop the '91'.
    - If 11 digits starting with '0', drop the '0'.
    - Valid ONLY if exactly 10 digits and starts with 6, 7, 8 or 9.
    - Store canonically as '+91XXXXXXXXXX'.
    """
    if raw_value is None:
        return None
    val = raw_value.strip()
    if not val:
        return None

    digits = re.sub(r'\D', '', val)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]

    if len(digits) == 10 and digits[0] in "6789":
        return f"+91{digits}"
    return None


def parse_date(raw_value: Optional[str]) -> Tuple[Optional[str], bool]:
    """
    Validate and normalize a date string.
    Rules:
    - Accept YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (case-insensitive).
    - Dates are day-first.
    - Impossible dates (like 31/02) are invalid.
    - Returns (canonical_date_iso, is_valid).
      If raw_value is empty/whitespace, returns (None, True).
      If present but invalid, returns (None, False).
    """
    if raw_value is None:
        return None, True
    val = raw_value.strip()
    if not val:
        return None, True

    # Try supported day-first formats
    formats = [
        "%Y-%m-%d",    # ISO format (year first)
        "%d/%m/%Y",    # Day-first slash
        "%d-%m-%Y",    # Day-first dash
        "%d-%b-%Y",    # Day-first month name (e.g. 01-Dec-2026)
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(val, fmt)
            return dt.strftime("%Y-%m-%d"), True
        except ValueError:
            continue

    return None, False


def parse_decimal(raw_value: Optional[str]) -> Tuple[Optional[float], bool]:
    """
    Validate and normalize a decimal currency amount.
    Rules:
    - Optionally prefixed by ₹, Rs, Rs., or INR (case-insensitive, optional space).
    - Commas are thousands separators.
    - Must be >= 0. Round to 2 places.
    - Returns (normalized_amount, is_valid).
      If raw_value is empty/whitespace, returns (None, True).
      If present but invalid, returns (None, False).
    """
    if raw_value is None:
        return None, True
    val = raw_value.strip()
    if not val:
        return None, True

    # Strip currency prefixes (₹, Rs, Rs., INR) case-insensitively
    clean = re.sub(r'(?i)^(?:₹|rs\.?|inr)\s*', '', val).strip()
    clean = clean.replace(',', '')

    try:
        amt = float(clean)
        if amt >= 0:
            return round(amt, 2), True
        return None, False
    except ValueError:
        return None, False


def parse_enum(raw_value: Optional[str], allowed_values: tuple[str, ...]) -> Tuple[Optional[str], bool]:
    """
    Validate and normalize an enum string.
    Rules:
    - Compared case-insensitively against allowed values.
    - Stored lowercase.
    - Returns (normalized_value, is_valid).
      If raw_value is empty/whitespace, returns (None, True).
      If present but not in allowed_values, returns (None, False).
    """
    if raw_value is None:
        return None, True
    val = raw_value.strip()
    if not val:
        return None, True

    allowed_lower = {v.lower() for v in allowed_values}
    lower_val = val.lower()
    if lower_val in allowed_lower:
        return lower_val, True
    return None, False


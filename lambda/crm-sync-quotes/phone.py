import re
from typing import Optional, List, Tuple


_DIGITS_RE = re.compile(r"\D+")


def _digits_only(s: str) -> str:
    return _DIGITS_RE.sub("", s or "")


def _try_candidate(
    number: str, area_code: str, default_country_code: str
) -> Optional[str]:
    """Return E.164 string or None for one (number, area_code) pair."""
    digits = _digits_only(number)
    if not digits:
        return None

    expected_len_with_cc = len(default_country_code) + 10

    # Already country-prefixed?
    if (
        len(digits) == expected_len_with_cc
        and digits.startswith(default_country_code)
    ):
        return f"+{digits}"

    # Already a complete 10-digit national number — area code not needed.
    if len(digits) == 10:
        return f"+{default_country_code}{digits}"

    # Concatenate area code + number, strip leading zeros
    combined = (_digits_only(area_code) + digits).lstrip("0")

    if len(combined) == 10:
        return f"+{default_country_code}{combined}"

    if (
        len(combined) == expected_len_with_cc
        and combined.startswith(default_country_code)
    ):
        return f"+{combined}"

    return None


def normalize_to_e164(
    movil: str,
    ladam: str,
    tel1: str,
    tel2: str,
    tel3: str,
    lada: str,
    default_country_code: str = "52",
) -> Optional[str]:
    """
    Return an E.164 string (e.g. '+528112345678') or None if no phone field
    yields a recoverable Mexican mobile number.

    Candidate priority:
      1. (MOVIL, LADAM)
      2. (TEL1, LADA)
      3. (TEL2, LADA)
      4. (TEL3, LADA)
    """
    candidates: List[Tuple[str, str]] = [
        (movil, ladam),
        (tel1, lada),
        (tel2, lada),
        (tel3, lada),
    ]
    for number, area_code in candidates:
        result = _try_candidate(number, area_code, default_country_code)
        if result is not None:
            return result
    return None

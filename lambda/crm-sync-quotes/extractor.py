import re
import unicodedata
from dataclasses import dataclass
from typing import Optional, Sequence
from email.utils import parseaddr


@dataclass(frozen=True)
class EmailExtractor:
    """
    Extracts the first SES-safe email address from a messy string.

    Features:
      - Handles "Name <email@domain>"
      - Splits on common separators (; , | whitespace)
      - Removes control/whitespace/zero-width chars
      - Removes accents from local-part (e.g., Almacén2 -> Almacen2)
      - IDNA-normalizes domain (international domains -> punycode)
      - Validates with an ASCII-only regex (SES-friendly)

    Returns:
      - normalized email string if found
      - None if no valid email found
    """

    separators: Sequence[str] = (";", ",", "|", " ")
    zero_width: frozenset[str] = frozenset({"\u200b", "\u200c", "\u200d", "\ufeff"})

    # Practical SES-friendly ASCII email regex
    email_ascii_re: re.Pattern = re.compile(
        r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@" r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )

    def extract_first(self, raw: str | None) -> Optional[str]:
        if not raw:
            return None

        s = str(raw)

        # 1) Try tokenized candidates first (fast + matches your current behavior)
        for token in self._tokenize_candidates(s):
            if "@" not in token:
                continue
            normalized = self._normalize_and_validate(token)
            if normalized:
                return normalized

        # 2) Fallback: scan larger chunks split by typical delimiters (more forgiving)
        for piece in re.split(r"[;,|]", s):
            if "@" not in piece:
                continue
            normalized = self._normalize_and_validate(piece.strip())
            if normalized:
                return normalized

        return None

    def extract_first_or_empty(self, raw: str | None) -> str:
        """Drop-in convenience for your current code style that expects ''."""
        return self.extract_first(raw) or ""

    def _tokenize_candidates(self, s: str) -> list[str]:
        s = s.strip()
        if not s:
            return []
        for sep in self.separators:
            if sep in s:
                parts = [p.strip() for p in s.split(sep)]
                return [p for p in parts if p]
        return [s]

    def _normalize_and_validate(self, candidate: str) -> Optional[str]:
        _, addr = parseaddr(candidate)
        candidate = addr or candidate
        candidate = unicodedata.normalize("NFKC", candidate).strip()
        candidate = self._remove_controls_and_separators(candidate)

        if "@" not in candidate:
            return None

        local, domain = candidate.split("@", 1)
        local = self._remove_accents(local).strip(".")
        domain = domain.strip(".").lower()
        try:
            domain = domain.encode("idna").decode("ascii")
        except Exception:
            return None

        email = f"{local}@{domain}"

        if not self.email_ascii_re.fullmatch(email):
            return None

        return email

    def _remove_controls_and_separators(self, s: str) -> str:
        out: list[str] = []
        for ch in s:
            cat = unicodedata.category(ch)
            if cat.startswith(("C", "Z")) or ch in self.zero_width:
                continue
            out.append(ch)
        return "".join(out)

    def _remove_accents(self, s: str) -> str:
        if not s:
            return s
        normalized = unicodedata.normalize("NFKD", s)
        return "".join(ch for ch in normalized if not unicodedata.combining(ch))

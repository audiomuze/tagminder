"""Helpers for title-like capitalization.

Purpose:
    Provide a reusable, English-style title-casing helper for music titles and
    album titles. The rules are intentionally conservative: they normalize
    standard title case, preserve valid Roman numerals, preserve a small set of
    known acronyms, and avoid over-eager changes when a token looks ambiguous.

This module is part of Tagminder.

SQLite tables referenced:
    - None

Author: audiomuze
Last updated: 2026-06-13
"""

from __future__ import annotations

import re
import unicodedata


_SMALL_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "as",
        "at",
        "but",
        "by",
        "for",
        "from",
        "if",
        "in",
        "into",
        "nor",
        "of",
        "on",
        "or",
        "over",
        "per",
        "so",
        "the",
        "to",
        "up",
        "via",
        "with",
        "yet",
    }
)

_LOCATION_CONTEXT_WORDS = frozenset(
    {
        "at",
        "from",
        "in",
        "into",
        "near",
        "of",
        "on",
        "outside",
        "through",
        "to",
        "toward",
        "towards",
        "via",
        "within",
        "inside",
        "around",
        "across",
        "by",
    }
)

_CONTRACTION_SUFFIXES = frozenset({"d", "ll", "m", "n", "re", "s", "t", "ve"})

_ACRONYMS = frozenset(
    {
        # Only include acronyms that are unambiguously non-words.
        # Do NOT include "us" (pronoun), "am" (verb), "in" (preposition) etc.
        "cd",
        "dj",
        "ep",
        "eu",
        "fm",
        "lp",
        "r&b",
        "tv",
        "uk",
        "usa",
    }
)

_MUSICAL_MODIFIER_WORDS = frozenset(
    {
        "agitato",
        "assai",
        "con",
        "dolce",
        "espressivo",
        "giocoso",
        "ma",
        "molto",
        "non",
        "poco",
        "sempre",
        "troppo",
        "vivace",
    }
)

_STATE_CODES = frozenset(
    {
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
    }
)

_ROMAN_NUMERAL_RE = re.compile(
    r"^(?=[MDCLXVI])(M{0,4}(CM|CD|D?C{0,3})"
    r"(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))$",
    re.IGNORECASE,
)

_DOTTED_ACRONYM_RE = re.compile(r"^(?:[A-Za-z]\.){2,}[A-Za-z]?\.?$")
_WORD_RE = re.compile(r"^([^\w]*)([\w][\w'’./&-]*)([^\w]*)$")


def normalize_title_case(text: str | None) -> str | None:
    """Normalize a title-like string to conservative English title case."""

    if text is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    if not cleaned:
        return None

    tokens = cleaned.split(" ")
    normalized_tokens: list[str] = []
    prev_core_lower: str | None = None
    prev_core_in_caps_run = False
    prev_had_comma = False
    clause_start = True

    for index, token in enumerate(tokens):
        is_first = index == 0
        is_last = index == len(tokens) - 1
        next_core_lower: str | None = None
        next_core_in_caps_run = False
        if not is_last:
            _, next_core, _ = _split_token(tokens[index + 1])
            if next_core:
                next_core_lower = next_core.lower()
            next_core_in_caps_run = _is_caps_run_token(next_core)

        leading, core, trailing = _split_token(token)
        if not core:
            normalized_tokens.append(token)
            prev_had_comma = "," in token
            clause_start = token in {":", "?", "!", "-", "--", "--", "–", "—", "/"}
            continue

        # Afrikaans invariant: standalone apostrophe-n article is always lowercase
        # and always uses the ASCII apostrophe form: 'n.
        if leading in {"'", "’"} and core.lower() == "n" and trailing == "":
            normalized_tokens.append("'n")
            prev_core_lower = "n"
            prev_had_comma = False
            clause_start = False
            continue

        normalized_core = _normalize_core(
            core,
            is_first=is_first,
            is_last=is_last,
            clause_start=clause_start,
            prev_core_lower=prev_core_lower,
            prev_core_in_caps_run=prev_core_in_caps_run,
            prev_had_comma=prev_had_comma,
            next_core_lower=next_core_lower,
            next_core_in_caps_run=next_core_in_caps_run,
        )
        normalized_tokens.append(f"{leading}{normalized_core}{trailing}")

        prev_core_lower = _core_lower(normalized_core)
        prev_core_in_caps_run = _is_caps_run_token(core)
        prev_had_comma = "," in trailing
        clause_start = trailing.endswith((":", "?", "!", "/", "-", "--", "–", "—"))

    return " ".join(normalized_tokens)


def _split_token(token: str) -> tuple[str, str, str]:
    match = _WORD_RE.match(token)
    if not match:
        return token, "", ""
    return match.group(1), match.group(2), match.group(3)


def _core_lower(core: str) -> str:
    return re.sub(r"[^\w&]+", "", core).replace("_", "").lower()


def _normalize_core(
    core: str,
    *,
    is_first: bool,
    is_last: bool,
    clause_start: bool,
    prev_core_lower: str | None,
    prev_core_in_caps_run: bool,
    prev_had_comma: bool,
    next_core_lower: str | None,
    next_core_in_caps_run: bool,
) -> str:
    if not core:
        return core

    if "/" in core:
        return "/".join(
            _normalize_core(
                part,
                is_first=(part_index == 0),
                is_last=(part_index == len(core.split("/")) - 1),
                clause_start=True,
                prev_core_lower=None,
                prev_core_in_caps_run=False,
                prev_had_comma=False,
                next_core_lower=None,
                next_core_in_caps_run=False,
            )
            for part_index, part in enumerate(core.split("/"))
        )

    if "-" in core:
        parts = core.split("-")
        return "-".join(
            _normalize_core(
                part,
                is_first=(part_index == 0 and is_first),
                is_last=(part_index == len(parts) - 1 and is_last),
                clause_start=(clause_start and part_index == 0),
                prev_core_lower=prev_core_lower if part_index == 0 else None,
                prev_core_in_caps_run=prev_core_in_caps_run if part_index == 0 else False,
                prev_had_comma=prev_had_comma if part_index == 0 else False,
                next_core_lower=None,
                next_core_in_caps_run=False,
            )
            for part_index, part in enumerate(parts)
        )

    core_lower = core.lower()

    # Preserve diacritic-bearing words exactly as-is to avoid language-specific
    # casing regressions in multilingual metadata.
    if _contains_diacritics(core):
        return core

    if _is_dotted_acronym(core) or _is_acronym(core_lower):
        return _normalize_acronym(core)

    # Preserve likely standalone acronyms only. Long all-caps words are treated as
    # regular words so partially normalized phrases (e.g. "MOUTH OF a RIVER") can
    # converge to proper title case on subsequent runs.
    if len(core) >= 2 and core.isupper() and core.isalpha() and not (
        prev_core_in_caps_run or next_core_in_caps_run
    ) and len(core) <= 4:
        return core

    if _is_roman_numeral(core):
        return core.upper()

    if core.endswith(".") and _is_roman_numeral(core[:-1]):
        return core[:-1].upper() + "."

    # Musical key context: "in A minor", "in F# major", etc.
    if _is_musical_key_token(core) and prev_core_lower == "in" and next_core_lower in {"major", "minor"}:
        return core.upper()

    # State codes are only auto-uppercased in strongly geographic contexts.
    if _is_state_code(core) and (
        (prev_had_comma and is_last)
        or (core.isupper() and prev_core_lower in _LOCATION_CONTEXT_WORDS)
    ):
        return core.upper()

    # Preserve common lowercase classical tempo/modifier words unless they start a clause.
    if core.islower() and core_lower in _MUSICAL_MODIFIER_WORDS and not (is_first or clause_start):
        return core_lower

    if core_lower in _SMALL_WORDS and not (is_first or is_last or clause_start):
        return core_lower

    return _titlecase_with_apostrophes(core)


def _is_acronym(core_lower: str) -> bool:
    return core_lower in _ACRONYMS


def _normalize_acronym(core: str) -> str:
    if _DOTTED_ACRONYM_RE.match(core):
        return core.upper()
    return core.upper()


def _is_dotted_acronym(core: str) -> bool:
    return bool(_DOTTED_ACRONYM_RE.match(core))


def _is_roman_numeral(core: str) -> bool:
    return bool(_ROMAN_NUMERAL_RE.match(core))


def _is_state_code(core: str) -> bool:
    return core.upper() in _STATE_CODES and len(core) == 2 and core.isalpha()


def _is_musical_key_token(core: str) -> bool:
    return bool(re.fullmatch(r"[A-Ga-g](?:[#b])?", core))


def _is_caps_run_token(core: str) -> bool:
    return core.isalpha() and core.isupper() and (len(core) >= 2 or core.lower() in _SMALL_WORDS)


def _contains_diacritics(text: str) -> bool:
    normalized = unicodedata.normalize("NFD", text)
    return any(unicodedata.combining(ch) for ch in normalized)


def _titlecase_with_apostrophes(core: str) -> str:
    core_lower = core.lower()
    if core_lower in {"n'", "n’"}:
        return "n'"

    parts = re.split(r"(['’])", core)
    if len(parts) == 1:
        return _capitalize_simple(core)

    normalized_parts: list[str] = []
    for index, part in enumerate(parts):
        if part in {"'", "’"}:
            normalized_parts.append(part)
            continue

        lower = part.lower()
        if index > 0 and lower in _CONTRACTION_SUFFIXES:
            normalized_parts.append(lower)
        else:
            normalized_parts.append(_capitalize_simple(lower))

    return "".join(normalized_parts)


def _capitalize_simple(text: str) -> str:
    if not text:
        return text
    return text[:1].upper() + text[1:].lower()

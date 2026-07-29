"""Contributor name case normalization helpers.

Shared fallback casing for contributor-like fields when canonical reference mappings
are unavailable.

This module is intentionally shared by contributor normalization and MusicBrainz ID
population scripts so unresolved contributor display casing follows one consistent
set of rules across write paths.
"""

from __future__ import annotations

import re


SURNAME_DICT = {
    # Mac surnames
    "macintyre": "MacIntyre",
    "macallister": "MacAllister",
    "mackenzie": "MacKenzie",
    "macdonald": "MacDonald",
    "maclachlan": "MacLachlan",
    "macgregor": "MacGregor",
    "macpherson": "MacPherson",
    "maclean": "MacLean",
    "macleod": "MacLeod",
    "macneil": "MacNeil",
    # Mc surnames
    "mcbean": "McBean",
    "mccutcheon": "McCutcheon",
    "mcdaniel": "McDaniel",
    "mcdaniels": "McDaniels",
    "mcdermott": "McDermott",
    "mcdonagh": "McDonagh",
    "mcdonald": "McDonald",
    "mcintyre": "McIntyre",
    "mckenzie": "McKenzie",
    "mcallister": "McAllister",
    "mcfarland": "McFarland",
    "mcgregor": "McGregor",
    "mcguire": "McGuire",
    "mcgrath": "McGrath",
    "mcguirk": "McGuirk",
    "mcilrath": "McIlrath",
    "mckinna": "McKinna",
    "mclaughlin": "McLaughlin",
    "mclean": "McLean",
    "mcleod": "McLeod",
    "mcmahon": "McMahon",
    "mcnamara": "McNamara",
    "mcpherson": "McPherson",
    "mcvey": "McVey",
    # O' surnames
    "obrien": "O'Brien",
    "odonnell": "O'Donnell",
    "oconnor": "O'Connor",
    "oneill": "O'Neill",
    "omally": "O'Malley",
    "ohara": "O'Hara",
    "okeeffe": "O'Keeffe",
    "oreilly": "O'Reilly",
    "osullivan": "O'Sullivan",
    # Fitz surnames
    "fitzgibbon": "FitzGibbon",
    "fitzhenry": "FitzHenry",
    # De / De La surnames
    "decoster": "DeCoster",
    "de coster": "DeCoster",
    "desantis": "DeSantis",
    "delorean": "DeLorean",
    "delacruz": "De La Cruz",
    "delarosa": "De La Rosa",
    "deguzman": "De Guzman",
    "degaulle": "de Gaulle",
    "demedici": "de Medici",
    "devito": "DeVito",
    "depalma": "DePalma",
    "donatello": "Donatello",
    # Van surnames
    "vanpelt": "Van Pelt",
    "vandamme": "Van Damme",
    "vanhalen": "Van Halen",
    "vanderbilt": "Vanderbilt",
    "vanderveer": "Vanderveer",
    "vanburen": "Van Buren",
    "vanhouten": "Van Houten",
    "vangogh": "van Gogh",
    # Von surnames
    "vonbeethoven": "von Beethoven",
    "vontrapp": "von Trapp",
    "vonbraun": "von Braun",
    "vondoom": "Von Doom",
}


def smart_title(text: str | None) -> str | None:
    """Apply contributor-oriented title casing for unresolved names."""
    if not text:
        return text

    lowered = text.lower()
    if lowered in SURNAME_DICT:
        return SURNAME_DICT[lowered]

    def fix_caps_word(word: str, is_first_word: bool = False) -> str:
        lowered_word = word.lower()
        if lowered_word in SURNAME_DICT:
            return SURNAME_DICT[lowered_word]

        if re.match(r"^([A-Za-z]\.)+$", word, re.IGNORECASE):
            return word.upper()

        lower_words = {
            "of",
            "a",
            "an",
            "the",
            "and",
            "but",
            "or",
            "for",
            "nor",
            "on",
            "at",
            "to",
            "from",
            "by",
        }

        if is_first_word:
            return word.capitalize()
        if re.match(r"^[IVXLCDM]+$", word.upper()):
            return word.upper()
        if "." in word:
            parts = word.split(".")
            processed_parts = []
            for part in parts:
                if part and len(part) == 1:
                    processed_parts.append(part.upper())
                else:
                    processed_parts.append(part.capitalize())
            return ".".join(processed_parts)
        if "'" in word or "’" in word:
            apos_pos = max(word.find("'"), word.find("’"))
            if 0 < apos_pos < len(word) - 1:
                return word[:apos_pos].capitalize() + word[apos_pos:]
            return word.capitalize()
        if "-" in word:
            return "-".join(part.capitalize() for part in word.split("-"))
        if word.lower() in lower_words:
            return word.lower()
        return word.capitalize()

    word_pattern = r"(?:[A-Za-z]\.){2,}|[A-Za-z]\.|Mc\w+|O'\w+|\w+(?:['’]\w+)?"
    non_word_pattern = r"[^\w\s]+"
    combined_pattern = rf"({word_pattern})|({non_word_pattern})|\s+"

    parts = re.findall(combined_pattern, text)
    result: list[str] = []
    capitalize_next = True

    for part_tuple in parts:
        word = part_tuple[0] or part_tuple[1]
        if word:
            if re.match(word_pattern, word):
                processed_word = fix_caps_word(word, is_first_word=capitalize_next)
                if processed_word.lower().endswith("'s") or processed_word.lower().endswith("’s"):
                    processed_word = processed_word[:-2] + "'s"
                elif (
                    word.lower().startswith("o'")
                    and len(word) > 2
                    and word[2].lower() != "s"
                    and word[2] != " "
                ):
                    processed_word = "O'" + fix_caps_word(word[2:], is_first_word=False)

                result.append(processed_word)
                capitalize_next = False
            else:
                result.append(word)
                capitalize_next = word in "({[<"
        else:
            result.append(" ")

    processed_text = "".join(result)
    processed_text = re.sub(r"(\w)['’]S\b", r"\1's", processed_text)
    return processed_text

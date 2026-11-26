# ruff: noqa: RUF001  (Cyrillic letters that look Latin are the point of this module)
# Ukraine's official romanisation (Cabinet of Ministers resolution 55, 2010), used in passports:
# Шевченко -> Shevchenko, Юлія -> Yuliia.
UKRAINIAN = {
    "а": "a", "б": "b", "в": "v", "г": "h", "ґ": "g", "д": "d", "е": "e", "є": "ie", "ж": "zh",
    "з": "z", "и": "y", "і": "i", "ї": "i", "й": "i", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "shch", "ь": "", "ю": "iu", "я": "ia",
}  # fmt: skip

# At the start of a word these letters are written differently.
UKRAINIAN_WORD_START = {"є": "ye", "ї": "yi", "й": "y", "ю": "yu", "я": "ya"}

APOSTROPHES = frozenset("'ʼ’")


def romanize_ukrainian(text: str) -> str:
    latin: list[str] = []
    at_word_start = True
    for index, char in enumerate(text):
        lower = char.lower()
        if char in APOSTROPHES:
            continue
        if lower == "г" and index > 0 and text[index - 1].lower() == "з":
            letters = "gh"  # зг is written zgh, so it isn't read as zh
        elif at_word_start and lower in UKRAINIAN_WORD_START:
            letters = UKRAINIAN_WORD_START[lower]
        else:
            letters = UKRAINIAN.get(lower, char)
        if char != lower and letters:
            # A capital in an all-caps word (ШЕВЧЕНКО) stays all caps: SHEVCHENKO, not ShEVChENKO.
            neighbours = text[index - 1 : index] + text[index + 1 : index + 2]
            all_caps = any(neighbour.isupper() for neighbour in neighbours)
            letters = letters.upper() if all_caps else letters[0].upper() + letters[1:]
        latin.append(letters)
        at_word_start = not char.isalpha()
    return "".join(latin)

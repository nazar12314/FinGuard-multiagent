import icu

from finguard.modules.screening.domain.romanisation import romanize_ukrainian

_CYRILLIC_BLOCK = range(0x0400, 0x0500)
_UKRAINIAN_LETTERS = frozenset("ґєіїҐЄІЇ")


class IcuTransliterator:
    def __init__(self) -> None:
        # ICU's generic romanisation is scholarly (Шевченко -> Sevcenko), while lists and
        # passports mostly use BGN/PCGN-style spellings (Shevchenko), so both are produced, and
        # Ukraine's passport system (Yuliia, Honcharenko) besides. Any-Latin afterwards picks up
        # the letters a language's table doesn't cover.
        self._generic = icu.Transliterator.createInstance("Any-Latin; Latin-ASCII")
        self._russian = icu.Transliterator.createInstance(
            "Russian-Latin/BGN; Any-Latin; Latin-ASCII"
        )
        self._ukrainian = icu.Transliterator.createInstance(
            "Ukrainian-Latin/BGN; Any-Latin; Latin-ASCII"
        )

    def latin_spellings(self, text: str) -> list[str]:
        if text.isascii():
            return [text]
        spellings = [self._generic.transliterate(text)]
        if any(ord(char) in _CYRILLIC_BLOCK for char in text):
            spellings.append(self._russian.transliterate(text))
            if _UKRAINIAN_LETTERS.intersection(text):
                spellings.append(self._ukrainian.transliterate(text))
            spellings.append(self._generic.transliterate(romanize_ukrainian(text)))
        return list(dict.fromkeys(spellings))

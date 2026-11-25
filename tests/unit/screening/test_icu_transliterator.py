import pytest

from finguard.modules.screening.adapters.icu_transliterator import IcuTransliterator


@pytest.fixture(scope="module")
def transliterator() -> IcuTransliterator:
    return IcuTransliterator()


def test_leaves_ascii_names_alone(transliterator: IcuTransliterator) -> None:
    assert transliterator.latin_spellings("Petr Ivanov") == ["Petr Ivanov"]


def test_strips_accents_from_latin_names(transliterator: IcuTransliterator) -> None:
    assert transliterator.latin_spellings("Gülşen Öztürk") == ["Gulsen Ozturk"]


def test_gives_cyrillic_names_their_generic_and_bgn_spellings(
    transliterator: IcuTransliterator,
) -> None:
    assert transliterator.latin_spellings("Христо Иванов") == [
        "Hristo Ivanov",
        "Khristo Ivanov",
    ]


def test_adds_the_ukrainian_spelling_for_ukrainian_names(
    transliterator: IcuTransliterator,
) -> None:
    spellings = transliterator.latin_spellings("Шевченко Юлія")

    assert spellings[0] == "Sevcenko Ulia"
    assert "Shevchenko Yuliya" in spellings


def test_romanises_other_scripts(transliterator: IcuTransliterator) -> None:
    assert transliterator.latin_spellings("王凤朝") == ["wang feng chao"]

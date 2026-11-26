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
    assert transliterator.latin_spellings("Христо Иванов")[:2] == [
        "Hristo Ivanov",
        "Khristo Ivanov",
    ]


def test_adds_the_ukrainian_spelling_for_ukrainian_names(
    transliterator: IcuTransliterator,
) -> None:
    spellings = transliterator.latin_spellings("Шевченко Юлія")

    assert spellings[0] == "Sevcenko Ulia"
    assert "Shevchenko Yuliya" in spellings


def test_gives_every_cyrillic_name_its_ukrainian_passport_spelling(
    transliterator: IcuTransliterator,
) -> None:
    assert "Shevchenko Yuliia" in transliterator.latin_spellings("Шевченко Юлія")
    # No letter here is specific to Ukrainian, yet the name may well be Ukrainian.
    assert "Honcharenko Oleh" in transliterator.latin_spellings("Гончаренко Олег")


def test_romanises_other_scripts(transliterator: IcuTransliterator) -> None:
    assert transliterator.latin_spellings("王凤朝") == ["wang feng chao"]

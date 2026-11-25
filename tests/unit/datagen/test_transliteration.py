import pytest

from datagen.transliteration import romanize_ukrainian


@pytest.mark.parametrize(
    ("cyrillic", "latin"),
    [
        ("Шевченко", "Shevchenko"),
        ("Юлія", "Yuliia"),
        ("Олексій", "Oleksii"),
        ("Євген Яковенко", "Yevhen Yakovenko"),
        ("Їжакевич", "Yizhakevych"),
        ("Згурський", "Zghurskyi"),
        ("Лукʼян", "Lukian"),
        ("Ольга-Марія", "Olha-Mariia"),
        ("Ґанна", "Ganna"),
    ],
)
def test_follows_the_official_romanisation(cyrillic: str, latin: str) -> None:
    assert romanize_ukrainian(cyrillic) == latin


def test_leaves_latin_text_alone() -> None:
    assert romanize_ukrainian("Anna Kowalska") == "Anna Kowalska"

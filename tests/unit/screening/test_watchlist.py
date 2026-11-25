import pytest
from tests.watchlist_entities import PERSON

from finguard.modules.screening.domain.watchlist import name_forms


def test_normalises_case_accents_and_punctuation() -> None:
    assert name_forms("  Müller-Lüdenscheidt, Hans-Jörg ").normalised == (
        "muller ludenscheidt hans jorg"
    )


def test_sorts_tokens_so_name_order_doesnt_matter() -> None:
    assert name_forms("Ivanov, Petr").token_sorted == name_forms("Petr IVANOV").token_sorted


@pytest.mark.parametrize("spelling", ["Mohamed", "Muhamad", "Mohammed"])
def test_phonetic_keys_meet_common_spelling_variants(spelling: str) -> None:
    assert name_forms(spelling).phonetic == name_forms("Mohammed").phonetic


def test_a_name_of_only_punctuation_normalises_to_nothing() -> None:
    assert name_forms("-- / --").normalised == ""


def test_an_entity_lists_its_name_then_aliases_once() -> None:
    assert PERSON.names() == ("Petr Ivanov", "Ivanov, Petr")

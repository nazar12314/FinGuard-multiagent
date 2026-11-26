import polars as pl

from datagen.people import customers_of_people


def test_people_reach_customers_up_a_holding_chain_even_a_circular_one() -> None:
    tables = {
        "companies": pl.DataFrame(
            {"company_id": ["co-1", "co-h1", "co-h2"], "customer_id": ["cus-1", None, None]}
        ),
        "shareholdings": pl.DataFrame(
            {
                "company_id": ["co-1", "co-h1", "co-h2", "co-h2"],
                "holder_id": ["co-h1", "co-h2", "co-1", "per-top"],
                "holder_type": ["company", "company", "company", "person"],
            }
        ),
        "officers": pl.DataFrame({"company_id": ["co-1"], "person_id": ["per-director"]}),
        "persons": pl.DataFrame(
            {
                "person_id": ["per-sole", "per-director", "per-top"],
                "customer_id": ["cus-2", None, None],
            }
        ),
    }

    people = customers_of_people(
        tables["persons"], tables["companies"], tables["officers"], tables["shareholdings"]
    )

    assert people.rows() == [
        ("per-director", "cus-1"),
        ("per-sole", "cus-2"),
        ("per-top", "cus-1"),
    ]

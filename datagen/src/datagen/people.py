import polars as pl


def customers_of_people(
    persons: pl.DataFrame,
    companies: pl.DataFrame,
    officers: pl.DataFrame,
    shareholdings: pl.DataFrame,
) -> pl.DataFrame:
    """(person_id, customer_id) for every customer a person stands behind: sole traders, and the
    officers and owners of company customers, including owners further up a holding chain.
    A nominee director stands behind every company they sit on."""
    reach = companies.filter(pl.col("customer_id").is_not_null()).select(
        "company_id", "customer_id"
    )
    company_holders = shareholdings.filter(pl.col("holder_type") == "company")
    while True:
        parents = company_holders.join(reach, on="company_id").select(
            company_id="holder_id", customer_id="customer_id"
        )
        grown = pl.concat([reach, parents]).unique()
        # Circular ownership stops growing once every company in the loop is reached.
        if grown.height == reach.height:
            break
        reach = grown
    owners = shareholdings.filter(pl.col("holder_type") == "person").select(
        "company_id", person_id="holder_id"
    )
    return (
        pl.concat(
            [
                persons.filter(pl.col("customer_id").is_not_null()).select(
                    "person_id", "customer_id"
                ),
                pl.concat([owners, officers.select("company_id", "person_id")])
                .join(reach, on="company_id")
                .select("person_id", "customer_id"),
            ]
        )
        .unique()
        .sort("person_id", "customer_id")
    )


def laundering_linked_people(people: pl.DataFrame, customer_labels: pl.DataFrame) -> set[str]:
    """People who stand behind at least one laundering-linked customer."""
    linked = customer_labels.filter("laundering_linked").get_column("customer_id").implode()
    return set(people.filter(pl.col("customer_id").is_in(linked)).get_column("person_id"))

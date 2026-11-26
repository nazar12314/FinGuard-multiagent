import random
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl
from faker import Faker

from datagen.customers import COUNTRIES, seeded_faker

# How often a laundering ring that spans several of our customers gives itself away by sharing a
# device or a registered address.
RING_SHARES_DEVICE = 0.6
RING_SHARES_ADDRESS = 0.3

# Innocent sharing, common among small businesses, so that sharing alone never means a ring.
COMPANIES_AT_VIRTUAL_OFFICE = 0.12
COMPANIES_PER_VIRTUAL_OFFICE = 40
SOLE_TRADERS_WITH_RELATIVE = 0.04
RELATIVES_SHARE_DEVICE = 0.5
CUSTOMERS_WITH_BOOKKEEPER = 0.04
BOOKKEEPERS_PER_COUNTRY = 3

PLATFORMS = [("ios", 0.45), ("android", 0.4), ("web", 0.15)]


@dataclass(frozen=True)
class Contacts:
    addresses: pl.DataFrame
    customer_addresses: pl.DataFrame
    devices: pl.DataFrame
    customer_devices: pl.DataFrame
    labels: pl.DataFrame


def laundering_rings(holders: pl.DataFrame, account_labels: pl.DataFrame) -> list[list[str]]:
    """Our customers grouped by the laundering attempt they take part in, where it's several."""
    members = (
        holders.join(account_labels, on="account_id")
        .explode("attempt_ids")
        .drop_nulls("attempt_ids")
        .group_by("attempt_ids")
        .agg(customers=pl.col("customer_id").unique().sort())
        .filter(pl.col("customers").list.len() > 1)
        .sort("attempt_ids")
    )
    return [list(customers) for customers in members.get_column("customers").to_list()]


def generate_contacts(customers: pl.DataFrame, rings: list[list[str]], seed: int) -> Contacts:
    builder = _Builder(random.Random(f"{seed}:contacts"), seed)
    rows = customers.sort("customer_id").rows(named=True)
    by_id = {row["customer_id"]: row for row in rows}

    for row in rows:
        builder.link_address(row["customer_id"], builder.new_address(row["country"]), "registered")
        if row["customer_type"] == "sole_trader":
            home = builder.new_address(row["country"])
            builder.link_address(row["customer_id"], home, "owner_home")
        for _ in range(builder.rng.choice([1, 1, 2, 2, 3])):
            builder.link_device(row, builder.new_device())

    builder.share_virtual_offices([r for r in rows if r["customer_type"] == "company"])
    builder.share_between_relatives([r for r in rows if r["customer_type"] == "sole_trader"])
    builder.share_with_bookkeepers(rows)
    for ring in rings:
        builder.share_within_ring([by_id[customer_id] for customer_id in ring])
    return builder.contacts()


@dataclass
class _Builder:
    rng: random.Random
    seed: int
    addresses: list[dict[str, object]] = field(default_factory=list)
    customer_addresses: dict[tuple[str, str], str] = field(default_factory=dict)
    devices: list[dict[str, object]] = field(default_factory=list)
    customer_devices: dict[tuple[str, str], date] = field(default_factory=dict)
    labels: list[dict[str, object]] = field(default_factory=list)
    fakers: dict[str, Faker] = field(default_factory=dict)

    def new_address(self, country: str) -> str:
        faker = self._faker(country)
        address_id = f"adr-{len(self.addresses) + 1:06d}"
        self.addresses.append(
            {
                "address_id": address_id,
                "street": faker.street_address(),
                "postcode": faker.postcode(),
                "city": faker.city(),
                "country": country,
            }
        )
        return address_id

    def link_address(self, customer_id: str, address_id: str, role: str) -> None:
        self.customer_addresses[(customer_id, role)] = address_id

    def new_device(self) -> str:
        device_id = f"dev-{self.rng.getrandbits(48):012x}"
        [platform] = self.rng.choices([p for p, _ in PLATFORMS], [w for _, w in PLATFORMS])
        self.devices.append({"device_id": device_id, "platform": platform})
        return device_id

    def link_device(self, customer: dict[str, object], device_id: str) -> None:
        onboarded_at = customer["onboarded_at"]
        assert isinstance(onboarded_at, date)
        first_seen = onboarded_at + timedelta(days=self.rng.randint(0, 30))
        self.customer_devices.setdefault((str(customer["customer_id"]), device_id), first_seen)

    def share_virtual_offices(self, companies: list[dict[str, object]]) -> None:
        for country in COUNTRIES:
            local = [c for c in companies if c["country"] == country.code]
            tenants = [c for c in local if self.rng.random() < COMPANIES_AT_VIRTUAL_OFFICE]
            if not tenants:
                continue
            offices = [
                self.new_address(country.code)
                for _ in range(max(1, len(local) // COMPANIES_PER_VIRTUAL_OFFICE))
            ]
            for office in offices:
                self._label("address", office, "virtual_office")
            for tenant in tenants:
                self.link_address(
                    str(tenant["customer_id"]), self.rng.choice(offices), "registered"
                )

    def share_between_relatives(self, sole_traders: list[dict[str, object]]) -> None:
        for trader in sole_traders:
            if self.rng.random() >= SOLE_TRADERS_WITH_RELATIVE:
                continue
            neighbours = [
                t for t in sole_traders if t["country"] == trader["country"] and t is not trader
            ]
            if not neighbours:
                continue
            relative = self.rng.choice(neighbours)
            home = self.customer_addresses[(str(trader["customer_id"]), "owner_home")]
            self.link_address(str(relative["customer_id"]), home, "owner_home")
            self._label("address", home, "family")
            if self.rng.random() < RELATIVES_SHARE_DEVICE:
                device = self.new_device()
                self._label("device", device, "family")
                self.link_device(trader, device)
                self.link_device(relative, device)

    def share_with_bookkeepers(self, customers: list[dict[str, object]]) -> None:
        for country in COUNTRIES:
            clients = [
                c
                for c in customers
                if c["country"] == country.code and self.rng.random() < CUSTOMERS_WITH_BOOKKEEPER
            ]
            if len(clients) < 2:
                continue
            bookkeepers = [self.new_device() for _ in range(BOOKKEEPERS_PER_COUNTRY)]
            for device in bookkeepers:
                self._label("device", device, "bookkeeper")
            for client in clients:
                self.link_device(client, self.rng.choice(bookkeepers))

    def share_within_ring(self, members: list[dict[str, object]]) -> None:
        if self.rng.random() < RING_SHARES_DEVICE:
            device = self.new_device()
            self._label("device", device, "ring")
            for member in members:
                self.link_device(member, device)
        if self.rng.random() < RING_SHARES_ADDRESS:
            # A registered address is in the company's own country, so only members registered
            # in the same country can share one.
            for country in COUNTRIES:
                local = [m for m in members if m["country"] == country.code]
                if len(local) < 2:
                    continue
                address = self.new_address(country.code)
                self._label("address", address, "ring")
                for member in local:
                    self.link_address(str(member["customer_id"]), address, "registered")

    def contacts(self) -> Contacts:
        used_addresses = set(self.customer_addresses.values())
        used_devices = {device for _, device in self.customer_devices}
        return Contacts(
            addresses=pl.DataFrame(
                [a for a in self.addresses if a["address_id"] in used_addresses],
                schema=ADDRESS_SCHEMA,
            ),
            customer_addresses=pl.DataFrame(
                [
                    {"customer_id": customer, "role": role, "address_id": address}
                    for (customer, role), address in sorted(self.customer_addresses.items())
                ],
                schema=CUSTOMER_ADDRESS_SCHEMA,
            ),
            devices=pl.DataFrame(
                sorted(
                    (d for d in self.devices if d["device_id"] in used_devices),
                    key=lambda d: str(d["device_id"]),
                ),
                schema=DEVICE_SCHEMA,
            ),
            customer_devices=pl.DataFrame(
                [
                    {"customer_id": customer, "device_id": device, "first_seen_at": seen}
                    for (customer, device), seen in sorted(self.customer_devices.items())
                ],
                schema=CUSTOMER_DEVICE_SCHEMA,
            ),
            labels=pl.DataFrame(self.labels, schema=SHARED_ATTRIBUTE_LABEL_SCHEMA)
            .filter(pl.col("attribute_id").is_in(sorted(self._shared())))
            .unique()
            .sort("attribute", "attribute_id"),
        )

    def _shared(self) -> set[str]:
        """Addresses and devices that more than one customer ends up using."""
        users: dict[str, set[str]] = {}
        for (customer, _), address in self.customer_addresses.items():
            users.setdefault(address, set()).add(customer)
        for customer, device in self.customer_devices:
            users.setdefault(device, set()).add(customer)
        return {attribute for attribute, customers in users.items() if len(customers) > 1}

    def _label(self, attribute: str, attribute_id: str, reason: str) -> None:
        self.labels.append({"attribute": attribute, "attribute_id": attribute_id, "reason": reason})

    def _faker(self, country: str) -> Faker:
        if country not in self.fakers:
            [locale] = [c.locale for c in COUNTRIES if c.code == country]
            self.fakers[country] = seeded_faker(locale, f"{self.seed}:contacts:{country}")
        return self.fakers[country]


ADDRESS_SCHEMA = pl.Schema(
    {
        "address_id": pl.String(),
        "street": pl.String(),
        "postcode": pl.String(),
        "city": pl.String(),
        "country": pl.String(),
    }
)
CUSTOMER_ADDRESS_SCHEMA = pl.Schema(
    {"customer_id": pl.String(), "role": pl.String(), "address_id": pl.String()}
)
DEVICE_SCHEMA = pl.Schema({"device_id": pl.String(), "platform": pl.String()})
CUSTOMER_DEVICE_SCHEMA = pl.Schema(
    {"customer_id": pl.String(), "device_id": pl.String(), "first_seen_at": pl.Date()}
)
SHARED_ATTRIBUTE_LABEL_SCHEMA = pl.Schema(
    {"attribute": pl.String(), "attribute_id": pl.String(), "reason": pl.String()}
)

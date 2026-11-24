from uuid import UUID

from finguard.platform.ids import new_id


def test_ids_are_stdlib_uuid7() -> None:
    message_id = new_id()

    assert isinstance(message_id, UUID)
    assert message_id.version == 7


def test_ids_are_time_ordered() -> None:
    ids = [new_id() for _ in range(1_000)]

    assert ids == sorted(ids)

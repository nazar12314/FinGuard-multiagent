import pytest

from finguard.core.auth import DevHeaderAuthenticator, Unauthenticated
from finguard.platform.identity import Principal, Role

USER_ID = "0196d5a0-0000-7000-8000-000000000001"


async def test_dev_headers_identify_the_caller_and_their_roles() -> None:
    principal = await DevHeaderAuthenticator().authenticate(
        {"X-Dev-User-Id": USER_ID, "X-Dev-Roles": "analyst, senior_reviewer"}
    )

    assert principal == Principal(
        user_id=principal.user_id, roles=frozenset({Role.ANALYST, Role.SENIOR_REVIEWER})
    )
    assert str(principal.user_id) == USER_ID


async def test_a_caller_without_roles_is_authenticated_with_none() -> None:
    principal = await DevHeaderAuthenticator().authenticate({"X-Dev-User-Id": USER_ID})

    assert principal.roles == frozenset()


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({}, "missing X-Dev-User-Id"),
        ({"X-Dev-User-Id": "alice"}, "must be a UUID"),
        ({"X-Dev-User-Id": USER_ID, "X-Dev-Roles": "analyst,root"}, "accepts only"),
    ],
    ids=["missing-user", "malformed-user", "unknown-role"],
)
async def test_rejects_unusable_dev_headers(headers: dict[str, str], message: str) -> None:
    with pytest.raises(Unauthenticated, match=message):
        await DevHeaderAuthenticator().authenticate(headers)

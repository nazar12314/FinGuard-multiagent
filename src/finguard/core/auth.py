from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

from finguard.platform.errors import ApplicationError
from finguard.platform.identity import Principal, Role


class Unauthenticated(ApplicationError):
    code = "unauthenticated"


class Authenticator(Protocol):
    async def authenticate(self, headers: Mapping[str, str]) -> Principal:
        """Return the caller, or raise Unauthenticated."""
        ...


class DevHeaderAuthenticator:
    """Trusts the identity the client sends. Settings refuse it when env=prod."""

    user_id_header = "X-Dev-User-Id"
    roles_header = "X-Dev-Roles"

    async def authenticate(self, headers: Mapping[str, str]) -> Principal:
        raw_user_id = headers.get(self.user_id_header)
        if not raw_user_id:
            raise Unauthenticated(f"missing {self.user_id_header} header")
        try:
            user_id = UUID(raw_user_id)
        except ValueError:
            raise Unauthenticated(f"{self.user_id_header} must be a UUID") from None

        names = [name.strip() for name in headers.get(self.roles_header, "").split(",")]
        try:
            roles = frozenset(Role(name) for name in names if name)
        except ValueError:
            allowed = ", ".join(Role)
            raise Unauthenticated(f"{self.roles_header} accepts only: {allowed}") from None

        return Principal(user_id=user_id, roles=roles)

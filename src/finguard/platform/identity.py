from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class Role(StrEnum):
    ANALYST = "analyst"
    SENIOR_REVIEWER = "senior_reviewer"
    ADMIN = "admin"


# Lives in the shared kernel because application services, not routes, enforce roles.
@dataclass(frozen=True, slots=True)
class Principal:
    user_id: UUID
    roles: frozenset[Role]

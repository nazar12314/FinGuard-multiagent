from uuid import UUID

from uuid_utils.compat import uuid7


# Time-ordered ids keep Postgres primary-key indexes append-mostly. Python 3.14 has
# uuid.uuid7() in the standard library; until then uuid-utils provides it.
def new_id() -> UUID:
    return uuid7()

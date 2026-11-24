from contextlib import AsyncExitStack
from dataclasses import dataclass

from finguard.core.auth import Authenticator, DevHeaderAuthenticator
from finguard.core.settings import AuthSettings, Settings


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    authenticator: Authenticator


def build_authenticator(settings: AuthSettings) -> Authenticator:
    # Exhaustive over the Literal: mypy reports a missing return once a new mode is added.
    match settings.mode:
        case "dev_header":
            return DevHeaderAuthenticator()


async def build_api_container(settings: Settings, resources: AsyncExitStack) -> Container:
    """Wire the API process. Every client opened here is registered on `resources`,
    which the caller owns, so it is closed even if a later step fails."""
    return Container(settings=settings, authenticator=build_authenticator(settings.auth))

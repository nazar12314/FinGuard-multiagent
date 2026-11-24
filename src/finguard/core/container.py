from contextlib import AsyncExitStack
from dataclasses import dataclass

from finguard.core.auth import Authenticator, DevHeaderAuthenticator
from finguard.core.settings import Settings


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    resources: AsyncExitStack  # every client opened while building is closed on shutdown
    authenticator: Authenticator


async def build_api_container(settings: Settings) -> Container:
    return Container(
        settings=settings,
        resources=AsyncExitStack(),
        authenticator=DevHeaderAuthenticator(),
    )

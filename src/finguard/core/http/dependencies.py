from typing import Annotated, cast

from fastapi import Depends, Request

from finguard.core.container import Container
from finguard.platform.identity import Principal


def get_container(request: Request) -> Container:
    return cast(Container, request.app.state.container)


ContainerDep = Annotated[Container, Depends(get_container)]


async def get_principal(request: Request, container: ContainerDep) -> Principal:
    return await container.authenticator.authenticate(request.headers)


CurrentPrincipal = Annotated[Principal, Depends(get_principal)]

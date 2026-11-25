import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from finguard.core.container import Container, build_api_container
from finguard.core.http import diagnostics
from finguard.core.http.errors import install_exception_handlers
from finguard.core.http.middleware import RequestContextMiddleware
from finguard.core.logging import LogEvent, get_logger
from finguard.core.ops import ops_routes
from finguard.core.settings import Settings

ContainerFactory = Callable[[Settings, AsyncExitStack], Awaitable[Container]]

logger = get_logger(__name__)


def create_app(
    settings: Settings, *, container_factory: ContainerFactory = build_api_container
) -> FastAPI:
    app_version = version("finguard")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info(LogEvent.APP_STARTING, env=settings.env, version=app_version)
        async with AsyncExitStack() as resources:  # closes clients in reverse order of opening
            app.state.container = await container_factory(settings, resources)
            yield
        logger.info(LogEvent.APP_STOPPED)

    app = FastAPI(
        title=settings.service_name,
        version=app_version,
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )
    app.add_middleware(RequestContextMiddleware)
    app.router.routes.extend(ops_routes(lambda request: request.app.state.container.readiness))
    if settings.dev_endpoints_enabled:
        app.include_router(diagnostics.router)
    install_exception_handlers(app)
    # One server span per request; the per-message receive and send spans are only noise.
    FastAPIInstrumentor.instrument_app(
        app,
        excluded_urls=_exact_paths(
            "/docs", "/docs/oauth2-redirect", "/openapi.json", "/healthz", "/readyz", "/metrics"
        ),
        exclude_spans=["receive", "send"],
    )
    return app


def _exact_paths(*paths: str) -> str:
    # The instrumentation searches each pattern anywhere in the URL (scheme://host/path,
    # without the query), so a bare "docs" would also hide /cases/docs.
    return ",".join(rf"^[a-z]+://[^/]+{re.escape(path)}$" for path in paths)

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from app.config import get_settings
from app.logging import configure_logging
from app.mcp_server import mcp

settings = get_settings()
configure_logging(settings.log_level)

_log = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with mcp.session_manager.run():
        yield


app = FastAPI(title="Namhya Unicommerce MCP", version="0.2.0", lifespan=lifespan, redirect_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    _log.debug(">>> %s %s headers=%s", request.method, request.url.path, dict(request.headers))
    response = await call_next(request)
    _log.info(">>> %s %s %s", request.method, request.url.path, response.status_code)
    return response


app.mount("/mcp", mcp.streamable_http_app())   # Claude Code / API clients
app.mount("/sse", mcp.sse_app())               # Claude Desktop


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "environment": settings.app_env}


class _MCPPathFix:
    """Rewrite /mcp → /mcp/ internally so claude.ai doesn't get a 307 redirect."""
    def __init__(self, inner) -> None:
        self._inner = inner

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") == "http" and scope.get("path") == "/mcp":
            scope = {**scope, "path": "/mcp/", "raw_path": b"/mcp/"}
        await self._inner(scope, receive, send)


app = _MCPPathFix(app)

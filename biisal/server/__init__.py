# Â© agrprojects

import re

from aiohttp import web
from .stream_routes import routes


_PROTECTED_MEDIA_PREFIXES = (
    "/prepare/",
    "/api/generate/",
    "/api/download/",
    "/api/telegram/",
    "/watch/",
    "/thumb/",
)
_DIRECT_MEDIA_PATH = re.compile(r"^/(?:[A-Za-z0-9_-]{6})?\d+(?:/|$)")

_CORS_PREFIXES = ("/api/",)


def _apply_cors(headers, origin):
    headers["Access-Control-Allow-Origin"] = origin or "*"
    headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    headers["Access-Control-Allow-Headers"] = "*"
    headers["Access-Control-Max-Age"] = "86400"
    headers["Vary"] = "Origin"


@web.middleware
async def cors_headers(request: web.Request, handler):
    """Allow the PWA (a different origin) to fetch the /api/* JSON endpoints.

    The media stream/download links load inside an iframe (same-origin to the
    embed) so they never needed CORS; the Telegram delivery call is a real
    cross-origin fetch() and must receive Access-Control-Allow-Origin."""
    needs_cors = request.path.startswith(_CORS_PREFIXES)
    origin = request.headers.get("Origin", "*")

    if needs_cors and request.method == "OPTIONS":
        resp = web.Response(status=204)
        _apply_cors(resp.headers, origin)
        return resp

    try:
        resp = await handler(request)
    except web.HTTPException as exc:
        if needs_cors:
            _apply_cors(exc.headers, origin)
        raise
    if needs_cors:
        _apply_cors(resp.headers, origin)
    return resp


@web.middleware
async def require_media_access_code(request: web.Request, handler):
    """Reject protected page/API requests before route code can run.

    The individual handlers also validate the code against Supabase. This
    middleware is intentionally limited to the protected route families so
    public health, favicon, robots, and admin routes remain available while a
    stale or accidentally changed handler cannot render a media page without
    a code.
    """
    is_protected_route = request.path.startswith(_PROTECTED_MEDIA_PREFIXES)
    is_direct_media_path = bool(_DIRECT_MEDIA_PATH.match(request.path))
    if is_protected_route or is_direct_media_path:
        access_code = request.rel_url.query.get("access_code", "").strip()
        if not access_code:
            raise web.HTTPForbidden(
                text="This link requires an access_code.",
                content_type="text/plain",
            )
    return await handler(request)


async def web_server():
    web_app = web.Application(
        client_max_size=30000000,
        middlewares=[cors_headers, require_media_access_code],
    )
    web_app.add_routes(routes)
    return web_app

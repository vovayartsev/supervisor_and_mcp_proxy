from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send


class TokenAuthMiddleware:
    """Allows /mcp* iff ?token=... or `Authorization: Bearer ...` matches.
    /healthz is always allowed.
    """

    def __init__(self, app: ASGIApp, token: str | None) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.token:
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path == "/healthz" or path.startswith("/healthz/"):
            await self.app(scope, receive, send)
            return
        if not (path == "/mcp" or path.startswith("/mcp/") or path.startswith("/mcp?")):
            await self.app(scope, receive, send)
            return

        if self._authorized(scope):
            await self.app(scope, receive, send)
            return

        await _send_401(send)

    def _authorized(self, scope: Scope) -> bool:
        qs = scope.get("query_string", b"").decode("latin-1")
        for part in qs.split("&"):
            if part.startswith("token="):
                if part[len("token="):] == self.token:
                    return True
        for name, val in scope.get("headers", []):
            if name == b"authorization":
                v = val.decode("latin-1")
                if v.startswith("Bearer ") and v[7:] == self.token:
                    return True
        return False


async def _send_401(send: Send) -> None:
    await send({
        "type": "http.response.start",
        "status": 401,
        "headers": [(b"content-type", b"application/json")],
    })
    await send({"type": "http.response.body", "body": b'{"error":"unauthorized"}'})

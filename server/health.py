from starlette.requests import Request
from starlette.responses import PlainTextResponse


def register_healthchecks(mcp):
    @mcp.custom_route("/_liveness", methods=["GET"])
    async def liveness_check(_: Request) -> PlainTextResponse:
        """
        Liveness check endpoint to verify server status.
        """
        return PlainTextResponse("OK", status_code=200)

    @mcp.custom_route("/_readiness", methods=["GET"])
    async def readiness_check(_: Request) -> PlainTextResponse:
        """
        Readiness check endpoint to verify server is ready to handle requests.
        """
        return PlainTextResponse("READY", status_code=200)

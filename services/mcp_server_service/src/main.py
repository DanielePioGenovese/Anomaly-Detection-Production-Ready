"""
main.py
--------
Boots the MCP server.

MODE env var:
  "rest"   → FastAPI REST only (default — used by MCP client via HTTP)
  "mcp"    → FastMCP stdio/SSE (used by LangGraph MCP tool protocol)
  "both"   → FastAPI + FastMCP SSE mounted on same uvicorn app
"""

import logging
import os

import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("main")


def start_rest() -> None:
    from src.config import get_config
    from src.api import app

    cfg = get_config()
    scfg = cfg["mcp_server"]
    uvicorn.run(
        app,
        host=scfg["host"],
        port=scfg["port"],
        log_level=scfg["log_level"].lower(),
    )


def start_both() -> None:
    """Mount the FastMCP SSE app onto the FastAPI app and serve together."""
    from src.config import get_config
    from src.api import app as rest_app
    from src.server import mcp

    cfg = get_config()
    scfg = cfg["mcp_server"]

    # Mount MCP SSE transport at /mcp
    mcp_app = mcp.sse_app()
    rest_app.mount("/mcp", mcp_app)
    logger.info("FastMCP SSE mounted at /mcp")

    uvicorn.run(
        rest_app,
        host=scfg["host"],
        port=scfg["port"],
        log_level=scfg["log_level"].lower(),
    )


def start_mcp_stdio() -> None:
    """Run FastMCP over stdio (for local MCP client testing)."""
    from src.server import mcp
    mcp.run()


if __name__ == "__main__":
    mode = os.getenv("MODE", "both").lower()
    logger.info("Starting MCP Server in MODE=%s", mode)

    if mode == "rest":
        start_rest()
    elif mode == "mcp":
        start_mcp_stdio()
    elif mode == "both":
        start_both()
    else:
        raise ValueError(f"Unknown MODE='{mode}'. Use 'rest', 'mcp', or 'both'.")
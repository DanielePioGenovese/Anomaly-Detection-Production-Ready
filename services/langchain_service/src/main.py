"""
main.py
--------
Entry point for the MCP Client container.

MODE env var controls what starts:
  - "api"      → FastAPI server only  (default)
  - "consumer" → anomaly consumer only
  - "both"     → API + consumer as threads (convenient for local dev)
"""

import logging
import os
import threading

import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("main")


def start_api() -> None:
    from src.config import get_config
    from src.api import app

    cfg = get_config()
    uvicorn.run(
        app,
        host=cfg["mcp_client"]["host"],
        port=cfg["mcp_client"]["port"],
        log_level=cfg["mcp_client"]["log_level"].lower(),
    )


def start_consumer() -> None:
    from src.anomaly_consumer import run
    run()


if __name__ == "__main__":
    mode = os.getenv("MODE", "api").lower()
    logger.info("Starting MCP Client in MODE=%s", mode)

    if mode == "api":
        start_api()

    elif mode == "consumer":
        start_consumer()

    elif mode == "both":
        # Run consumer in a background thread; API in the main thread
        t = threading.Thread(target=start_consumer, daemon=True, name="anomaly-consumer")
        t.start()
        logger.info("Anomaly consumer thread started.")
        start_api()

    else:
        raise ValueError(f"Unknown MODE='{mode}'. Use 'api', 'consumer', or 'both'.")
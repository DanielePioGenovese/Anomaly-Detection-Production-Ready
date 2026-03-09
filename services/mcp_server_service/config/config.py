import os
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.yaml"


@lru_cache(maxsize=1)
def get_config() -> dict:
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    _env_overrides(cfg)
    return cfg


def _env_overrides(cfg: dict) -> None:
    if url := os.getenv("QDRANT_URL"):
        cfg["qdrant"]["url"] = url
    if key := os.getenv("QDRANT_API_KEY"):
        cfg["qdrant"]["api_key"] = key
    if col := os.getenv("QDRANT_COLLECTION"):
        cfg["qdrant"]["collection_name"] = col
    if key := os.getenv("GOOGLE_API_KEY"):
        cfg["embeddings"]["dense"]["google_api_key"] = key
    if port := os.getenv("MCP_SERVER_PORT"):
        cfg["mcp_server"]["port"] = int(port)
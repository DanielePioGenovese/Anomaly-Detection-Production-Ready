"""
api.py
-------
FastAPI REST wrapper that exposes the retrieval pipeline over HTTP.

This is the REST interface the MCP Client (tools.py) calls.
The FastMCP server (server.py) runs alongside this on a separate mount.

Endpoints:
  POST /retrieve   → run hybrid search + rerank
  GET  /health     → liveness + Qdrant check
"""

import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .models import HealthResponse, RetrieveRequest, RetrieveResponse
from .retriever import health_check, retrieve
from .config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(
    title="MCP Server — RAG Retrieval API",
    description="Hybrid search (dense + sparse) with cross-encoder reranking over Qdrant",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    qdrant_ok = health_check()
    cfg = get_config()
    return HealthResponse(
        status="ok" if qdrant_ok else "degraded",
        collection=cfg["qdrant"]["collection_name"],
        qdrant_ok=qdrant_ok,
    )


@app.post("/retrieve", response_model=RetrieveResponse)
async def retrieve_endpoint(request: RetrieveRequest) -> RetrieveResponse:
    """
    Run the full retrieval pipeline:
      query → dense+sparse embed → hybrid search → RRF → rerank → contexts
    """
    logger.info("POST /retrieve | query='%s' top_k=%d", request.query[:80], request.top_k)

    try:
        texts, scores = retrieve(query=request.query, top_k=request.top_k)
    except Exception as exc:
        logger.exception("Retrieval failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return RetrieveResponse(
        contexts=texts,
        scores=scores,
        total_found=len(texts),
    )
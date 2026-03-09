"""
api.py
-------
FastAPI server that exposes the MCP Client agent.

Endpoints
---------
POST /investigate   — trigger an anomaly investigation
GET  /health        — liveness probe
GET  /thread/{id}   — retrieve a previous thread's state
"""

import logging

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .agent import MCPClientAgent
from .models import AnomalyInvestigationRequest, AnomalyInvestigationResponse, AgentMessage

from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


# ── Singleton agent (initialised once at startup) ─────────────────────────────

_agent: MCPClientAgent | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _agent
    logger.info("Initialising MCPClientAgent …")
    _agent = MCPClientAgent()

    yield

    logger.info("MCPClientAgent ready.")

app = FastAPI(
    title="MCP Client",
    description="LangGraph agent for anomaly investigation via MCP Server tools",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_agent() -> MCPClientAgent:
    if _agent is None:
        raise HTTPException(status_code=503, detail="Agent not yet initialised")
    return _agent


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "mcp-client"}


@app.post("/investigate", response_model=AnomalyInvestigationResponse)
async def investigate(request: AnomalyInvestigationRequest) -> AnomalyInvestigationResponse:
    """
    Run a full anomaly investigation.
    Called by the anomaly consumer when `anomaly=True` arrives on RedPanda.
    """
    agent = get_agent()

    try:
        result = agent.investigate(
            entity_id=request.entity_id,
            prediction_score=request.prediction_score,
            features=request.features,
            metadata=request.metadata,
            thread_id=request.thread_id,
        )
    except Exception as exc:
        logger.exception("Investigation failed for entity_id=%s", request.entity_id)
        raise HTTPException(status_code=500, detail=str(exc))

    return AnomalyInvestigationResponse(
        thread_id=result["thread_id"],
        entity_id=result["entity_id"],
        conclusion=result["conclusion"],
        messages=[AgentMessage(**m) for m in result["messages"]],
        retrieved_contexts=result["retrieved_contexts"],
        status=result["status"],
    )
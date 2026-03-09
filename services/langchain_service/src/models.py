from pydantic import BaseModel, Field
from typing import Any, Optional
from enum import Enum


# ── Incoming prediction event from RedPanda ─────────────────────────────────

class PredictionEvent(BaseModel):
    """Schema for messages arriving on the 'predictions' RedPanda topic."""
    entity_id: str                          # e.g. user_id / transaction_id
    timestamp: str
    prediction_score: float
    anomaly: bool
    features: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── MCP Client API request / response ───────────────────────────────────────

class AnomalyInvestigationRequest(BaseModel):
    """Payload the anomaly consumer sends to the MCP client API."""
    entity_id: str
    prediction_score: float
    features: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)
    thread_id: Optional[str] = None         # optional: resume an existing thread


class AgentMessage(BaseModel):
    role: str                               # "user" | "assistant" | "tool"
    content: str


class AnomalyInvestigationResponse(BaseModel):
    thread_id: str
    entity_id: str
    conclusion: str                         # final agent answer
    messages: list[AgentMessage]
    retrieved_contexts: list[str] = Field(default_factory=list)
    status: str = "completed"               # completed | error


# ── MCP Server tool schemas ──────────────────────────────────────────────────

class RetrieveContextRequest(BaseModel):
    query: str
    top_k: int = 5


class RetrieveContextResponse(BaseModel):
    contexts: list[str]
    scores: list[float]


# ── Agent state (LangGraph) ──────────────────────────────────────────────────

class AgentState(BaseModel):
    messages: list[dict[str, Any]] = Field(default_factory=list)
    entity_id: str = ""
    prediction_score: float = 0.0
    features: dict[str, Any] = Field(default_factory=dict)
    retrieved_contexts: list[str] = Field(default_factory=list)
    iteration: int = 0
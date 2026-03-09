"""
server.py
----------
FastMCP server that exposes retrieval as an MCP tool.

This is what the LangGraph agent in mcp_client calls when it uses
the MCP protocol directly (as opposed to the REST /retrieve endpoint).

Tool exposed:
  - retrieve_context(query, top_k) → list of context passages
"""

import logging
from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from .retriever import retrieve

logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="anomaly-rag-server",
    instructions=(
        "This MCP server provides retrieval-augmented generation context "
        "for anomaly investigation. Use the retrieve_context tool to fetch "
        "relevant passages from the knowledge base."
    ),
)


@mcp.tool()
def retrieve_context(
    query: Annotated[str, Field(description="Natural-language search query")],
    top_k: Annotated[int, Field(description="Number of results to return", ge=1, le=20)] = 5,
) -> dict:
    """
    Retrieve relevant context passages from the knowledge base.

    Uses hybrid search (dense + sparse) with cross-encoder reranking.
    Returns the most relevant document chunks for the given query.
    """
    logger.info("[MCP] retrieve_context called | query='%s' top_k=%d", query[:80], top_k)

    texts, scores = retrieve(query=query, top_k=top_k)

    if not texts:
        return {"contexts": [], "scores": [], "message": "No results found"}

    return {
        "contexts": texts,
        "scores": [round(s, 6) for s in scores],
        "total": len(texts),
    }
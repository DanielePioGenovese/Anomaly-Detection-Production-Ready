FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /mcp_server_service

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev --group mcp-server

COPY services/mcp_server_service/src ./src/

ENV PYTHONPATH="/mcp_server_service"
ENV PATH="/mcp_server_service/.venv/bin:$PATH"

CMD ["uv", "run", "src.server"]
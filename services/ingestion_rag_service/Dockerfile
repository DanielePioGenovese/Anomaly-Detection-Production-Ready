FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_HTTP_TIMEOUT=300

WORKDIR /ingestion_rag_service

COPY pyproject.toml uv.lock ./

# Cache mount persists the uv download cache on the host across builds
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --group ingestion-rag

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY services/ingestion_rag_service/ ./

CMD ["uv", "run", "-m", "src.ingestion"]
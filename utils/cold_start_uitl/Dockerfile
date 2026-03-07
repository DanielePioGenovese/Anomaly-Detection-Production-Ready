FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --group cold-start

COPY utils/services/feature_store_service/src /cold_start_util/feature_store_service

CMD ["uv", "run", "-m", "cold_start"]

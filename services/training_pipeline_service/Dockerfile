FROM python:3.11.9-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# caching: prima manifest/lock
COPY pyproject.toml uv.lock* ./

# installa SOLO training group
RUN uv sync --frozen --no-dev --group training

# poi copia il codice
COPY src/training_pipeline /app/training_pipeline

CMD ["uv", "run", "/app/training_pipeline/train.py"]
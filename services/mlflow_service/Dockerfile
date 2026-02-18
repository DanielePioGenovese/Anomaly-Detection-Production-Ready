FROM python:3.11-slim

WORKDIR /app

# Install uv (binario)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copia solo i manifest per caching
COPY pyproject.toml uv.lock ./

# Installa SOLO il gruppo mlflow-server
# Usa il python di sistema (3.11) per creare il venv
ENV UV_PYTHON=python3.11
RUN uv sync --no-dev --group mlflow-server

# Cartelle persistenti montate a runtime
RUN mkdir -p /mlflow/artifacts

EXPOSE 5000

ENV PATH="/app/.venv/bin:$PATH"

# Il comando di start è definito nel compose per flessibilità, 
# ma qui mettiamo un default coerente
CMD ["uv", "run", "mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]

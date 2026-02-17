# Dockerfile for MLflow Tracking Server
# Build: docker build -f Dockerfile.mlflow -t mlflow-server:latest .
# Run: docker run -p 5000:5000 -v $(pwd)/mlruns:/mlruns mlflow-server:latest

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set working directory
WORKDIR /mlflow

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow and dependencies
RUN pip install --no-cache-dir \
    mlflow==2.10.2 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    psycopg2-binary==2.9.9

# Create directory for mlruns (MLflow artifacts)
RUN mkdir -p /mlruns

# Expose MLflow UI port
EXPOSE 5000

# Set environment variables
ENV MLFLOW_BACKEND_STORE_URI=file:///mlruns
ENV MLFLOW_DEFAULT_ARTIFACT_ROOT=/mlruns/artifacts
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000').read()" || exit 1

# Start MLflow server
CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--backend-store-uri", "file:///mlruns", \
     "--default-artifact-root", "/mlruns/artifacts"]
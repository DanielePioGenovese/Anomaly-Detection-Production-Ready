# Use NVIDIA CUDA base image
FROM nvidia/cuda:12.1.0-base-ubuntu22.04

# Avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.12 via deadsnakes repository
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    python3-pip \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.12 as default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1 \
    && update-alternatives --set python3 /usr/bin/python3.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

# Install UV package manager
RUN pip install --no-cache-dir uv

WORKDIR /ingestion_rag_service

# Configure model and cache directories
ENV HF_HOME="/ingestion_rag_service/models/huggingface"
ENV FASTEMBED_CACHE_PATH="/ingestion_rag_service/models/fastembed"

RUN mkdir -p $HF_HOME $FASTEMBED_CACHE_PATH

# Copy dependency management files
COPY pyproject.toml uv.lock ./

# Sync dependencies with uv (enforcing Python 3.12)
RUN uv sync --frozen --no-dev --group ingestion-rag --python 3.12

# Copy source code
COPY services/ingestion_rag_service/src/ ./src/
COPY services/ingestion_rag_service/config/ ./config/

# Define container entry point
ENTRYPOINT ["uv", "run", "--python", "3.12", "-m", "src.ingestion"]
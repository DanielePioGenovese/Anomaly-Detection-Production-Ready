# Ingestion RAG Service

## Overview

One-shot service that loads knowledge-base documents, splits them into chunks, embeds them using a **hybrid dense + sparse strategy**, and indexes them into a Qdrant collection. It runs once to populate the vector store that the MCP server queries at investigation time.

## File Structure

```
services/ingestion_rag_service/
├── Dockerfile
├── config/
│   ├── __init__.py         # Exports Config and ingestion_settings
│   └── config.py           # Pydantic settings (Qdrant URL, model names, paths)
└── src/
    └── ingestion.py        # Load → split → embed → index pipeline
```

## Base Image

```
nvidia/cuda:12.6.0-cudnn-runtime-ubuntu24.04
```

Requires an NVIDIA GPU (`runtime: nvidia` in `compose.yaml`). The dense embedding model (`BAAI/bge-m3`) is loaded onto CUDA at runtime — CPU fallback is not configured.

PyTorch is installed from the CUDA 12.6 wheel index:
```
https://download.pytorch.org/whl/cu126
```

## Pipeline

```
rag_files/*.txt
      │
      ▼
  TextLoader  (UTF-8, metadata: source, path, doc_type=kb_txt)
      │
      ▼
  RecursiveCharacterTextSplitter
      chunk_size=900, chunk_overlap=150
      separators: ["\n\n", "\n", ". ", " ", ""]
      │
      ▼
  ┌─────────────────────────────────────┐
  │  Dense embeddings                   │
  │  BAAI/bge-m3  (HuggingFace, CUDA)  │
  ├─────────────────────────────────────┤
  │  Sparse embeddings                  │
  │  Qdrant/bm25  (FastEmbed)           │
  └─────────────────────────────────────┘
      │
      ▼
  QdrantVectorStore  (HYBRID mode, gRPC, force_recreate=True)
      collection: ingestion_rag_service
```

## Hybrid Retrieval Strategy

| Embedding | Model | Purpose |
|---|---|---|
| Dense | `BAAI/bge-m3` | Semantic similarity — understands meaning and context |
| Sparse | `Qdrant/bm25` | Keyword matching — precise term recall |

`RetrievalMode.HYBRID` combines both signals, making the vector store robust to both semantic queries ("what causes bearing wear?") and exact-term queries ("Current_Imbalance_Ratio threshold").

> `force_recreate=True` — the collection is **dropped and rebuilt on every run**. Re-run this service any time the knowledge base files in `rag_files/` change.

## Configuration (`config.py`)

| Setting | Default | Description |
|---|---|---|
| `qdrant_url` | `http://qdrant:6334` | Qdrant gRPC endpoint |
| `qdrant_collection` | `ingestion_rag_service` | Collection name (must match MCP server config) |
| `qdrant_api_key` | `None` | Set via `QDRANT_API_KEY` env var if auth is enabled |
| `data_dir` | `/ingestion_rag_service/rag_files` | Source directory for `.txt` knowledge-base files |
| `embedding_model` | `BAAI/bge-m3` | HuggingFace dense embedding model |

## Model Cache Paths

| Model | Cache path inside container |
|---|---|
| HuggingFace (`bge-m3`) | `/ingestion_rag_service/models/hugging_face` |
| FastEmbed (`bm25`) | `/ingestion_rag_service/models/fastembed` |

Both paths are bind-mounted from `./local_models` in `compose.yaml` so models are downloaded once and reused across runs.

## Knowledge Base (`rag_files/`)

Located at the **project root**, not inside the service directory. Mounted read-only into the container at `/ingestion_rag_service/rag_files`.

```
rag_files/
├── machine_1.txt   # Milnor M-Series — deployed June 2021 — High criticality
├── machine_2.txt   # Girbau GENESIS/HS — deployed March 2023 — Medium criticality
└── machine_3.txt   # Milnor M-Series — deployed January 2022 — Critical
```

Each file is a **per-machine knowledge document** written for the LLM agent (addressed as "Gemma"). It contains:
- Model reference and deployment date
- Criticality level and role in the production line
- Dated incident log with root causes and operator notes
- Maintenance context (known quirks, failure patterns)
- A "Pro-Tip for Gemma" — heuristic rules to guide the agent's anomaly diagnosis

These files are the domain knowledge the MCP server retrieves when the agent investigates an anomaly. They must be kept up to date as machines are serviced or incidents occur. **Re-run this service after any file change** to rebuild the Qdrant collection.

## Volumes (from `compose.yaml`)

| Host path | Container path | Mode |
|---|---|---|
| `./rag_files` | `/ingestion_rag_service/rag_files` | read-only |
| `./local_models` | `/ingestion_rag_service/models` | read-write |

## Build & Run

```bash
# Build
docker build -f services/ingestion_rag_service/Dockerfile -t ingestion_rag_service:latest .

# Run (requires NVIDIA GPU)
docker compose run --rm ingestion_rag
```

Re-run whenever `rag_files/` content changes to rebuild the Qdrant collection.
# Feature Store Service

## Overview

Feast-based feature store that acts as the **central feature registry and serving layer** for the anomaly detection system. It defines all entities, data sources, feature views, and the feature service contract consumed by the inference pipeline. It exposes features to online inference via Redis and to offline training via Parquet.

## File Structure

```
services/feature_store_service/
├── Dockerfile
└── src/
    ├── feature_store.yaml      # Project config: registry, online/offline store backends
    ├── entity.py               # Machine entity definition
    ├── data_sources.py         # FileSource (batch) and PushSource (streaming) definitions
    ├── features.py             # FeatureView definitions (schema + TTL + source binding)
    └── feature_services.py     # FeatureService — versioned feature contract for the model
```

## Infrastructure

| Backend | Technology | Config |
|---|---|---|
| Online store | Redis | `redis:6379`, key TTL: 24 h |
| Offline store | File (Parquet) | Local filesystem |
| Registry | SQLite | `/data/registry/registry.db` |
| Provider | Local | — |

## Entity

| Entity | Join key | Type | Description |
|---|---|---|---|
| `machine` | `Machine_ID` | INT64 | Unique identifier for each washing machine |

## Data Sources

### Batch source
| Name | Type | Path | Written by |
|---|---|---|---|
| `washing_batch_source` | `FileSource` | `/data/offline/machines_batch_features` | PySpark batch pipeline (daily) |

### Streaming sources (PushSource + offline backfill)

| Push source | Backing FileSource path | Used by |
|---|---|---|
| `vibration_push_source` | `/data/offline/streaming_backfill/vibration/` | `machine_streaming_features_10m` |
| `current_push_source` | `/data/offline/streaming_backfill/current/` | `machine_streaming_features_5min` |

Each `PushSource` has a dedicated `FileSource` backing store so point-in-time joins during training still work. The streaming pipeline calls `POST /push` with the source name to write directly into Redis and optionally to the offline backfill.

## Feature Views

### `machine_streaming_features_10m`
| Field | Type | TTL | Source |
|---|---|---|---|
| `Vibration_RollingMax_10min` | Float32 | 15 min (10 min window + 5 min grace) | `vibration_push_source` |

### `machine_streaming_features_5min`
| Field | Type | TTL | Source |
|---|---|---|---|
| `Current_Imbalance_Ratio` | Float32 | 8 min (5 min window + 3 min grace) | `current_push_source` |
| `Current_Imbalance_RollingMean_5min` | Float32 | 8 min | `current_push_source` |

### `machine_batch_features`
| Field | Type | TTL | Source |
|---|---|---|---|
| `Daily_Vibration_PeakMean_Ratio` | Float32 | 3 years* | `washing_batch_source` |

> *TTL is intentionally large for cold-start debugging; production value should be ~7 days.

### Why a separate PushSource per window?
- Each view has an **independent TTL** matched to its window length — short TTLs for streaming, long for batch
- The streaming pipeline can push partial records per window with **no stateful merge** step
- Offline backfill paths are isolated, keeping **point-in-time joins clean**
- Ownership is explicit: the 10-min vibration signal and the 5-min current signal are independent

## Feature Service

### `machine_anomaly_service_v1`

Versioned contract consumed by the inference pipeline. Combines all three views into a single feature vector:

```python
features=[
    machine_streaming_features_10m,    # Vibration_RollingMax_10min
    machine_streaming_features_5min,   # Current_Imbalance_Ratio, Current_Imbalance_RollingMean_5min
    machine_batch_features,            # Daily_Vibration_PeakMean_Ratio
]
```

Callers request features by service name, decoupling model code from the underlying view implementation.

## Compose Lifecycle

The service runs in two modes, both using the same image:

| Compose service | Profile | Command | When |
|---|---|---|---|
| `feature_store_apply` | `setup` | `feast apply` | One-time: registers all definitions in the registry |
| `feature_store_service` | `online` | `feast serve --host 0.0.0.0 --port 6566` | Persistent: serves features via HTTP on port `8000` (mapped from `6566`) |

```bash
# Register feature definitions (run once, or after any change to src/)
docker compose --profile setup run --rm feature_store_apply

# Start the feature server
docker compose --profile online up feature_store_service
```

## Build

```bash
docker build -f services/feature_store_service/Dockerfile -t feature_store_service:latest .
```
# Data Engineering Service

## Overview

PySpark service that transforms raw synthetic sensor data into a **feature-enriched dataset** ready for both the streaming pipeline and the batch pipeline. It sits between `create_datasets_service` (source) and `batch_feature_pipeline` + the streaming consumer (consumers), and must complete before either can run.

## File Structure

```
services/
├── dockerfile.spark_services                   # Shared image (batch, data_engineering, create_datasets)
└── data_engineering_service/
    ├── config/
    │   └── feature_engineering_config.yaml     # All feature definitions and processing settings
    └── src/
        └── data_engineering.py                 # FeatureEngineering class + main entry point
```

## What It Processes

Two input datasets → two enriched output datasets:

| Input | Output | Labels |
|---|---|---|
| `data/synthetic_datasets/industrial_washer_normal` | `data/processed_datasets/machines_batch_features` | No |
| `data/synthetic_datasets/industrial_washer_with_anomalies` | `data/processed_datasets/machines_with_anomalies_features` | Yes (`is_anomaly`) |

Output is written as **flat Parquet** (`part-*.parquet`, no `Machine_ID` subdirectories), sorted by `timestamp → Machine_ID`.

## Processing Pipeline

```
Read parquet
      ↓
Data quality checks  (null timestamps, duplicate machine+timestamp rows)
      ↓
Compute derived columns  (Current_Imbalance_Ratio)
      ↓
Apply streaming features  (time-based rolling windows per machine)
      ↓
Apply batch features      (daily aggregations joined back to every row)
      ↓
Write flat parquet (overwrite)
```

## Computed Features

### Derived Column (intermediate — not written to output directly)

| Column | Formula | Purpose |
|---|---|---|
| `Current_Imbalance_Ratio` | `(max(L1,L2,L3) − min(L1,L2,L3)) / mean(L1,L2,L3)` | Three-phase electrical imbalance signal; healthy motor < 0.02 |

---

### Streaming Features — short-term rolling windows

| Feature | Source | Window | Aggregation | Why |
|---|---|---|---|---|
| `Vibration_RollingMax_10min` | `Vibration_mm_s` | 10 min | max | Catches shock events without reacting to single noisy readings |
| `Current_Imbalance_RollingMean_5min` | `Current_Imbalance_Ratio` | 5 min | mean | Detects winding/bearing faults via electrical signal before vibration changes |

Windows are time-based and partitioned per `Machine_ID`.

---

### Batch Features — daily aggregations

| Feature | Formula | Aggregation type | Why |
|---|---|---|---|
| `Daily_Vibration_PeakMean_Ratio` | `max(Vibration_mm_s) / mean(Vibration_mm_s)` per machine per day | daily | Sustained high ratio across a full day = repeated shocks → mechanical deterioration |

> **First-period guard**: the first calendar day per machine is set to `null`. A machine observed for only a few hours cannot produce a meaningful daily ratio — leaving it populated would mislead the model.

## Data Quality Checks

| Check | Behaviour on failure |
|---|---|
| Null timestamps | Rows are filtered out with a warning |
| Duplicate `(timestamp, Machine_ID)` | Duplicates are dropped with a warning |
| Rolling max ≥ source value | Error logged if violated |
| Rolling mean within source range | Warning logged if out of bounds |
| Batch feature post-join validation | Warning logged |
| Final timestamp order | Summary table printed per machine |

## Configuration (`feature_engineering_config.yaml`)

Key sections:

```yaml
datasets:          # Input/output paths and label column per dataset
schema:            # timestamp_column, partition_columns (Machine_ID)
rolling_features:  # Streaming feature definitions (source, window, aggregation)
batch_features:    # Batch feature definitions (source, aggregation_type)
processing:        # write_mode, cache_intermediate, repartition
data_quality:      # Toggle individual checks on/off
spark_config:      # shuffle.partitions, adaptive query execution
```

New streaming or batch features can be added by appending entries under `rolling_features` or `batch_features` — no code changes required.

## Run

### Via Docker Compose
```bash
docker compose run --rm data_engineering
```

Invoked automatically after `create_datasets` completes (`condition: service_completed_successfully`).

### Single dataset (CLI)
```bash
uv run -m services.data_engineering_service.src.data_engineering \
  --dataset industrial_washer_normal
```

### All datasets (CLI)
```bash
uv run -m services.data_engineering_service.src.data_engineering \
  --config services/data_engineering_service/config/feature_engineering_config.yaml
```

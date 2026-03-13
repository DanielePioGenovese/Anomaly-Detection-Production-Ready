# Create Datasets Service

## Overview

One-shot PySpark service that synthetically generates **industrial washing machine sensor data** and saves it to Parquet. It is the first step in the data pipeline — nothing downstream can run until it completes successfully.

## File Structure

```
services/
├── dockerfile.spark_services               # Shared image (batch, data_engineering, create_datasets)
└── create_datasets_service/
    ├── config/
    │   ├── __init__.py                     # Exports all config constants
    │   └── config.py                       # Paths and generation parameters
    └── src/
        ├── industrial_washer_generator.py  # Core generator logic
        ├── example_usage.py                # Entry point (invoked by compose)
        ├── spark_configs.py                # Spark session presets
        └── test_generator.py               # Quick validation (10K rows)
```

> Uses the same `dockerfile.spark_services` image as `batch_pipeline_service` and `data_engineering_service`.

## What It Generates

Two datasets, both written to `data/synthetic_datasets/`:

| Dataset | Path | Description |
|---|---|---|
| Normal | `industrial_washer_normal/` | Clean sensor readings, no faults |
| Anomaly | `industrial_washer_with_anomalies/` | Same data with 2% injected faults + `is_anomaly` label |
| Normal (streaming) | `industrial_washer_normal_streaming/` | Continuation of normal timestamps for streaming sim |
| Anomaly (streaming) | `industrial_washer_with_anomalies_streaming/` | Streaming variant with anomalies |

CSV samples (first 10K rows) are also saved alongside each Parquet directory for quick inspection.

## Sensor Schema

| Column | Type | Description |
|---|---|---|
| `timestamp` | Timestamp | 1-second intervals, 3 machines report in sync |
| `Machine_ID` | Int | 1 – 3 |
| `Cycle_Phase_ID` | Int | 0=Idle, 1=Fill, 2=Heat, 3=Wash, 4=Rinse, 5=Spin, 6=Drain |
| `Current_L1/L2/L3` | Float | Phase currents (A) — vary by cycle phase |
| `Voltage_L_L` | Float | Line-to-line voltage (~400 V ±5%) |
| `Water_Temp_C` | Float | Water temperature (°C) |
| `Motor_RPM` | Float | Motor speed (0 when idle/filling) |
| `Water_Flow_L_min` | Float | Water flow rate (L/min) |
| `Vibration_mm_s` | Float | Vibration level (mm/s) — peaks during Spin |
| `Water_Pressure_Bar` | Float | Water pressure (bar) |
| `is_anomaly` | Int | 0/1 — anomaly dataset only |

## Injected Anomaly Types

| Type | Share of anomalies | Affected sensor |
|---|---|---|
| Overcurrent | ~35% | `Current_L1/L2/L3` > 50 A |
| Voltage fault | ~20% | `Voltage_L_L` drop (320–350 V) or spike (450–480 V) |
| Overheating | ~20% | `Water_Temp_C` 85–100 °C |
| Excessive vibration | ~15% | `Vibration_mm_s` 15–25 mm/s |
| Motor malfunction | ~10% | `Motor_RPM` wrong for current phase |

## Configuration (`config.py`)

| Constant | Default | Description |
|---|---|---|
| `DATASETS_PATH` | `data/synthetic_datasets` | Output root directory |
| `DEFAULT_NUM_ROWS` | `1_000_000` | Rows per generated dataset |
| `DEFAULT_ANOMALY_RATE` | `0.02` | Fraction of anomalous records |
| `NUM_MACHINES` | `10` | Number of simulated machines |
| `TIMESTAMP_START` | `2024-01-01 00:00:00` | Start time for training data |
| `TIMESTAMP_INTERVAL_SECONDS` | `1` | All machines report every second |

## Spark Session Presets (`spark_configs.py`)

| Function | Driver Memory | Use Case |
|---|---|---|
| `get_spark_local_dev()` | 2 GB | Quick testing (10K–100K rows) |
| `get_spark_local_prod()` | 4 GB | Full 1M row generation |
| `get_spark_high_memory()` | 8 GB | 5M+ rows |
| `get_spark_cluster()` | 4 GB | Distributed Spark cluster |
| `get_spark_minimal()` | 1 GB | Constrained environments |

## Run

### Via Docker Compose (standard)
```bash
docker compose run --rm create_datasets
```

`compose.yaml` invokes `example_usage.py` directly:
```yaml
command: ["-m", "services.create_datasets_service.src.example_usage"]
environment:
  SPARK_DRIVER_MEMORY: 2g
```

### Quick validation (before full run)
```bash
docker compose run --rm create_datasets \
  uv run python services/create_datasets_service/src/test_generator.py
```
Runs 7 automated checks on a 10K-row sample: row counts, anomaly rate, schema, data ranges, null values, anomaly magnitude, and timestamp uniqueness.

## Compose Dependencies

```
create_datasets  ──►  data_engineering  ──►  batch_feature_pipeline
   (completes)           (completes)
```

Both downstream services declare `condition: service_completed_successfully` on this service.
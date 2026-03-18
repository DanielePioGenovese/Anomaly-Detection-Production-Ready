# Training Service - Isolation Forest Anomaly Detection

## Overview

The **Training Service** is responsible for training the Isolation Forest anomaly detection model for washing machine fault prediction. This service loads historical data from the datalake (parquet files), trains a scikit-learn pipeline with preprocessing steps, and registers the model in MLflow for versioning and deployment.

**Key Features:**
- **Datalake-native**: Loads data directly from parquet files (no Feast dependency)
- **Memory-efficient**: Supports subsampling for training and chunked inference
- **Full MLflow integration**: Logs parameters, metrics, artifacts, and model signature
- **Production-ready pipeline**: Combines preprocessing (imputation, scaling, encoding) with IsolationForest

## Architecture

### Training Pipeline Flow

```
┌─────────────────────────────────┐
│  Datalake (Parquet Files)       │
│  /data/processed_datasets/...   │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  DataManager                    │
│  - Single file or directory     │
│  - Recursive parquet loading    │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Feature Engineering            │
│  - Numeric: Impute → Scale      │
│  - Categorical: Impute → OHE    │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  IsolationForest Training       │
│  - Subsampling (if needed)      │
│  - Chunked inference            │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  MLflow Logging                 │
│  - Parameters & Metrics         │
│  - Model + Signature            │
│  - Artifacts (metrics.json)     │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Model Registry                 │
│  Model Name: if_anomaly_detector│
└─────────────────────────────────┘
```

### Model Pipeline Components

```
sklearn.Pipeline
├── Step 1: Preprocessing (ColumnTransformer)
│   ├── Numeric Pipeline
│   │   ├── SimpleImputer (median strategy)
│   │   └── StandardScaler
│   └── Categorical Pipeline
│       ├── SimpleImputer (constant="missing")
│       └── OneHotEncoder (handle_unknown="ignore")
└── Step 2: IsolationForest
    ├── n_estimators (default: 100)
    ├── contamination (default: 0.2)
    └── random_state (default: 42)
```

## File Structure

```
training_service/
├── src/
│   ├── __init__.py              # Package initialization
│   ├── train.py                 # Main training script
│   ├── load_from_datalake.py    # DataManager class (parquet loading)
│   ├── model.py                 # ModelFactory (pipeline builder)
│   └── utils.py                 # MLflow signature helper
├── config/
│   ├── __init__.py
│   └── settings.py              # Pydantic configuration
├── dockerfile                   # Container definition
├── pyproject.toml              # uv dependencies
└── README.md                    # This file
```

## Components

### 1. `train.py` - Main Training Script

**Purpose**: Orchestrates the full training workflow

**Workflow:**
1. Load data from datalake (parquet files)
2. Sort chronologically and extract features
3. Build preprocessing + IsolationForest pipeline
4. Train with optional subsampling (memory protection)
5. Run chunked inference on full dataset
6. Compute anomaly statistics and score distributions
7. Log everything to MLflow

**Key Functions:**
- `main()`: End-to-end training pipeline

**MLflow Artifacts:**
- **Parameters**: contamination, n_estimators, dataset size, chunk size
- **Metrics**: anomaly_rate, score_mean/std, percentiles, latency
- **Model**: Full sklearn Pipeline with signature
- **Files**: metrics.json (detailed distribution stats)

### 2. `load_from_datalake.py` - Data Loading

**Purpose**: Load training data from parquet files

**Class: `DataManager`**
- **Method**: `load_data()` → pd.DataFrame
- **Supports**:
  - Single `.parquet` file
  - Directory with multiple parquet files (recursive)
- **Returns**: Concatenated DataFrame with all rows

### 3. `model.py` - Pipeline Factory

**Purpose**: Build scikit-learn preprocessing + model pipeline

**Class: `ModelFactory`**
- **Method**: `build_pipeline(num_cols, cat_cols, settings)` → sklearn.Pipeline

**Preprocessing Logic:**
- **Numeric features**:
  - Median imputation (robust to outliers)
  - StandardScaler (mean=0, std=1)
- **Categorical features** (e.g., Cycle_Phase_ID):
  - Constant imputation (fill_value="missing")
  - OneHotEncoder (handle_unknown="ignore")

**Model Configuration:**
- `n_estimators`: Number of trees (default: 100)
- `contamination`: Expected anomaly rate (default: 0.2 = 20%)
- `random_state`: Reproducibility seed (default: 42)
- `n_jobs`: -1 (use all CPU cores)

### 4. `utils.py` - MLflow Helpers

**Purpose**: Generate model signature for MLflow

**Function: `create_and_log_signature(x_sample, model_pipe)`**
- Infers input schema from raw DataFrame (not transformed features)
- Ensures production API can send JSON directly
- Returns `ModelSignature` for model registration

### 5. `settings.py` - Configuration

**Purpose**: Centralized configuration using Pydantic

**Key Settings:**
- **MLflow**:
  - `mlflow_tracking_uri`: "http://mlflow:5000"
  - `mlflow_experiment_name`: "isolation_forest_prod"
  - `mlflow_model_name`: "if_anomaly_detector"
- **Datalake**:
  - `entity_df_path`: Path to parquet file or directory
  - `event_timestamp_column`: "timestamp"
- **Processing**:
  - `max_fit_rows`: 50,000 (subsampling cap)
  - `inference_chunk_size`: 10,000 (memory protection)
- **Model Hyperparameters**:
  - `contamination`: 0.2 (20% expected anomalies)
  - `if_n_estimators`: 100
  - `random_state`: 42

## Configuration

All settings are managed through `config/settings.py` and can be overridden via environment variables.


### Key Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `entity_df_path` | `/data/processed_datasets/machines_with_anomalies_features` | Path to training parquet files |
| `max_fit_rows` | 50,000 | Maximum rows for IsolationForest training (prevents OOM) |
| `inference_chunk_size` | 10,000 | Rows per inference batch (memory control) |
| `contamination` | 0.2 | Expected anomaly rate (0.2 = 20%) |
| `if_n_estimators` | 100 | Number of trees in IsolationForest ensemble |

### Run in Docker

```bash
docker compose up --build training_service
```

### Expected Output

```
[MAIN] Starting first training run
[DATA] Loading data from datalake: /data/processed_datasets/machines_with_anomalies_features
[DATA] Reading parquet directory: /data/processed_datasets/machines_with_anomalies_features
[DATA] Found 5 parquet file(s)
[DATA] Loaded in 2.34s — 150,000 rows
[DATA] Sorting by timestamp...
[DATA] Total rows: 150,000
[DATA] Feature columns (12): ['Vibration_mm_s', 'Temperature_C', 'Current_A', ...]
[PIPELINE] Numeric: 9 | Categorical: 1
[TRAIN] Subsampling 150,000 → 50,000 rows
[TRAIN] Fitting pipeline on 50,000 rows...
[TRAIN] Completed in 8.45s
[INFERENCE] 150,000 rows → 15 chunk(s) of 10,000
[INFERENCE] Chunk 1/15 — 10,000 rows
...
[INFERENCE] Completed in 12.67s | 0.084 ms/record
[METRICS] Anomalies: 30,000 (20.00%)
[METRICS] Normal: 120,000 (80.00%)
[METRICS] Score mean: -0.1234 | std: 0.0567
[ARTIFACTS] Exported metrics to /outputs/metrics.json
[MLFLOW] Pipeline registered successfully
[MAIN] First training run completed successfully!
```

## MLflow Integration

### Logged Information

**Parameters:**
- Dataset characteristics (size, feature counts)
- Training configuration (contamination, n_estimators)
- Processing settings (max_fit_rows, chunk_size)

**Metrics:**
- Anomaly detection results (count, rate)
- Score distribution (mean, std, percentiles: p01, p05, p10, p25, p50, p75, p90, p95, p99)
- Performance (train_time, inference_time, latency_ms_per_record)

**Artifacts:**
- `metrics.json`: Detailed statistics export
- Model: Full sklearn Pipeline with input/output signature

**Model Registry:**
- Registered under name: `if_anomaly_detector`
- Versioned automatically by MLflow
- Ready for deployment to inference service


## Data Flow

```
┌────────────────────────────────────────────┐
│  Raw Sensor Data                           │
│  (streaming + batch pipelines)             │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│  Feature Engineering                       │
│  (data_engineering.py, batch_job.py)       │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│  Datalake (Parquet Files)                  │
│  /data/processed_datasets/machines_...     │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│  Training Service (THIS SERVICE)           │
│  - Load data                               │
│  - Train IsolationForest                   │
│  - Evaluate and log to MLflow              │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│  MLflow Model Registry                     │
│  Model: if_anomaly_detector                │
│  Version: Auto-incremented                 │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│  Inference Service                         │
│  - Load model from MLflow                  │
│  - Serve predictions via API               │
└────────────────────────────────────────────┘
```

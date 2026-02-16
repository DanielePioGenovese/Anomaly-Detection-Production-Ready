# Streaming Service — Real-Time Feature Engineering with Quix Streams

## Role in the Architecture

The Streaming Service is the **real-time processing backbone** of the anomaly detection system. It consumes raw telemetry from Redpanda, applies feature engineering and normalization, pushes features to Redis via Feast, and publishes processed data for downstream consumers.

```
┌──────────────┐   telemetry-data   ┌─────────────────────────────────────────┐
│   Redpanda   │ ─────────────────▸ │         Streaming Service (Quix)        │
│   (Kafka)    │                    │                                         │
└──────────────┘                    │  1. Consume raw telemetry               │
                                    │  2. Feature engineering (rolling aggs)  │
                                    │  3. Normalize with pre-trained Scaler   │
                                    │  4. Push features to Redis (Feast)      │
                                    │  5. Produce to processed-telemetry      │
                                    │                                         │
                                    └────────────┬──────────────┬─────────────┘
                                                 │              │
                                    ┌────────────▼──┐    ┌──────▼──────────┐
                                    │    Redis      │    │    Redpanda     │
                                    │ (Online Store)│    │ processed-topic │
                                    └───────────────┘    └─────────────────┘
```

## Technology: Quix Streams

[Quix Streams](https://github.com/quixio/quix-streams) is a Python-native stream processing library designed for real-time data pipelines. It provides a Pandas-like API on top of Kafka consumers/producers.

### Why Quix Streams over other options

| Alternative | Trade-off |
|-------------|-----------|
| **Apache Flink** | JVM-based, heavy infrastructure, overkill for single-service processing |
| **Kafka Streams** | JVM-only, no Python support |
| **Raw confluent-kafka** | Low-level — requires manual serialization, offset management, error handling |
| **Apache Spark Streaming** | Heavy runtime, high memory footprint, designed for cluster-scale |
| **Quix Streams** ✅ | Python-native, lightweight, built-in JSON serde, exactly-once semantics, minimal boilerplate |

## Processing Pipeline (`src/app.py`)

```python
# 1. Initialize Quix application connected to Redpanda
app = Application(broker_address="redpanda:9092", ...)

# 2. Define input/output topics
input_topic  = app.topic("telemetry-data", value_deserializer="json")
output_topic = app.topic("processed-telemetry", value_serializer="json")

# 3. Create streaming dataframe
sdf = app.dataframe(input_topic)

# 4. Apply transformation (for each message):
#    a. Compute rolling window aggregations
#    b. Normalize using pre-trained preprocessor (joblib artifact)
#    c. Push to Feast Online Store (Redis)
#    d. Return enriched feature vector
sdf = sdf.apply(process_and_push)

# 5. Publish to output topic
sdf = sdf.to_topic(output_topic)

# 6. Run the pipeline
app.run(sdf)
```

### Message Flow

**Input** (`telemetry-data` topic):
```json
{
  "Machine_ID": "WM_001",
  "timestamp": "2024-01-15 10:30:00",
  "Current_L1": 12.5,
  "Voltage_L_L": 400.2,
  "Vibration_mm_s": 3.8,
  "Water_Temp_C": 45.0,
  ...
}
```

**Output** (`processed-telemetry` topic):
```json
{
  "Machine_ID": "WM_001",
  "timestamp": "2024-01-15 10:30:00",
  "features": [0.23, -0.45, 1.12, ...],
  "raw_data": { ... }
}
```

## Artifacts Required

| Artifact | Source | Path in Container |
|----------|--------|--------------------|
| `preprocessor.joblib` | Training Service | `/streaming_service/data/models/preprocessor.joblib` |
| `feature_store.yaml` | Feature Store Config | `/streaming_service/feature_store.yaml` |
| Feast Registry | Feature Store Apply | `/app/data/registry/registry.db` |

## Docker Configuration

```yaml
# compose.yaml
streaming_service:
  build:
    context: .
    dockerfile: services/streaming_service/dockerfile
  depends_on:
    redpanda:
      condition: service_started
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
    - FEAST_REPO_PATH=/streaming_service
  volumes:
    - ./data/models:/streaming_service/data/models:ro    # Pre-trained preprocessor
    - ./data/registry:/app/data/registry                 # Feast registry
```

## Dependency Group

Defined in `pyproject.toml` under `[dependency-groups] streaming`:

```toml
streaming = [
    "quixstreams>=3.0.0",
    "pandas>=2.2.0",
    "scikit-learn>=1.5.0",
    "joblib>=1.4.0",
    "feast[redis]>=0.58.0",
    "python-dotenv>=1.0.0",
]
```

## Running

```bash
# Via Docker Compose
make streaming

# Or directly
docker compose up --build streaming_service
```

## Monitoring

Check the Redpanda Console at `http://localhost:8080` to monitor:
- **`telemetry-data`** topic — incoming raw messages
- **`processed-telemetry`** topic — outgoing processed features
- Consumer group lag for `quix-streaming-processor-v1`

# If Anomaly Service

## Overview

Lightweight Redpanda consumer that acts as the **bridge between the inference pipeline and the MCP agent**. It reads every prediction from the `predictions` topic, silently discards normal results, and fires a `POST /chat/stream` to the LangChain service for every confirmed anomaly.

## File Structure

```
services/if_anomaly_service/
├── Dockerfile
├── config/
│   ├── __init__.py         # Exports Config
│   └── config.py           # Pydantic settings (Kafka + MCP endpoints)
└── src/
    ├── __init__.py
    ├── anomaly_consumer.py  # Production consumer (QuixStreams)
    └── fakeproducer.py      # Debug-only: injects mock predictions into Redpanda
```

## Flow

```
Redpanda
  predictions topic
        │
        ▼
  anomaly_consumer
        │
        ├── is_anomaly == 0  →  skip (no action)
        │
        └── is_anomaly == 1  →  POST /chat/stream  →  LangChain service
                                    (machine_id, anomaly_score, features)
```

## Key Design Decisions

**Event-time alignment** — a custom `timestamp_extractor` reads the `timestamp` field embedded in the Kafka message payload and uses it as the event time for QuixStreams sliding windows. This ensures the consumer processes events in the order they actually occurred, not the order Redpanda received them. Falls back to the broker timestamp on any parsing error.

**Filter before action** — the stream is filtered with `sdf[sdf['is_anomaly'] == 1]` before calling `trigger_mcp_investigation`, so the side-effect function is only ever called for confirmed anomalies.

## Configuration (`config.py`)

All values can be overridden via environment variables (Pydantic `BaseSettings`):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_SERVER` | `redpanda:9092` | Redpanda broker address |
| `TOPIC_PREDICTIONS` | `predictions` | Topic to consume from |
| `TOPIC_TELEMETRY` | `telemetry-data` | Telemetry topic (referenced but not consumed here) |
| `AUTO_OFFSET_RESET` | `latest` | Start from latest message on first run |
| `CONSUMER_GROUP` | `if-anomaly-group` | Kafka consumer group id |
| `MCP_CLIENT_URL` | `http://langchain_service:8010` | Injected at runtime via compose |

> `MCP_CLIENT_URL` is set in `compose.yaml` to `http://langchain_service:8010` — the default in `config.py` (`http://mcp_client:8000`) is a placeholder only.

## Prediction Message Schema

The consumer expects messages on the `predictions` topic with this shape:

```json
{
  "machine_id":     "M_0001",
  "is_anomaly":     1,
  "anomaly_score":  0.873,
  "timestamp":      "2024-01-15T10:30:00+00:00",
  "features": {
    "Machine_ID":                        1,
    "Vibration_RollingMax_10min":        "31.4",
    "Current_Imbalance_RollingMean_5min":"0.21",
    "Current_Imbalance_Ratio":           "0.33",
    "Daily_Vibration_PeakMean_Ratio":    "9.7"
  }
}
```

## `fakeproducer.py` — Debug Only

Produces 49 synthetic prediction messages to the `predictions` topic, with anomalies injected at fixed intervals (messages 3, 6, 9, 12, 15, 21, 24, 27, 28, 30). Used exclusively to test the `anomaly_consumer → LangChain service` call chain without running the full inference pipeline.

```bash
# Trigger the fake producer
docker compose run --rm fakeproducer
```

> This is never used in production. The `fakeproducer` compose service has `restart: "no"`.

## Build & Run

```bash
# Build
docker build -f services/if_anomaly_service/Dockerfile -t if_anomaly_service:latest .

# Run consumer (production)
docker compose --profile online up if_anomaly

# Run fake producer (debug only)
docker compose run --rm fakeproducer
```
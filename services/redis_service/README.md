# Redis — Online Feature Store

## Role in the Architecture

Redis serves as the **Online Feature Store** for the anomaly detection system. It provides sub-millisecond access to the latest feature values, enabling real-time inference on incoming telemetry data.

```
┌──────────────┐     push      ┌───────────┐     get_online_features()     ┌───────────────┐
│  Streaming   │ ─────────────▸│   Redis   │◂──────────────────────────── │   Inference   │
│   Service    │   (via Feast) │  (Online  │         (via Feast)          │   Service     │
│  (Quix)      │               │   Store)  │                              │               │
└──────────────┘               └───────────┘                              └───────────────┘
       ▲                             ▲
       │                             │
       │                     materialize()
       │                             │
┌──────────────┐              ┌──────────────┐
│   Redpanda   │              │   Feature    │
│   (Kafka)    │              │   Loader     │
└──────────────┘              └──────────────┘
```

## How It Works

1. **Feature Loader** runs `feast materialize()` to bulk-load historical features from Parquet → Redis
2. **Streaming Service** pushes real-time features via `store.push()` using Feast's PushSource API
3. **Inference Service** reads the latest feature vector via `store.get_online_features()` for prediction

## Configuration

### `config/redis.conf`

| Setting | Value | Rationale |
|---------|-------|-----------|
| `save ""` | Persistence disabled | Features are recomputable from source data — no need for disk persistence |
| `appendonly no` | AOF disabled | Same rationale — reduces I/O overhead |
| `maxmemory 512mb` | Memory cap | Prevents Redis from consuming all host memory |
| `maxmemory-policy allkeys-lru` | LRU eviction | When memory is full, evict least-recently-used keys first |
| `bind 0.0.0.0` | Listen on all interfaces | Required for Docker container networking |

### Feast Integration

Redis is configured as the online store in `feature_store_service/config/feature_store.yaml`:

```yaml
online_store:
  type: redis
  connection_string: "redis:6379"
  key_ttl_seconds: 86400   # Features expire after 24 hours
```

## Docker

```yaml
# compose.yaml
redis:
  build:
    context: ./services/redis_service
    dockerfile: Dockerfile
  container_name: redis_online_store
  ports:
    - "6379:6379"
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 10s
    timeout: 5s
    retries: 5
```

The healthcheck ensures downstream services (Feast, Streaming) only start after Redis is fully operational.

## Monitoring

Connect to Redis from the host:

```bash
redis-cli -p 6379
> INFO memory          # Check memory usage
> DBSIZE              # Count of stored keys
> KEYS feast:*        # List Feast-managed keys
```

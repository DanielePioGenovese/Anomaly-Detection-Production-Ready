import os


class Config:
    """Runtime configuration for the Streaming Service.

    All values can be overridden via environment variables so the same image
    runs identically in development (docker-compose) and production (k8s).
    """

    # ── Kafka / Redpanda ──────────────────────────────────────────────────────
    KAFKA_SERVER: str      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    TOPIC_TELEMETRY: str   = os.getenv("TOPIC_TELEMETRY", "predictions")
    AUTO_OFFSET_RESET: str = os.getenv("AUTO_OFFSET_RESET", "latest")
    CONSUMER_GROUP: str    = os.getenv("CONSUMER_GROUP", "anomaly-investigator-group")

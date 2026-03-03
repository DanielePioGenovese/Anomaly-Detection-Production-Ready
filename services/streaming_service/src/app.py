import time
import json
import logging
import math
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from quixstreams import Application
from quixstreams.sinks.community.file.local import LocalFileSink
from quixstreams.models import TimestampType
import requests

from config.config import Config
from typing import Any

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StreamingService")

app = Application(broker_address=Config.KAFKA_SERVER)
input_topic = app.topic(Config.TOPIC_TELEMETRY)

# ----------------------------
# Timestamp utilities
# ----------------------------

def _parse_iso8601(ts: str) -> datetime:
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'

    dt = datetime.fromisoformat(ts)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def timestamp_setter(record: dict) -> int:
    ts_str = record['timestamp']
    dt = _parse_iso8601(ts_str)
    return int(dt.timestamp() * 1000)


def timestamp_extractor(
        value: Any,
        kafka_ts_ms: float
) -> int:
    try:
        if isinstance(value, dict) and 'timestamp' in value:
            return timestamp_setter(value)
    except Exception as e:
        logger.error(f'Timestamp extractor error, falling back to Kafka timestamp: {e}')

    return int(kafka_ts_ms)


# ----------------------------
# Feast Push Service
# ----------------------------

class FeastPusher:

    def __init__(self, base_url: str, push_source_name: str, push_to: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._push_source_name = push_source_name
        self._push_to = push_to
        self._session = requests.Session()

    def wait_until_ready(self, timeout_s: int = 120) -> None:
        deadline = time.time() + timeout_s

        while time.time() < deadline:
            try:
                r = self._session.get(f"{self._base_url}/health", timeout=2)

                if r.status_code == 200:
                    logger.info("Feast feature server is ready")

                    # Reset session to ensure a clean state
                    self._session.close()
                    self._session = requests.Session()

                    return

            except requests.RequestException:
                pass

            time.sleep(1)

        raise RuntimeError("Feast feature server is not available")

    def push(self, record: dict[str, Any]) -> None:

        df = {k: [v] for k, v in record.items()}

        payload = {
            'push_source_name': self._push_source_name,
            'to': self._push_to,
            'df': df
        }

        r = self._session.post(
            f"{self._base_url}/push",
            json=payload,
            timeout=5,
            headers={"Content-Type": "application/json"},
        )

        if r.status_code >= 300:
            raise RuntimeError(f"Feast push failed: HTTP {r.status_code} - {r.text}")


# ----------------------------
# Feature Mapping Function
# ----------------------------

def to_machine_stream_features(window_row: dict[str, Any]) -> dict[str, Any]:
    """
    Reformat the aggregated streaming row into the final feature dictionary,
    including derived and rolling streaming features.
    """

    return {
        "Machine_ID": int(window_row["Machine_ID"]),
        "timestamp": str(window_row["timestamp"]),

        # Intermediate derived column
        "Current_Imbalance_Ratio": float(window_row["Current_Imbalance_Ratio"]),

        # 10-minute rolling max vibration
        "Vibration_RollingMax_10min": float(window_row["Vibration_RollingMax_10min"]),

        # 5-minute rolling mean imbalance
        "Current_Imbalance_RollingMean_5min": float(window_row["Current_Imbalance_RollingMean_5min"]),
    }
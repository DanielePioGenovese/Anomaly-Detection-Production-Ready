import time
import logging
from datetime import datetime, timedelta, timezone

from quixstreams import Application
from quixstreams.sinks.community.file.local import LocalFileSink
import requests
from config.config import Config
from typing import Any


from quixstreams.dataframe.windows import (
    Latest,
    Mean,
    Max
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StreamingService")


# ----------------------------
# Timestamp utilities
# ----------------------------

def _parse_iso8601(ts: str) -> datetime:
    """Normalise ISO-8601 strings to an aware datetime (UTC fallback)."""
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def timestamp_setter(record: dict) -> int:
    """Convert the record's 'timestamp' field to epoch milliseconds."""
    dt = _parse_iso8601(record['timestamp'])
    return int(dt.timestamp() * 1000)


from quixstreams.models import TimestampType

def timestamp_extractor(
    value: Any,
    headers: Any,
    timestamp: float,
    timestamp_type: TimestampType
) -> int:
    """
    QuixStreams timestamp extractor.
    Prefers the event timestamp in the message; falls back to the Kafka broker timestamp.
    """
    try:
        if isinstance(value, dict) and 'timestamp' in value:
            return timestamp_setter(value)
    except Exception as e:
        logger.error(f'Timestamp extractor error, falling back to Kafka timestamp: {e}')

    return int(timestamp)


# ----------------------------
# Feast Push Service
# ----------------------------

class FeastPusher:
    """Thin HTTP client for the Feast feature server's /push endpoint."""

    def __init__(self, base_url: str, push_source_name: str, push_to: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._push_source_name = push_source_name
        self._push_to = push_to
        self._session = requests.Session()

    def wait_until_ready(self, timeout_s: int = 120) -> None:
        """Block until the Feast server responds on /health or the timeout expires."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                r = self._session.get(f"{self._base_url}/health", timeout=2)
                if r.status_code == 200:
                    logger.info("Feast feature server is ready")
                    self._session.close()
                    self._session = requests.Session()
                    return
            except requests.RequestException:
                pass
            time.sleep(1)
        raise RuntimeError("Feast feature server is not available")

    def push(self, record: dict[str, Any]) -> None:
        """Push a single feature record to the online (and offline) store."""
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
# Feature mapping functions
# ----------------------------

def to_vibration_features(row: dict) -> dict:
    """Map 10-min window output → Feast schema (vibration features only)."""
    return {
        "Machine_ID": row.get("Machine_ID"),
        "timestamp": row["latest_timestamp"],
        "Vibration_RollingMax_10min": row["Vibration_RollingMax_10min"],
    }


def to_current_features(row: dict) -> dict:
    """Map 5-min window output → Feast schema (current imbalance features only)."""
    return {
        "Machine_ID": row.get("Machine_ID"),
        "timestamp": row["latest_timestamp"],
        "Current_Imbalance_RollingMean_5min": row["Current_Imbalance_RollingMean_5min"],
    }

    
def compute_current_imbalance_ratio(record: dict) -> float:
    """
    Instantaneous 3-phase current imbalance scalar.
    Formula: (max(L1,L2,L3) - min(L1,L2,L3)) / mean(L1,L2,L3)
    Returns NaN when the mean is zero or any input is missing/invalid.
    """
    try:
        c1 = float(record.get("Current_L1", 0.0))
        c2 = float(record.get("Current_L2", 0.0))
        c3 = float(record.get("Current_L3", 0.0))
        maximum = max(c1, c2, c3)
        minimum = min(c1, c2, c3)
        mean_val = (c1 + c2 + c3) / 3.0
        if mean_val == 0.0:
            logger.warning(f"Mean current is zero for Machine_ID={record.get('Machine_ID')}; setting imbalance = NaN")
            return float("nan")
        return float((maximum - minimum) / mean_val)
    except Exception as e:
        logger.error(f"Failed to compute Current_Imbalance_Ratio for record (Machine_ID={record.get('Machine_ID')}): {e}")
        return float("nan")


# ----------------------------
# Main pipeline
# ----------------------------

def main() -> None:
    logger.info('Starting streaming transformations')

    feast = FeastPusher(
        base_url=Config.FEAST_SERVER_URL,
        push_source_name=Config.PUSH_SOURCE_NAME,
        push_to=Config.PUSH_TO
    )
    feast.wait_until_ready()

    app = Application(
        broker_address=Config.KAFKA_SERVER,
        consumer_group=Config.TOPIC_TELEMETRY,
        auto_offset_reset=Config.AUTO_OFFSET_RESET,
        state_dir=Config.STATE_DIR
    )

    topic = app.topic(
        Config.TOPIC_TELEMETRY,
        value_deserializer="json",
        timestamp_extractor=timestamp_extractor,
    )

    # Single intermediate topic for BOTH windows (tagged by type)
    topic_windows = app.topic("features-windows", value_serializer="json")

    raw_sink = LocalFileSink(directory=Config.ENTITY_DF, format=Config.ENTITY_DF_FORMAT)

    sdf = app.dataframe(topic)
    sdf.sink(raw_sink)

    sdf['Current_Imbalance_Ratio'] = sdf.apply(compute_current_imbalance_ratio)

    # ── 10-min window ──────────────────────────────────────────────────────────
    sdf_10min = (
        sdf.sliding_window(duration_ms=timedelta(minutes=10), grace_ms=timedelta(minutes=2))
        .agg(
            Machine_ID=Latest("Machine_ID"),
            Vibration_RollingMax_10min=Max("Vibration_mm_s"),
            latest_timestamp=Latest("timestamp"),
        )
        .current()
    )
    sdf_10min["window_type"] = "10min"
    sdf_10min.to_topic(topic_windows)

    # ── 5-min window ───────────────────────────────────────────────────────────
    sdf_5min = (
        sdf.sliding_window(duration_ms=timedelta(minutes=5), grace_ms=timedelta(minutes=2))
        .agg(
            Machine_ID=Latest("Machine_ID"),
            Current_Imbalance_RollingMean_5min=Mean("Current_Imbalance_Ratio"),
            Current_Imbalance_Ratio=Latest("Current_Imbalance_Ratio"),
            latest_timestamp=Latest("timestamp"),
        )
        .current()
    )
    sdf_5min["window_type"] = "5min"
    sdf_5min.to_topic(topic_windows)

    # ── Stateful merge + Feast push ────────────────────────────────────────────
    sdf_merged = app.dataframe(topic_windows)

    def merge_windows(record: dict, state) -> dict:
        """
        Cache the latest values from each window in state (keyed per Machine_ID).
        Emit a fully-combined record on every update.
        """
        window_type = record.get("window_type")

        if window_type == "10min":
            state.set("Vibration_RollingMax_10min",  record.get("Vibration_RollingMax_10min"))
            state.set("latest_timestamp",             record.get("latest_timestamp"))

        elif window_type == "5min":
            state.set("Current_Imbalance_RollingMean_5min", record.get("Current_Imbalance_RollingMean_5min"))
            state.set("Current_Imbalance_Ratio",            record.get("Current_Imbalance_Ratio"))
            state.set("latest_timestamp",                   record.get("latest_timestamp"))

        raw_ts    = state.get("latest_timestamp") or record.get("latest_timestamp")
        utc_ts    = _parse_iso8601(raw_ts).isoformat()

        return {
            "Machine_ID":                        record["Machine_ID"],
            "timestamp":                         utc_ts,
            "Vibration_RollingMax_10min":        state.get("Vibration_RollingMax_10min"),
            "Current_Imbalance_RollingMean_5min":state.get("Current_Imbalance_RollingMean_5min"),
            "Current_Imbalance_Ratio":           state.get("Current_Imbalance_Ratio"),
        }

    def push_to_feast(record: dict) -> None:
        logger.info(f"Pushing to Feast: {record}")
        try:
            feast.push(record)
        except Exception as e:
            logger.exception(f"Error pushing to Feast: {e}")

    (
        sdf_merged
        .apply(merge_windows, stateful=True)
        .apply(push_to_feast)
    )

    logger.info('Pipelines configured — starting app')
    app.run()

if __name__ == '__main__':
    main()
"""
Inference Pipeline — Washing Machine Anomaly Detection
======================================================

  - Model loaded once at startup (closure)
  - Feature column order loaded from the MLflow model signature (no hardcoding)
  - Feast features fetched via HTTP REST (/get-online-features)
  - score() applied to every message via sdf.apply()
  - Output published natively via sdf.to_topic()   ← no separate Producer

Data flow:
  [telemetry-data]  →  fetch Feast features  →  IsolationForest  →  [predictions]
"""

import logging
import random
from datetime import datetime, timezone
from typing import Any

import mlflow
import mlflow.sklearn
import pandas as pd
import requests
from quixstreams import Application
import numpy as np

from config import Config

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger(Config.SERVICE_NAME)

# ─────────────────────────────────────────────────────────────────────────────
# MODEL LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_model() -> tuple:
    """
    Load the sklearn Pipeline from the MLflow Model Registry and extract
    the feature column order from the model signature.

    The signature was saved during training with create_and_log_signature(),
    which used the raw (untransformed) DataFrame — so the column names and
    order here exactly match what the Pipeline expects at inference time.

    Returns
    -------
    (model_pipeline, model_uri_string, feature_columns_list)
    """
    mlflow.set_tracking_uri(Config.MLFLOW_TRACKING_URI)
    model_uri = f"models:/{Config.MLFLOW_MODEL_NAME}/{Config.MLFLOW_MODEL_STAGE}"

    logger.info(
        "Loading model from MLflow: %s  (tracking=%s)",
        model_uri, Config.MLFLOW_TRACKING_URI,
    )
    loaded = mlflow.sklearn.load_model(model_uri)

    # Load feature column order from the MLflow signature so training and
    # inference are always in sync with zero manual maintenance.
    model_info = mlflow.models.get_model_info(model_uri)
    feature_columns = [col.name for col in model_info.signature.inputs]
    logger.info("✓ Model loaded — %d features: %s", len(feature_columns), feature_columns)

    return loaded, model_uri, feature_columns


# ─────────────────────────────────────────────────────────────────────────────
# FEAST  —  HTTP REST helper
# ─────────────────────────────────────────────────────────────────────────────

def feast_get_online_features(
    session: requests.Session,
    machine_id: int,
) -> dict[str, Any]:
    """
    Fetch the full feature vector for *machine_id* from the Feast feature
    server via HTTP POST /get-online-features.

    The feature service 'machine_anomaly_service_v1' returns:
      • machine_streaming_features  (raw sensors + rolling windows)
      • machine_batch_features      (daily aggregations)

    Returns
    -------
    dict  {feature_name: value}   — None for features with status != PRESENT
    """
    payload = {
        "feature_service":    Config.FEAST_FEATURE_SERVICE,
        "full_feature_names": Config.FEAST_FULL_FEATURE_NAMES,
        "entities": {
            Config.FEAST_ENTITY_KEY: [machine_id],  # e.g. {"Machine_ID": [42]}
        },
    }

    url = f"{Config.FEAST_SERVER_URL.rstrip('/')}/get-online-features"
    response = session.post(url, json=payload, timeout=Config.FEAST_REQUEST_TIMEOUT_S)
    response.raise_for_status()

    data    = response.json()
    names   = data["metadata"]["feature_names"]   # list of feature name strings
    results = data["results"]                      # list of {values, statuses, ...}

    features: dict[str, Any] = {}
    for name, res in zip(names, results):
        status = (res.get("statuses") or ["MISSING"])[0]
        value  = (res.get("values")   or [None])[0]
        features[name] = value if status == "PRESENT" else None

    logger.debug("Feast features for Machine %d: %s", machine_id, features)

    return features


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_x(features: dict[str, Any], feature_columns: list[str]) -> pd.DataFrame:
    """
    Order the feature dict into a one-row DataFrame whose columns match
    feature_columns exactly (the order the Pipeline was trained on, as
    read from the MLflow model signature).

    Type handling:
      - Cycle_Phase_ID is kept as str so the OneHotEncoder branch in the
        ColumnTransformer receives the same type it was trained on.
      - All other columns are coerced to numeric.
    """
    row = {col: features.get(col) for col in feature_columns}
    x   = pd.DataFrame([row], columns=feature_columns)

    x = x.replace({None: np.nan})

    for col in x.columns:
        if col == "Cycle_Phase_ID":
            # Keep as string for OneHotEncoder consistency with training
            x[col] = x[col].astype(str)
        else:
            x[col] = pd.to_numeric(x[col], errors="coerce")

    return x


# ─────────────────────────────────────────────────────────────────────────────
# PREDICTION HELPER (UNSUPERVISED - NO THRESHOLDS)
# ─────────────────────────────────────────────────────────────────────────────

def predict(model, x: pd.DataFrame) -> tuple[int, float]:
    """
    Run IsolationForest inference using the model's native decision logic.
    
    IMPORTANT: This is an UNSUPERVISED model. The contamination parameter
    set during training determines the decision boundary. We do NOT apply
    custom thresholds here — we trust the model's built-in classification.
    
    Parameters
    ----------
    model : sklearn Pipeline
        The loaded MLflow model (preprocessing + IsolationForest)
    x : pd.DataFrame
        Single-row feature DataFrame
    
    Returns
    -------
    (is_anomaly, anomaly_score)
        is_anomaly : int
            1 = anomaly, 0 = normal (mapped from sklearn's -1/+1)
        anomaly_score : float
            Anomaly score from score_samples() (lower = more anomalous)
            Typically ranges from ~-0.5 to ~0.5
    """
    # Get the raw sklearn prediction: -1 = anomaly, +1 = normal
    raw_label = int(model.predict(x)[0])
    
    # Get the anomaly score (consistent with training which used score_samples)
    # Lower scores = more anomalous
    # Note: decision_function() and score_samples() are equivalent for IsolationForest
    preprocessed_x = model.named_steps["pre"].transform(x)
    anomaly_score = float(model.named_steps["model"].score_samples(preprocessed_x)[0])
    
    # Map sklearn convention to output convention:
    # sklearn: -1 = anomaly, +1 = normal
    # output:   1 = anomaly,  0 = normal
    is_anomaly = Config.OUTPUT_ANOMALY if raw_label == -1 else Config.OUTPUT_NORMAL
    
    return is_anomaly, anomaly_score


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:

    # 1. Load model + feature column order from MLflow
    model, model_uri, feature_columns = load_model()

    # 2. Open a persistent HTTP session for Feast REST calls
    session = requests.Session()

    # 3. QuixStreams application
    app = Application(
        broker_address=Config.KAFKA_BOOTSTRAP_SERVERS,
        consumer_group=Config.CONSUMER_GROUP,
        auto_offset_reset=Config.AUTO_OFFSET_RESET,
    )

    # 4. Topics
    t_in  = app.topic(Config.TOPIC_INPUT,  value_deserializer="json")   # telemetry-data
    t_out = app.topic(Config.TOPIC_OUTPUT, value_serializer="json")     # predictions

    # 5. Streaming dataframe
    sdf = app.dataframe(t_in)

    # ── Core inference function ───────────────────────────────────────────────
    def score(msg: dict[str, Any]) -> dict[str, Any]:
        """
        Full inference pipeline for a single telemetry message.

        Called by sdf.apply() for every record consumed from [telemetry-data].
        The result dict is serialised as JSON and published to [predictions]
        via sdf.to_topic() — no separate Kafka Producer needed.

        Output keys
        -----------
        machine_id         : canonical string id  (e.g. "M_0001")
        numeric_id         : int entity key used for Feast
        is_anomaly         : 1 = anomaly, 0 = normal
        anomaly_score      : IsolationForest score_samples value (< 0 = more anomalous)
        features           : feature snapshot returned by Feast
        missing_features   : list of feature names that were None / not PRESENT
        source_timestamp   : original telemetry event timestamp
        scored_at          : UTC timestamp of this inference
        model_uri          : MLflow model URI used
        """
        # ── Build output skeleton (always populated, even on error) ──────────
        out = {
            "machine_id":       msg.get("Machine_ID"),
            "source_timestamp": msg.get("timestamp", ""),
            "scored_at":        datetime.now(timezone.utc).isoformat(),
            "model_uri":        model_uri,
        }

        try:
            # ── Parse Machine_ID → integer entity key ────────────────────────
            raw_id     = out["machine_id"]
            s          = str(raw_id).strip()
            numeric_id = int(s.replace("M_", "")) if s.startswith("M_") else int(s)

            out["numeric_id"] = numeric_id
            out["machine_id"] = f"M_{numeric_id:04d}"

            # ── Fetch features from Feast (Redis via HTTP) ───────────────────
            features = feast_get_online_features(session, numeric_id)

            missing = [k for k, v in features.items() if v is None]
            if missing:
                logger.warning("[%s] Missing features: %s", out["machine_id"], missing)

            # ── Build aligned feature DataFrame ─────────────────────────────
            x = build_x(features, feature_columns)

            # ── Run model (uses native IsolationForest decision) ─────────────
            is_anomaly, anomaly_score = predict(model, x)

            is_forced = False
            if random.random() < 0.05:
                is_anomaly = 1
                is_forced = True
                logger.info("[%s] 🎲 RANDOM INJECTION: Forcing anomaly for testing.", out["machine_id"])

            # ── Enrich output ────────────────────────────────────────────────
            out["is_anomaly"]       = is_anomaly
            out["anomaly_score"]    = round(anomaly_score, 6)
            out["features"]         = features
            out["missing_features"] = missing

            logger.info(
                "[%s] is_anomaly=%d  score=%.4f",
                out["machine_id"], is_anomaly, anomaly_score,
            )

        except Exception as exc:
            # Never crash the streaming loop — log and pass the error downstream
            out["error"] = str(exc)
            logger.error(
                "[%s] Inference failed: %s",
                out.get("machine_id", "?"), exc, exc_info=True,
            )

        return out

    # 6. Wire the pipeline
    sdf = sdf.apply(score)
    sdf = sdf.to_topic(t_out)

    logger.info(
        "Inference pipeline running: '%s' → '%s'  (feast=%s, model=%s)",
        Config.TOPIC_INPUT, Config.TOPIC_OUTPUT,
        Config.FEAST_SERVER_URL, model_uri,
    )

    # 7. Start the consumption loop (blocks until SIGTERM / SIGINT)
    app.run()


if __name__ == "__main__":
    main()
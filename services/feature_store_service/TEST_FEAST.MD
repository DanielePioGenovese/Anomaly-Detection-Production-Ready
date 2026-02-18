# Feature Store — API Test Suite

A complete test suite to verify every aspect of your Feature Store. Tests are organized in logical order: from system health check, to data ingestion, to feature retrieval.

---

## 1. Connection Test (Health Check)

Verifies that the Feast server is running and able to communicate with the Registry database.

- **Method:** `GET`
- **URL:** `http://localhost:6566/health`
- **Expected result:** `200 OK` response. If it fails, the container did not start correctly.

---

## 2. Streaming Ingestion Test (Push API)

Simulates sending real-time data (as Quixstreams would do). This test checks whether the `PushSource` named `washing_stream_push` accepts data.

- **Method:** `POST`
- **URL:** `http://localhost:6566/push`
- **Body (JSON):**

```json
{
  "push_source_name": "washing_stream_push",
  "df": {
    "Machine_ID": [1001],
    "timestamp": ["2026-02-18T20:00:00Z"],
    "Cycle_Phase_ID": [1],
    "Current_L1": [15.5],
    "Current_L2": [14.8],
    "Current_L3": [16.2],
    "Voltage_L_L": [398.0],
    "Water_Temp_C": [45.0],
    "Motor_RPM": [1200.0],
    "Water_Flow_L_min": [10.5],
    "Vibration_mm_s": [2.4],
    "Water_Pressure_Bar": [3.1],
    "Current_Imbalance_Ratio": [0.08],
    "Vibration_RollingMax_10min": [4.5],
    "Current_Imbalance_RollingMean_5min": [0.05]
  },
  "to": "online"
}
```

- **Expected result:** Empty response `{}` with status `200`. This means the data is now stored in Redis.

---

## 3. Online Feature Retrieval Test (Single Entity)

Verifies that Feast can retrieve the data just pushed to Redis via the `FeatureService`.

- **Method:** `POST`
- **URL:** `http://localhost:6566/get-online-features`
- **Body (JSON):**

```json
{
  "feature_service": "machine_anomaly_service_v1",
  "entities": {
    "Machine_ID": [1001]
  }
}
```

- **Expected result:** A JSON response containing the values inserted in the previous test. Batch features (e.g., `Weekly_Current_StdDev`) will be `null` because Parquet materialization has not been run yet.

---

## 4. Multi-Entity Retrieval Test (Batch Retrieval)

Verifies that Feast correctly handles requests for multiple machines simultaneously.

- **Method:** `POST`
- **URL:** `http://localhost:6566/get-online-features`
- **Body (JSON):**

```json
{
  "feature_service": "machine_anomaly_service_v1",
  "entities": {
    "Machine_ID": [1001, 9999]
  }
}
```

- **Expected result:** Two result sets. Machine `1001` will return data; machine `9999` (which does not exist) will return all `null` values and a `NOT_FOUND` status.

---

## 5. Type Validation Test (Error Test)

Tests Feast's robustness by sending an incorrect data type (e.g., a string where a number is expected).

- **Method:** `POST`
- **URL:** `http://localhost:6566/push`
- **Body (JSON):**

```json
{
  "push_source_name": "washing_stream_push",
  "df": {
    "Machine_ID": [1001],
    "timestamp": ["2026-02-18T20:00:00Z"],
    "Motor_RPM": ["FAST"]
  },
  "to": "online"
}
```

- **Expected result:** A `400` or `500` error with a message indicating that the string could not be converted to `Float`.

---

## 6. Expiration Test (TTL — Time To Live)

In your `features.py` file, a `ttl` of 24 hours is set for the streaming feature view.

- **How to test:** Push data with a timestamp that is 2 days old (e.g., `2025-01-01T10:00:00Z`), then attempt to read it using Test #3.
- **Expected result:** Status `OUTSIDE_MAX_AGE` or a `null` value.

---

## Next Steps

Once you have verified that all tests pass, consider creating a script to simulate **Batch Data Loading (Materialization)** to eliminate the remaining `null` values in the feature retrieval results.
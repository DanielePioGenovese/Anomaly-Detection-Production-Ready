"""
test_feature_store.py
─────────────────────
Progressive test suite for the Feast feature store.
Tests are ordered from simplest to most complex:

  Level 1 – Registry smoke test   (is apply working?)
  Level 2 – Online store baseline (is Redis reachable? are values None before any write?)
  Level 3 – Push + read-back      (does the online write/read cycle work?)
  Level 4 – Materialize + verify  (does batch materialisation reach Redis?)
  Level 5 – Historical features   (does the offline store return training data?)

Run from the repo root:
    python -m pytest test_feature_store.py -v
Or directly:
    python test_feature_store.py
"""

import pandas as pd
import pytest
from feast import FeatureStore

# ---------------------------------------------------------------------------
# Timezone strategy
# ---------------------------------------------------------------------------
# The batch parquet files on disk use tz-naive timestamps (datetime64[ns]).
# Feast's point-in-time join will raise a TypeError if entity_df uses
# tz-aware timestamps and the parquet uses tz-naive ones.
# Rule: keep ALL timestamps tz-naive (pd.Timestamp.now() with no tz arg)
# so that Feast can compare them without a timezone mismatch.
# The one exception is materialize() — Feast converts the range to UTC
# internally, so passing tz-naive datetimes is safe there too.

# ---------------------------------------------------------------------------
# Shared store instance (reused across all tests)
# ---------------------------------------------------------------------------
REPO_PATH = "/feature_store_service"
store = FeatureStore(repo_path=REPO_PATH)

# Entity rows used consistently throughout the tests
ENTITY_ROWS = [{"Machine_ID": 1}, {"Machine_ID": 2}, {"Machine_ID": 3}]


# ===========================================================================
# LEVEL 1 — Registry smoke test
# Verifies that `feast apply` has registered all expected objects.
# Nothing external (Redis, parquet files) needs to be running.
# ===========================================================================

class TestRegistry:

    def test_feature_views_registered(self):
        """All expected feature views must be present in the registry."""
        names = {fv.name for fv in store.list_feature_views()}
        assert "machine_streaming_features" in names, \
            "machine_streaming_features not found — did you run feast apply?"
        assert "machine_batch_features" in names, \
            "machine_batch_features not found — did you run feast apply?"

    def test_entities_registered(self):
        """The 'machine' entity must be present in the registry."""
        names = {e.name for e in store.list_entities()}
        assert "machine" in names, \
            "'machine' entity not found — did you run feast apply?"

    def test_feature_service_registered(self):
        """The anomaly detection feature service must be present."""
        names = {fs.name for fs in store.list_feature_services()}
        assert "machine_anomaly_service_v1" in names, \
            "machine_anomaly_service_v1 not found — did you run feast apply?"

    def test_streaming_view_schema(self):
        """Streaming feature view must expose all expected field names."""
        fv = store.get_feature_view("machine_streaming_features")
        field_names = {f.name for f in fv.schema}
        expected = {
            "Cycle_Phase_ID", "Current_L1", "Current_L2", "Current_L3",
            "Voltage_L_L", "Water_Temp_C", "Motor_RPM", "Water_Flow_L_min",
            "Vibration_mm_s", "Water_Pressure_Bar",
            "Current_Imbalance_Ratio",
            "Vibration_RollingMax_10min",
            "Current_Imbalance_RollingMean_5min",
        }
        missing = expected - field_names
        assert not missing, f"Missing fields in streaming view: {missing}"

    def test_batch_view_schema(self):
        """Batch feature view must expose daily and weekly aggregation fields."""
        fv = store.get_feature_view("machine_batch_features")
        field_names = {f.name for f in fv.schema}
        assert "Daily_Vibration_PeakMean_Ratio" in field_names
        assert "Weekly_Current_StdDev" in field_names


# ===========================================================================
# LEVEL 2 — Online store baseline (before any write)
# Before materialisation or push the online store should return None for all
# features, but the call itself must NOT raise an exception.
# This confirms Redis is reachable and the entity lookup works.
# ===========================================================================

class TestOnlineStoreBaseline:

    def test_streaming_view_reachable(self):
        """get_online_features must succeed even when values are not yet loaded."""
        result = store.get_online_features(
            features=[
                "machine_streaming_features:Motor_RPM",
                "machine_streaming_features:Vibration_mm_s",
            ],
            entity_rows=ENTITY_ROWS,
        ).to_dict()

        # Entity key must always be present
        assert "Machine_ID" in result
        # Values may be None at this stage — that is expected and correct
        assert "Motor_RPM" in result

    def test_batch_view_reachable(self):
        """Batch view must be reachable from the online store."""
        result = store.get_online_features(
            features=[
                "machine_batch_features:Daily_Vibration_PeakMean_Ratio",
                "machine_batch_features:Weekly_Current_StdDev",
            ],
            entity_rows=ENTITY_ROWS,
        ).to_dict()

        assert "Machine_ID" in result
        assert "Daily_Vibration_PeakMean_Ratio" in result

    def test_feature_service_reachable(self):
        """Feature service must combine both views and return the full vector."""
        fs = store.get_feature_service("machine_anomaly_service_v1")
        result = store.get_online_features(
            features=fs,
            entity_rows=ENTITY_ROWS,
        ).to_dict()

        # 15 features + Machine_ID entity key = 16 total keys
        assert len(result) == 16, \
            f"Expected 16 keys from feature service, got {len(result)}: {list(result.keys())}"


# ===========================================================================
# LEVEL 3 — Push + read-back
# Pushes known values to the online store, then immediately reads them back.
# After the push the values must no longer be None — this validates the full
# online write → read cycle for the streaming pipeline.
# ===========================================================================

class TestPushAndReadBack:

    @staticmethod
    def _make_push_data() -> pd.DataFrame:
        """
        Build a push payload with a fresh UTC-aware timestamp.

        Why UTC here but tz-naive everywhere else?
        push() writes to Redis through Feast's online store layer, which
        stores the event_timestamp as UTC internally.  When get_online_features()
        checks the TTL it compares against UTC now.  If we pass a tz-naive
        timestamp Feast treats it as local time, which can cause the entry to
        appear expired (or in the future) depending on the host timezone.
        Using UTC explicitly makes the TTL check deterministic.

        Note: this is a @staticmethod / factory rather than a class-level
        attribute so that the timestamp is always *current* when the test
        runs, not stale from import time.
        """
        now_utc = pd.Timestamp.now(tz="UTC")
        return pd.DataFrame({
            "Machine_ID":                        [1,       2,       3],
            "timestamp":                         [now_utc, now_utc, now_utc],
            "Cycle_Phase_ID":                    [2,       3,       1],
            "Current_L1":                        [12.5,    13.2,    11.8],
            "Current_L2":                        [12.3,    13.1,    11.9],
            "Current_L3":                        [12.4,    13.0,    12.0],
            "Voltage_L_L":                       [230.5,   229.8,   231.2],
            "Water_Temp_C":                      [45.0,    50.5,    42.0],
            "Motor_RPM":                         [1200.0,  1150.0,  1300.0],
            "Water_Flow_L_min":                  [15.5,    16.2,    14.8],
            "Vibration_mm_s":                    [2.3,     2.1,     2.5],
            "Water_Pressure_Bar":                [3.2,     3.1,     3.3],
            "Current_Imbalance_Ratio":           [0.015,   0.018,   0.012],
            "Vibration_RollingMax_10min":        [3.5,     3.2,     3.8],
            "Current_Imbalance_RollingMean_5min":[0.014,   0.017,   0.013],
        })

    def test_push_does_not_raise(self):
        """push() must complete without errors."""
        store.push(
            push_source_name="washing_stream_push",
            df=self._make_push_data(),
            to="online",
        )

    def test_values_present_after_push(self):
        """After push, online store must return real values (not None)."""
        # Write
        store.push(
            push_source_name="washing_stream_push",
            df=self._make_push_data(),
            to="online",
        )

        # Read back
        result = store.get_online_features(
            features=[
                "machine_streaming_features:Motor_RPM",
                "machine_streaming_features:Vibration_mm_s",
                "machine_streaming_features:Current_Imbalance_Ratio",
                "machine_streaming_features:Vibration_RollingMax_10min",
                "machine_streaming_features:Current_Imbalance_RollingMean_5min",
            ],
            entity_rows=ENTITY_ROWS,
        ).to_dict()

        assert result["Motor_RPM"] != [None, None, None], \
            "Motor_RPM is still None after push — write to Redis failed"
        assert result["Vibration_mm_s"] != [None, None, None], \
            "Vibration_mm_s is still None after push"

    def test_pushed_values_match_input(self):
        """Values read back from Redis must match what was pushed."""
        store.push(
            push_source_name="washing_stream_push",
            df=self._make_push_data(),
            to="online",
        )

        result = store.get_online_features(
            features=["machine_streaming_features:Motor_RPM"],
            entity_rows=[{"Machine_ID": 1}],
        ).to_dict()

        # Machine_ID=1 should have Motor_RPM=1200.0
        assert result["Motor_RPM"][0] == pytest.approx(1200.0, rel=1e-3), \
            f"Expected 1200.0 for Machine_ID=1, got {result['Motor_RPM'][0]}"


# ===========================================================================
# LEVEL 4 — Materialisation
# Materialises the batch feature view from its FileSource into Redis, then
# reads back to confirm data arrived in the online store.
# The streaming view materialisation test is marked xfail because it requires
# streaming_feature_backfill.parquet to exist (generate it with
# generate_backfill_parquet.py first).
# ===========================================================================

class TestMaterialization:

    @staticmethod
    def _get_entity_rows_from_parquet(n: int = 3) -> list[dict]:
        """
        Read a sample of real Machine_IDs from the batch parquet.

        Why not hardcode [1, 2, 3]?
        The batch parquet is generated by a separate PySpark pipeline and
        may contain Machine_IDs that differ from the synthetic IDs used in
        push tests.  Using actual IDs from the source guarantees we look up
        entities that materialise() has actually written to Redis.
        """
        import glob, os
        parquet_dir = "/feature_store_service/data/offline/machines_batch_features"
        files = glob.glob(os.path.join(parquet_dir, "**/*.parquet"), recursive=True) \
              + glob.glob(os.path.join(parquet_dir, "*.parquet"))
        if not files:
            return [{"Machine_ID": 1}, {"Machine_ID": 2}, {"Machine_ID": 3}]
        sample = pd.read_parquet(files[0], columns=["Machine_ID"]).head(n)
        return [{"Machine_ID": int(mid)} for mid in sample["Machine_ID"].unique()[:n]]

    def test_batch_materialize(self):
        """Batch feature view must materialise from parquet into Redis."""
        end_date   = pd.Timestamp.now()                    # tz-naive — Feast converts internally
        start_date = end_date - pd.Timedelta(days=7)

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["machine_batch_features"],
        )

        # Use real Machine_IDs from the parquet so we don't look up entities
        # that were never written to Redis
        entity_rows = self._get_entity_rows_from_parquet()

        result = store.get_online_features(
            features=[
                "machine_batch_features:Daily_Vibration_PeakMean_Ratio",
                "machine_batch_features:Weekly_Current_StdDev",
            ],
            entity_rows=entity_rows,
        ).to_dict()

        assert any(v is not None for v in result["Daily_Vibration_PeakMean_Ratio"]), \
            (
                "Daily_Vibration_PeakMean_Ratio is None for all machines after materialisation.\n"
                f"  Entity rows used: {entity_rows}\n"
                "  If the parquet timestamp column is outside the 7-day window, "
                "widen start_date or check the actual date range in the parquet."
            )

    @pytest.mark.xfail(
        reason="Requires streaming_feature_backfill.parquet — run generate_backfill_parquet.py first"
    )
    def test_streaming_materialize(self):
        """Streaming feature view must materialise once the backfill parquet exists."""
        end_date   = pd.Timestamp.now(tz=None)
        start_date = end_date - pd.Timedelta(days=1)

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["machine_streaming_features"],
        )

        result = store.get_online_features(
            features=["machine_streaming_features:Motor_RPM"],
            entity_rows=ENTITY_ROWS,
        ).to_dict()

        assert any(v is not None for v in result["Motor_RPM"]), \
            "Motor_RPM is None for all machines after streaming materialisation"


# ===========================================================================
# LEVEL 5 — Historical features (offline store)
# Calls get_historical_features() which performs a point-in-time join against
# the parquet files.  The streaming part is xfail for the same reason as above.
# ===========================================================================

class TestHistoricalFeatures:

    def _make_entity_df(self) -> pd.DataFrame:
        """
        Build a small entity dataframe with tz-NAIVE timestamps.

        Why tz-naive here?
        The batch parquet on disk was written with tz-naive timestamps
        (datetime64[ns]).  Feast's point-in-time join in the Dask offline
        store compares the entity event_timestamp against the parquet
        timestamp column directly.  If one is tz-aware and the other is
        tz-naive, pandas raises:
            TypeError: Cannot compare tz-naive and tz-aware datetime-like objects
        Making both tz-naive avoids the mismatch.  If you ever migrate the
        parquet files to UTC-aware timestamps, switch this to tz="UTC" too.
        """
        now = pd.Timestamp.now()          # tz-naive — matches parquet timestamps
        return pd.DataFrame({
            "Machine_ID": [1, 2, 3],
            "event_timestamp": [
                now - pd.Timedelta(hours=1),
                now - pd.Timedelta(hours=2),
                now - pd.Timedelta(hours=3),
            ],
        })

    def test_batch_historical_features(self):
        """Historical retrieval of batch features must return a non-empty DataFrame."""
        training_df = store.get_historical_features(
            entity_df=self._make_entity_df(),
            features=[
                "machine_batch_features:Daily_Vibration_PeakMean_Ratio",
                "machine_batch_features:Weekly_Current_StdDev",
            ],
        ).to_df()

        assert len(training_df) == 3, \
            f"Expected 3 rows, got {len(training_df)}"
        assert "Daily_Vibration_PeakMean_Ratio" in training_df.columns
        assert "Weekly_Current_StdDev" in training_df.columns

    @pytest.mark.xfail(
        reason="Requires streaming_feature_backfill.parquet — run generate_backfill_parquet.py first"
    )
    def test_streaming_historical_features(self):
        """Historical retrieval of streaming features requires the backfill parquet."""
        training_df = store.get_historical_features(
            entity_df=self._make_entity_df(),
            features=[
                "machine_streaming_features:Motor_RPM",
                "machine_streaming_features:Vibration_mm_s",
                "machine_streaming_features:Vibration_RollingMax_10min",
                "machine_streaming_features:Current_Imbalance_Ratio",
            ],
        ).to_df()

        assert len(training_df) == 3
        assert "Motor_RPM" in training_df.columns

    @pytest.mark.xfail(
        reason="Requires streaming_feature_backfill.parquet — run generate_backfill_parquet.py first"
    )
    def test_full_feature_vector_historical(self):
        """Point-in-time join across both views must produce the full training vector."""
        training_df = store.get_historical_features(
            entity_df=self._make_entity_df(),
            features=[
                # Streaming features
                "machine_streaming_features:Current_L1",
                "machine_streaming_features:Vibration_mm_s",
                "machine_streaming_features:Motor_RPM",
                "machine_streaming_features:Vibration_RollingMax_10min",
                "machine_streaming_features:Current_Imbalance_Ratio",
                "machine_streaming_features:Current_Imbalance_RollingMean_5min",
                # Batch features
                "machine_batch_features:Daily_Vibration_PeakMean_Ratio",
                "machine_batch_features:Weekly_Current_StdDev",
            ],
        ).to_df()

        assert len(training_df) == 3, f"Expected 3 rows, got {len(training_df)}"
        # All feature columns must be present (values may still be NaN for old rows)
        expected_cols = {
            "Current_L1", "Vibration_mm_s", "Motor_RPM",
            "Vibration_RollingMax_10min", "Current_Imbalance_Ratio",
            "Current_Imbalance_RollingMean_5min",
            "Daily_Vibration_PeakMean_Ratio", "Weekly_Current_StdDev",
        }
        missing = expected_cols - set(training_df.columns)
        assert not missing, f"Missing columns in training DataFrame: {missing}"


# ===========================================================================
# Standalone entry point (no pytest required)
# ===========================================================================

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
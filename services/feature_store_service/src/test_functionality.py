"""
Test Feast Feature Store Functionality

Covers both FeatureViews introduced by the streaming / batch split:
  • machine_streaming_features  (raw sensors + rolling-window features)
  • machine_batch_features      (daily / weekly aggregation features)

Tests:
  1. Online features – by explicit feature names (streaming view)
  2. Online features – by feature service (both views combined)
  3. Push streaming features – all sensor + rolling-window columns
  4. Materialize streaming batch source to online store
  5. Materialize batch feature view to online store
  6. Historical features from offline store
  7. List all registered feature views
"""

if __name__ == '__main__':

    import pandas as pd
    from datetime import timezone
    from feast import FeatureStore

    store = FeatureStore(repo_path="/feature_store_service")

    DIVIDER = "=" * 80

    print(DIVIDER)
    print("FEAST FEATURE STORE API TESTS")
    print(DIVIDER)

    # =========================================================================
    # TEST 1: Online features – explicit names from machine_streaming_features
    # =========================================================================
    print("\n[TEST 1] Getting online features by name (streaming view)...")

    try:
        features = store.get_online_features(
            features=[
                "machine_streaming_features:Current_L1",
                "machine_streaming_features:Vibration_mm_s",
                "machine_streaming_features:Motor_RPM",
                "machine_streaming_features:Water_Temp_C",
                "machine_streaming_features:Vibration_RollingMax_10min",
                "machine_streaming_features:Current_Imbalance_Ratio",
                "machine_streaming_features:Current_Imbalance_RollingMean_5min",
            ],
            entity_rows=[
                {"Machine_ID": 1},
                {"Machine_ID": 2},
                {"Machine_ID": 3},
            ],
        ).to_dict()

        print("✓ Successfully retrieved streaming features")
        for key, values in features.items():
            print(f"  {key}: {values}")

    except Exception as e:
        print(f"✗ Error: {e}")

    # =========================================================================
    # TEST 2: Online features – explicit names from machine_batch_features
    # =========================================================================
    print("\n[TEST 2] Getting online features by name (batch view)...")

    try:
        features = store.get_online_features(
            features=[
                "machine_batch_features:Daily_Vibration_PeakMean_Ratio",
                "machine_batch_features:Weekly_Current_StdDev",
            ],
            entity_rows=[
                {"Machine_ID": 1},
                {"Machine_ID": 2},
                {"Machine_ID": 3},
            ],
        ).to_dict()

        print("✓ Successfully retrieved batch features")
        for key, values in features.items():
            print(f"  {key}: {values}")

    except Exception as e:
        print(f"✗ Error: {e}")

    # =========================================================================
    # TEST 3: Online features – via FeatureService (both views combined)
    # =========================================================================
    print("\n[TEST 3] Getting online features via feature service (all views)...")

    try:
        feature_service = store.get_feature_service("machine_anomaly_service_v1")

        features = store.get_online_features(
            features=feature_service,
            entity_rows=[
                {"Machine_ID": 1},
                {"Machine_ID": 2},
            ],
        ).to_dict()

        print("✓ Successfully retrieved features via feature service")
        print(f"  Total features returned: {len(features)}")
        print(f"  Feature names: {list(features.keys())}")

    except Exception as e:
        print(f"✗ Error: {e}")

    # =========================================================================
    # TEST 4: Push streaming features (all sensor + rolling-window columns)
    # =========================================================================
    print("\n[TEST 4] Pushing streaming features to online store...")

    try:
        now = pd.Timestamp.now(tz=None)  # timezone-naive to match parquet data

        streaming_data = pd.DataFrame({
            "Machine_ID":                       [1,      2,      3],
            "event_timestamp":                  [now,    now,    now],
            # Raw sensor readings
            "Cycle_Phase_ID":                   [2,      3,      1],
            "Current_L1":                       [12.5,   13.2,   11.8],
            "Current_L2":                       [12.3,   13.1,   11.9],
            "Current_L3":                       [12.4,   13.0,   12.0],
            "Voltage_L_L":                      [230.5,  229.8,  231.2],
            "Water_Temp_C":                     [45.0,   50.5,   42.0],
            "Motor_RPM":                        [1200.0, 1150.0, 1300.0],
            "Water_Flow_L_min":                 [15.5,   16.2,   14.8],
            "Vibration_mm_s":                   [2.3,    2.1,    2.5],
            "Water_Pressure_Bar":               [3.2,    3.1,    3.3],
            # Streaming pipeline features (pre-computed by PySpark)
            "Current_Imbalance_Ratio":          [0.015,  0.018,  0.012],
            "Vibration_RollingMax_10min":        [3.5,    3.2,    3.8],
            "Current_Imbalance_RollingMean_5min": [0.014, 0.017,  0.013],
        })

        store.push(
            push_source_name="washing_stream_push",
            df=streaming_data,
            to="online",
        )

        print("✓ Successfully pushed streaming features")
        print(f"  Pushed {len(streaming_data)} records to online store")

    except Exception as e:
        print(f"✗ Error: {e}")

    # =========================================================================
    # TEST 5: Materialize streaming feature view to online store
    # =========================================================================
    print("\n[TEST 5] Materializing machine_streaming_features to online store...")

    try:
        end_date   = pd.Timestamp.now(tz=None)
        start_date = end_date - pd.Timedelta(days=1)  # 24-hour TTL window

        print(f"  Range: {start_date} → {end_date}")

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["machine_streaming_features"],
        )

        print("✓ Successfully materialized streaming features")

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback; traceback.print_exc()

    # =========================================================================
    # TEST 6: Materialize batch feature view to online store
    # =========================================================================
    print("\n[TEST 6] Materializing machine_batch_features to online store...")

    try:
        end_date   = pd.Timestamp.now(tz=None)
        start_date = end_date - pd.Timedelta(days=7)  # full week for weekly features

        print(f"  Range: {start_date} → {end_date}")

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=["machine_batch_features"],
        )

        print("✓ Successfully materialized batch features")

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback; traceback.print_exc()

    # =========================================================================
    # TEST 7: Historical features from offline store
    # =========================================================================
    print("\n[TEST 7] Getting historical features from offline store...")

    try:
        now = pd.Timestamp.now(tz=timezone.utc)

        entity_df = pd.DataFrame({
            "Machine_ID": [1, 2, 3],
            "event_timestamp": [
                now - pd.Timedelta(hours=1),
                now - pd.Timedelta(hours=2),
                now - pd.Timedelta(hours=3),
            ],
        })

        print(f"  Entity timestamp dtype: {entity_df['event_timestamp'].dtype}")

        training_df = store.get_historical_features(
            entity_df=entity_df,
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

        print("✓ Successfully retrieved historical features")
        print(f"  Retrieved {len(training_df)} rows")
        print("\n  Sample data:")
        print(training_df.head())

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback; traceback.print_exc()

    # =========================================================================
    # TEST 8: List all registered feature views
    # =========================================================================
    print("\n[TEST 8] Listing all registered feature views...")

    try:
        for fv in store.list_feature_views():
            entity_names = [
                e.name if hasattr(e, "name") else str(e) for e in fv.entities
            ]
            print(f"  ✓ {fv.name}")
            print(f"      Entities : {entity_names}")
            print(f"      TTL      : {fv.ttl}")
            print(f"      Features : {len(fv.schema)} fields")
            print(f"      Fields   : {[f.name for f in fv.schema]}")
            print()

    except Exception as e:
        print(f"✗ Error: {e}")

    print(DIVIDER)
    print("TESTS COMPLETED")
    print(DIVIDER)
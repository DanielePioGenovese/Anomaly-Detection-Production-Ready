"""
QUICK TEST - Industrial Washing Machine Dataset Generator
Tests the generator with a smaller dataset for quick validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max

# ============================================================================
# Initialize Spark with minimal config
# ============================================================================

print("Initializing Spark...")
spark = SparkSession.builder \
    .appName("Washer Generator Test") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # Reduce log verbosity

# ============================================================================
# Import generator function
# ============================================================================

from src.industrial_washer_generator import generate_industrial_washer_datasets

# ============================================================================
# Test with smaller dataset (10,000 rows for quick testing)
# ============================================================================

print("\n" + "="*80)
print("TESTING WITH 10,000 ROWS")
print("="*80 + "\n")

normal_df, anomaly_df = generate_industrial_washer_datasets(
    spark=spark,
    num_rows=10_000,
    anomaly_rate=0.02
)

# ============================================================================
# Validation Tests
# ============================================================================

print("\n" + "="*80)
print("RUNNING VALIDATION TESTS")
print("="*80)

test_results = []

# Test 1: Check row counts
print("\n[Test 1] Row Counts")
normal_count = normal_df.count()
anomaly_count = anomaly_df.count()
print(f"  Normal dataset: {normal_count:,} rows")
print(f"  Anomaly dataset: {anomaly_count:,} rows")
test_results.append(("Row counts match", normal_count == anomaly_count == 10_000))

# Test 2: Check anomaly percentage
print("\n[Test 2] Anomaly Percentage")
anomaly_records = anomaly_df.filter(col("is_anomaly") == 1).count()
anomaly_pct = (anomaly_records / anomaly_count) * 100
print(f"  Anomalous records: {anomaly_records}")
print(f"  Anomaly rate: {anomaly_pct:.2f}%")
test_results.append(("Anomaly rate ~2%", 1.5 <= anomaly_pct <= 2.5))

# Test 3: Check schema
print("\n[Test 3] Schema Validation")
expected_cols_normal = [
    "timestamp", "Machine_ID", "Cycle_Phase_ID",
    "Current_L1", "Current_L2", "Current_L3", "Voltage_L_L",
    "Water_Temp_C", "Motor_RPM", "Water_Flow_L_min",
    "Vibration_mm_s", "Water_Pressure_Bar"
]
expected_cols_anomaly = expected_cols_normal + ["is_anomaly"]

normal_cols = normal_df.columns
anomaly_cols = anomaly_df.columns

print(f"  Normal dataset columns: {len(normal_cols)}")
print(f"  Anomaly dataset columns: {len(anomaly_cols)}")
test_results.append(("Normal schema correct", normal_cols == expected_cols_normal))
test_results.append(("Anomaly schema correct", anomaly_cols == expected_cols_anomaly))

# Test 4: Check data ranges
print("\n[Test 4] Data Range Validation")

# Machine IDs should be 1-50
machine_stats = normal_df.select(
    spark_min("Machine_ID").alias("min_machine"),
    spark_max("Machine_ID").alias("max_machine")
).collect()[0]
print(f"  Machine ID range: {machine_stats['min_machine']} - {machine_stats['max_machine']}")
test_results.append(("Machine IDs valid", 
                    machine_stats['min_machine'] >= 1 and machine_stats['max_machine'] <= 50))

# Cycle Phase IDs should be 0-6
phase_stats = normal_df.select(
    spark_min("Cycle_Phase_ID").alias("min_phase"),
    spark_max("Cycle_Phase_ID").alias("max_phase")
).collect()[0]
print(f"  Cycle Phase ID range: {phase_stats['min_phase']} - {phase_stats['max_phase']}")
test_results.append(("Cycle phases valid", 
                    phase_stats['min_phase'] == 0 and phase_stats['max_phase'] == 6))

# Voltage should be around 400V for normal data
voltage_stats = normal_df.select(
    avg("Voltage_L_L").alias("avg_voltage"),
    spark_min("Voltage_L_L").alias("min_voltage"),
    spark_max("Voltage_L_L").alias("max_voltage")
).collect()[0]
print(f"  Voltage range (normal): {voltage_stats['min_voltage']:.1f}V - {voltage_stats['max_voltage']:.1f}V")
print(f"  Average voltage: {voltage_stats['avg_voltage']:.1f}V")
test_results.append(("Voltage range normal", 
                    360 <= voltage_stats['min_voltage'] and voltage_stats['max_voltage'] <= 420))

# Test 5: Check for nulls
print("\n[Test 5] Null Value Check")
null_counts = {}
for col_name in normal_df.columns:
    null_count = normal_df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        null_counts[col_name] = null_count

print(f"  Columns with nulls: {len(null_counts)}")
if null_counts:
    print(f"  Details: {null_counts}")
test_results.append(("No null values", len(null_counts) == 0))

# Test 6: Check anomalies are actually anomalous
print("\n[Test 6] Anomaly Validation")

# Compare max current in normal vs anomalous records
normal_max_current = anomaly_df.filter(col("is_anomaly") == 0) \
    .select(spark_max("Current_L1")).collect()[0][0]
anomalous_max_current = anomaly_df.filter(col("is_anomaly") == 1) \
    .select(spark_max("Current_L1")).collect()[0][0]

print(f"  Max current (normal records): {normal_max_current:.2f}A")
print(f"  Max current (anomalous records): {anomalous_max_current:.2f}A")
test_results.append(("Anomalies have extreme values", 
                    anomalous_max_current > normal_max_current * 1.5))

# Test 7: Check timestamp continuity
print("\n[Test 7] Timestamp Validation")
timestamp_count = normal_df.select("timestamp").distinct().count()
print(f"  Unique timestamps: {timestamp_count:,}")
test_results.append(("Timestamps are unique", timestamp_count == 10_000))

# ============================================================================
# Display Test Results
# ============================================================================

print("\n" + "="*80)
print("TEST RESULTS SUMMARY")
print("="*80 + "\n")

passed = sum(1 for _, result in test_results if result)
total = len(test_results)

for test_name, result in test_results:
    status = "✓ PASS" if result else "✗ FAIL"
    print(f"{status} - {test_name}")

print(f"\nTests passed: {passed}/{total}")

if passed == total:
    print("\n🎉 ALL TESTS PASSED! Generator is working correctly.")
else:
    print(f"\n⚠️  {total - passed} test(s) failed. Please review the generator.")

# ============================================================================
# Display Sample Data
# ============================================================================

print("\n" + "="*80)
print("SAMPLE DATA")
print("="*80)

print("\nNormal Dataset (first 5 rows):")
normal_df.show(5, truncate=False)

print("\nAnomaly Dataset - Normal Records (first 3 rows):")
anomaly_df.filter(col("is_anomaly") == 0).show(3, truncate=False)

print("\nAnomaly Dataset - Anomalous Records (first 5 rows):")
anomaly_df.filter(col("is_anomaly") == 1).show(5, truncate=False)

# ============================================================================
# Statistics by Cycle Phase
# ============================================================================

print("\n" + "="*80)
print("STATISTICS BY CYCLE PHASE")
print("="*80)

anomaly_df.groupBy("Cycle_Phase_ID").agg(
    count("*").alias("total_count"),
    avg("Current_L1").alias("avg_current"),
    avg("Voltage_L_L").alias("avg_voltage"),
    avg("Water_Temp_C").alias("avg_temp"),
    avg("Motor_RPM").alias("avg_rpm")
).orderBy("Cycle_Phase_ID").show()

print("\n" + "="*80)
print("TEST COMPLETE")
print("="*80)
print("\nIf all tests passed, you can now run the full generator:")
print("  python example_usage.py")
print("\nOr import and use directly in your code:")
print("  from industrial_washer_generator import generate_industrial_washer_datasets")

# Clean up
spark.stop()
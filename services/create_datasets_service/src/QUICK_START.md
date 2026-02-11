# 🎯 QUICK START GUIDE - Industrial Washer Dataset Generator

## 📦 What You Got

A complete PySpark package to generate **1,000,000 rows** of industrial washing machine data with:

✅ **TWO datasets**: 
   1. Normal dataset (no anomalies)
   2. Anomaly dataset (2% anomalies + labels)

✅ **13 features**: Realistic electrical, thermal, and mechanical sensor data

✅ **5 anomaly types**: Overcurrent, voltage issues, overheating, vibration, motor malfunction

---

## 🚀 3-Step Quick Start

### Step 1️⃣: Install Dependencies

```bash
pip install pyspark
```

### Step 2️⃣: Run the Generator

```bash
# Quick test (10,000 rows)
python test_generator.py

# Full generation (1,000,000 rows)
python example_usage.py
```

### Step 3️⃣: Use in Your Code

```python
from pyspark.sql import SparkSession
from industrial_washer_generator import generate_industrial_washer_datasets

# Initialize Spark
spark = SparkSession.builder \
    .appName("My Anomaly Detector") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Generate datasets
normal_df, anomaly_df = generate_industrial_washer_datasets(
    spark=spark,
    num_rows=1_000_000,
    anomaly_rate=0.02
)

# Use them!
anomaly_df.show()
```

---

## 📁 Files Included

| File | Purpose |
|------|---------|
| `industrial_washer_generator.py` | Main generator function |
| `example_usage.py` | Complete usage examples |
| `test_generator.py` | Quick validation test |
| `spark_configs.py` | Spark configurations for different scenarios |
| `README.md` | Full documentation |
| `requirements.txt` | Python dependencies |

---

## 🎯 Common Use Cases

### 1. Train Supervised Anomaly Detection Model

```python
# Use anomaly_df with is_anomaly label
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

features = ["Current_L1", "Current_L2", "Current_L3", "Voltage_L_L", 
            "Water_Temp_C", "Motor_RPM", "Vibration_mm_s"]

assembler = VectorAssembler(inputCols=features, outputCol="features")
data = assembler.transform(anomaly_df)

train, test = data.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestClassifier(labelCol="is_anomaly", featuresCol="features")
model = rf.fit(train)

predictions = model.transform(test)
predictions.select("is_anomaly", "prediction", "probability").show()
```

### 2. Unsupervised Anomaly Detection (Isolation Forest / Autoencoder)

```python
# Train on normal_df, test on anomaly_df
# Use normal data to learn "normal" patterns

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=features, outputCol="features")
normal_data = assembler.transform(normal_df)

# Train your model on normal_data
# Apply to anomaly_df to find anomalies
```

### 3. Exploratory Data Analysis

```python
# Check anomaly distribution
anomaly_df.groupBy("is_anomaly", "Cycle_Phase_ID").count().show()

# Find machines with most anomalies
anomaly_df.filter("is_anomaly = 1") \
    .groupBy("Machine_ID").count() \
    .orderBy("count", ascending=False).show()

# Analyze electrical patterns
anomaly_df.groupBy("Cycle_Phase_ID").agg(
    avg("Current_L1").alias("avg_current"),
    avg("Voltage_L_L").alias("avg_voltage")
).show()
```

---

## 💾 Save Datasets

```python
from industrial_washer_generator import save_datasets

# Saves to Parquet + CSV samples
save_datasets(normal_df, anomaly_df, output_path="./my_datasets")
```

Output structure:
```
my_datasets/
├── industrial_washer_normal/              # 1M rows, Parquet
├── industrial_washer_with_anomalies/     # 1M rows + labels, Parquet
├── industrial_washer_normal_sample/      # 10K rows, CSV
└── industrial_washer_with_anomalies_sample/  # 10K rows, CSV
```

---

## 🔧 Customization

### Change Dataset Size

```python
# Smaller dataset for quick experiments
normal_df, anomaly_df = generate_industrial_washer_datasets(
    spark=spark,
    num_rows=100_000,    # 100K instead of 1M
    anomaly_rate=0.02
)
```

### Increase Anomaly Rate

```python
# More anomalies for imbalanced learning experiments
normal_df, anomaly_df = generate_industrial_washer_datasets(
    spark=spark,
    num_rows=1_000_000,
    anomaly_rate=0.05    # 5% anomalies instead of 2%
)
```

### Use Different Spark Config

```python
from spark_configs import get_spark_high_memory

spark = get_spark_high_memory()  # 8GB for large datasets
normal_df, anomaly_df = generate_industrial_washer_datasets(spark)
```

---

## 📊 Dataset Features

| Feature | Description | Unit | Typical Range |
|---------|-------------|------|---------------|
| timestamp | Recording time | DateTime | 30 days |
| Machine_ID | Machine identifier | Integer | 1-50 |
| Cycle_Phase_ID | Washing phase | Integer | 0-6 |
| Current_L1/L2/L3 | 3-phase current | Amperes | 2-35A |
| Voltage_L_L | Line voltage | Volts | 380-420V |
| Water_Temp_C | Water temperature | Celsius | 20-65°C |
| Motor_RPM | Motor speed | RPM | 0-1400 |
| Water_Flow_L_min | Flow rate | L/min | 0-45 |
| Vibration_mm_s | Vibration level | mm/s | 0.5-8.5 |
| Water_Pressure_Bar | Water pressure | Bar | 0.1-2.8 |
| is_anomaly | Anomaly label | 0/1 | 0=normal, 1=anomaly |

---

## 🚨 Anomaly Types (in anomaly_df)

1. **Overcurrent** (30%): Current 2.5-4x normal → Motor overload
2. **Voltage Issues** (25%): Drops to 320V or spikes to 480V → Power problems
3. **Overheating** (20%): Temp 85-100°C → Thermostat failure
4. **Excessive Vibration** (15%): 15-25 mm/s → Unbalanced load
5. **Motor Malfunction** (10%): Wrong RPM for phase → Mechanical failure

---

## 🔬 Washing Cycle Phases

| ID | Phase | Description | Current | RPM |
|----|-------|-------------|---------|-----|
| 0 | Idle | Standby | ~2A | 0 |
| 1 | Fill | Water filling | ~8.5A | 0 |
| 2 | Heat | Heating water | ~35A | ~50 |
| 3 | Wash | Active washing | ~18.5A | ~80 |
| 4 | Rinse | Rinsing | ~12A | ~70 |
| 5 | Spin | High-speed spin | ~28A | ~1400 |
| 6 | Drain | Water draining | ~6A | ~10 |

---

## ✅ Validation Checklist

Run `test_generator.py` to verify:
- ✅ Correct number of rows (1M)
- ✅ Anomaly rate ~2%
- ✅ All features present
- ✅ No null values
- ✅ Data in realistic ranges
- ✅ Anomalies are extreme
- ✅ Timestamps unique

---

## 📚 Next Steps

1. **Explore the data**: `example_usage.py`
2. **Train a model**: Use scikit-learn, XGBoost, or TensorFlow
3. **Evaluate performance**: Precision, Recall, F1-Score, AUC-ROC
4. **Feature engineering**: Create lag features, rolling statistics
5. **Deploy**: Build a real-time anomaly detection system

---

## 🆘 Troubleshooting

### Out of Memory?
```python
# Use smaller dataset
normal_df, anomaly_df = generate_industrial_washer_datasets(
    spark=spark, 
    num_rows=100_000  # Start small
)
```

### Spark Not Starting?
```bash
# Check Java installation
java -version

# Install Java if needed
sudo apt-get install default-jdk
```

### Import Errors?
```bash
pip install --upgrade pyspark
```

---

## 📖 Documentation

- Full guide: `README.md`
- Usage examples: `example_usage.py`
- Quick test: `test_generator.py`
- Configs: `spark_configs.py`

---

## 🎓 Learning Resources

**Topics to explore:**
- Anomaly detection algorithms
- Imbalanced learning (SMOTE, class weights)
- Time series analysis
- Feature engineering
- Model explainability (SHAP, LIME)

**Recommended libraries:**
- PySpark ML
- scikit-learn
- XGBoost
- TensorFlow/PyTorch
- Plotly/Matplotlib

---

## 🎉 You're Ready!

```bash
# Run the test
python test_generator.py

# Generate full datasets
python example_usage.py

# Start building your anomaly detector!
```

**Happy Machine Learning! 🚀**

For questions, check `README.md` or examine the example code.

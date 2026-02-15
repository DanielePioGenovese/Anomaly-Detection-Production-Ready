# Complete Workflow - Training Service V2

## 🎯 Obiettivo Finale

Addestrare un modello **Isolation Forest** su **TUTTO il dataset** ordinato temporalmente, calcolarne metriche, rilevare **drift** dal 2° training, classificare anomalie per severità, e registrare tutto su MLflow.

---

## 📍 FASE 1: STARTUP & INITIALIZATION

### 1.1 Determinazione Training Number

```python
def get_training_number(output_dir):
    metrics_history_file = f"{output_dir}/training_history.json"
    
    if NOT exists(metrics_history_file):
        return 1  # Primo training
    else:
        return len(history) + 1  # N-esimo training
```

**Flusso**:
```
Check file history
├─ File esiste? 
│  ├─ SÌ → Conta elementi → return len + 1
│  └─ NO → return 1 (primo)
```

**Output**: `training_number` ∈ [1, 2, 3, ...]

**Esempio**:
```
Iterazione 1: training_history.json NON esiste → training_number = 1
Iterazione 2: training_history.json contiene 1 elemento → training_number = 2
Iterazione 3: training_history.json contiene 2 elementi → training_number = 3
```

---

## 📍 FASE 2: DATA LOADING (Feast Integration)

### 2.1 Load Entity DataFrame

```python
# Load entity parquet
entity_df = pd.read_parquet(entity_df_path)
# Result:
#    Machine_ID | timestamp             | ...
#    001        | 2024-01-01 10:00:00  |
#    001        | 2024-01-01 10:05:00  |
#    002        | 2024-01-01 10:10:00  |

# Converti timestamp a UTC
entity_df[timestamp_col] = pd.to_datetime(entity_df[timestamp_col], utc=True)
```

**Input**: `entity_df_path` (parquet)  
**Output**: Entity DataFrame con timestamp UTC

### 2.2 Query Feast Feature Store

```python
# Connetti al Feature Store
store = FeatureStore(repo_path=feast_repo_path)

# Recupera feature service
feature_service = store.get_feature_service(feature_service_name)
# Tipicamente: "my_feature_service"

# Query features storiche
df = store.get_historical_features(
    entity_df=entity_df,
    features=feature_service
).to_df()
# Result:
#    Machine_ID | timestamp | vibration | temperature | phase_id | ...
#    001        | ...       | 45.2      | 72.1        | A        |
#    001        | ...       | 48.3      | 73.5        | B        |
#    002        | ...       | 102.1     | 68.2        | A        |
```

**Input**: 
- Entity DataFrame con timestamp
- Feature Service name

**Output**: DataFrame unificato con features

### 2.3 Normalize Timezone

```python
# Assicura coerenza UTC
df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)

logger.info(f"Loaded {len(df)} rows from Feast")
# Result: ~1,000,000 rows (dipende da dati storici)
```

**Output**: Completo DataFrame con ~1M righe ordinate temporalmente

---

## 📍 FASE 3: TEMPORAL ORDERING (NO SPLIT!)

### 3.1 Ordinamento Temporale

```python
# IMPORTANTE: Ordina ma NON splitta!
df = df.sort_values(timestamp_col).reset_index(drop=True)

# Timeline:
# ├─ Jan 2024  (righe 0-200K)         ← Passato
# ├─ Feb 2024  (righe 200K-400K)      ← Passato
# ├─ Mar 2024  (righe 400K-600K)      ← Passato
# ├─ Apr 2024  (righe 600K-800K)      ← Passato
# ├─ May 2024  (righe 800K-900K)      ← Passato
# └─ Jun 2024  (righe 900K-1M)        ← Recente

x_train = df  # TUTTO il dataset
logger.info(f"Dataset totale: {len(x_train)} righe ordinate temporalmente")
# Output: Dataset totale: 1000000 righe ordinate temporalmente
```

**Output**: Intero dataset ordinato (niente split!)

---

## 📍 FASE 4: FEATURE PREPARATION

### 4.1 Drop Non-Predictive Columns

```python
drop_cols = [
    event_timestamp_column,  # "timestamp"
    "Machine_ID"             # identificatore macchina
]

x_train = x_train.drop(columns=[c for c in drop_cols if c in x_train.columns])
# Removed: timestamp, Machine_ID
# Retained: vibration, temperature, pressure, phase_id, ...

logger.info(f"Features selected: {x_train.shape[1]} columns")
# Output: Features selected: 12 columns
```

**Perché droppare?**

| Colonna | Tipo | Motivo Drop |
|---------|------|------------|
| timestamp | Metadata | Usato per ordinamento, non predittivo |
| Machine_ID | ID | Identificatore, non pattern predittivo |

### 4.2 Feature Type Identification

```python
# Identifica automaticamente tipi di feature
num_cols = x_train.select_dtypes(include=["number", "bool"]).columns.tolist()
# Result: ['vibration', 'temperature', 'pressure', 'rpm', ...]

cat_cols = [c for c in x_train.columns if c not in num_cols]
# Result: ['phase_id', 'component_type', ...]

logger.info(f"Numeric: {len(num_cols)} | Categorical: {len(cat_cols)}")
# Output: Numeric: 9 | Categorical: 3
```

**Output**: 
```
x_train shape: (1000000, 12)
├─ Numeric cols: 9 (vibration, temperature, ...)
├─ Categorical cols: 3 (phase_id, component_type, ...)
└─ Ready for pipeline
```

---

## 📍 FASE 5: PIPELINE BUILDING

### 5.1 ColumnTransformer Setup

```python
# Numeric Pipeline
num_pipeline = Pipeline([
    ("imp", SimpleImputer(strategy="median")),      # Impute NaN
    ("scaler", StandardScaler())                     # Scale μ=0, σ=1
])

# Categorical Pipeline
cat_pipeline = Pipeline([
    ("imp", SimpleImputer(strategy="most_frequent")),  # Impute NaN
    ("ohe", OneHotEncoder(handle_unknown="ignore", 
                         sparse_output=False))          # One-Hot Encode
])

# Combine both
preprocessor = ColumnTransformer(
    transformers=[
        ("num", num_pipeline, num_cols),
        ("cat", cat_pipeline, cat_cols)
    ],
    remainder="drop"
)
```

**Preprocessing Detail**:

**Numeric Path**:
```
vibration: [10, 45, NaN, 30, ...]  (raw)
           ├─ ImpNaN → median=30
           │         [10, 45, 30, 30, ...]
           ├─ Scale → μ=0, σ=1
           │         [-0.5, 0.2, 0.0, 0.0, ...]
           └─ Ready for model
```

**Categorical Path**:
```
phase_id: ["A", "B", None, "A", "B", ...]  (raw)
          ├─ ImpNaN → moda="A"
          │          ["A", "B", "A", "A", "B", ...]
          ├─ OneHot → 3 categorie
          │          [1,0,0], [0,1,0], [1,0,0], [1,0,0], ...
          └─ Ready for model
```

### 5.2 Isolation Forest Configuration

```python
model = IsolationForest(
    n_estimators=100,          # 100 decision trees
    contamination=0.05,        # 5% anomalies expected
    random_state=42,           # Reproducibility
    n_jobs=-1                  # Parallel processing
)
```

**Parametri Spiegati**:

| Parametro | Valore | Significato |
|-----------|--------|------------|
| `n_estimators` | 100 | 100 alberi isolamento |
| `contamination` | 0.05 | Assumes 5% anomalies in data |
| `random_state` | 42 | Seed per riproducibilità |
| `n_jobs` | -1 | Usa tutti i CPU cores |

### 5.3 Final Pipeline Assembly

```python
pipe = Pipeline([
    ("pre", preprocessor),       # ColumnTransformer
    ("model", IsolationForest)   # Isolation Forest
])

logger.info("[PIPELINE] Pipeline created successfully")
# Output: Pipeline(steps=[('pre', ColumnTransformer(...)), 
#                        ('model', IsolationForest(...))])
```

**Pipeline Structure**:
```
Input (x_train)
  ├─ named_steps["pre"]
  │  ├─ numeric_transformer
  │  │  ├─ imputer (median)
  │  │  └─ scaler
  │  │
  │  └─ categorical_transformer
  │     ├─ imputer (mode)
  │     └─ one_hot_encoder
  │
  ├─ Transformed Data (all numeric)
  │
  └─ named_steps["model"]
     └─ IsolationForest
```

---

## 📍 FASE 6: MODEL TRAINING

### 6.1 Fit on Training Data

```python
start_train = time.time()

pipe.fit(x_train)  # TUTTO il dataset!

train_time = time.time() - start_train
logger.info(f"Training completed in {train_time:.2f}s")
# Output: Training completed in 45.23s

# Internal steps:
# 1. ColumnTransformer.fit(x_train)
#    ├─ Calcola mediana per numeric
#    ├─ Calcola moda per categorical
#    └─ Memorizza categorie OHE
#
# 2. IsolationForest.fit(X_transformed)
#    ├─ Costruisce 100 alberi di isolamento
#    ├─ Impara distribuzione "normale"
#    └─ Memorizza threshold anomalie
```

**Cosa Impara Isolation Forest**:

- **Pattern Normali**: La distribuzione principale dei dati
- **Outliers**: I 5% dei dati più anomali (contamination=0.05)
- **Threshold**: Soglia di separazione tra normale/anomalo

**Memoria Interna**:
```
Trained Model
├─ Preprocessor params
│  ├─ numeric_scaler: mean=50, std=15 (vibration)
│  ├─ numeric_scaler: mean=70, std=5 (temperature)
│  └─ categorical_encoder: 3 categories
│
└─ Isolation Forest trees (100)
   ├─ Tree 1: decision rules
   ├─ Tree 2: decision rules
   └─ ... (100 trees)
```

---

## 📍 FASE 7: INFERENCE ON TRAINING DATA

### 7.1 Feature Transformation

```python
start_inference = time.time()

# Applica preprocessing
x_train_pre = pipe.named_steps["pre"].transform(x_train)

# Result: shape (1000000, 25)
# - 9 numeric features scaled
# - 16 features from OHE (3 categorical → one-hot)
# Total: 25 numeric features

logger.info("Preprocessing completed")
```

### 7.2 Predictions

```python
# Predizioni: -1 (anomalia) o 1 (normale)
pred_train = pipe.predict(x_train)

# Result:
#   array([ 1,  1, -1,  1,  1,  1, -1,  1, ...])
#   └─ -1 indica anomalia
#   └─ 1 indica comportamento normale

n_anomalies = (pred_train == -1).sum()
logger.info(f"Anomalies detected: {n_anomalies}")
# Output: Anomalies detected: 50012
```

### 7.3 Anomaly Scores

```python
# Scores: da -1.0 (molto anomalo) a +1.0 (molto normale)
scores_train = pipe.named_steps["model"].score_samples(x_train_pre)

# Result:
#   array([-0.8934, -0.1234, 0.5123, -0.3456, 0.7234, ...])
#   └─ Lower score = more anomalous
#   └─ Higher score = more normal

min_score = scores_train.min()
max_score = scores_train.max()
logger.info(f"Score range: [{min_score:.4f}, {max_score:.4f}]")
# Output: Score range: [-0.9812, 0.8876]
```

### 7.4 Latency Measurement

```python
inference_time = time.time() - start_inference
latency = (inference_time * 1000) / len(x_train)

# Per 1M records in 10 seconds:
# latency = (10 * 1000) / 1000000 = 0.01 ms
# OR per 1M records in 1.45 seconds:
# latency = (1.45 * 1000) / 1000000 = 1.45 ms

logger.info(f"Average latency: {latency:.3f}ms per record")
# Output: Average latency: 1.450ms per record
```

**Latency Budget**:
```
Model inference: 1.45ms
├─ Preprocessing: 0.50ms (transform)
├─ Decision making: 0.80ms (predict)
└─ Score calculation: 0.15ms (score_samples)
```

---

## 📍 FASE 8: METRICS CALCULATION

### 8.1 Calculate Metrics

```python
evaluator = ProductionMetricsCalculator(contamination=0.05)

metrics = evaluator.calculate_metrics(
    x_data_pre=x_train_pre,
    predictions=pred_train,
    scores=scores_train,
    latency_ms=latency,
    name="training"
)
```

### 8.2 Metrics Breakdown

**Anomaly Detection Stats**:
```python
n_anomalies = (pred_train == -1).sum()
# Result: 50012

anomaly_rate = (n_anomalies / len(pred_train)) * 100
# Result: 5.0012%
```

**Score Distribution**:
```python
score_statistics = {
    "mean": np.mean(scores_train),      # -0.0812
    "std": np.std(scores_train),        # 0.4210
    "min": np.min(scores_train),        # -0.9812
    "max": np.max(scores_train)         # 0.8876
}

score_distribution = {
    "p1": np.percentile(scores_train, 1),      # -0.9234
    "p5": np.percentile(scores_train, 5),      # -0.7523
    "p50": np.percentile(scores_train, 50),    # -0.0812
    "p95": np.percentile(scores_train, 95),    # 0.4512
    "p99": np.percentile(scores_train, 99)     # 0.7234
}
```

**Latency**:
```python
inference_latency_ms = 1.45
```

### 8.3 Get Thresholds

```python
thresholds = evaluator.get_thresholds(scores_train, pred_train)

# Basati su percentili di training:
thresholds = {
    "p01": np.percentile(scores_train, 1),           # -0.8923
    "p05": np.percentile(scores_train, 5),           # -0.7234
    "p50": np.percentile(scores_train, 50),          # -0.0812
    "observed_max_anomaly": max(scores[pred==-1])    # 0.4521
}

logger.info(f"Thresholds extracted: p01={thresholds['p01']:.4f}")
# Output: Thresholds extracted: p01=-0.8923
```

**Output Metriche**:
```json
{
  "n_anomalies_detected": 50012,
  "anomaly_percentage": 5.0012,
  "score_statistics": {
    "mean": -0.0812,
    "std": 0.4210,
    "min": -0.9812,
    "max": 0.8876
  },
  "score_distribution": {
    "p1": -0.9234,
    "p5": -0.7523,
    "p50": -0.0812,
    "p95": 0.4512,
    "p99": 0.7234
  },
  "inference_latency_ms": 1.45
}
```

---

## 📍 FASE 9: DRIFT DETECTION (Dal 2° Training)

### 9.1 First Training Check

```python
if training_number == 1:
    logger.info("First training - no drift comparison possible")
    print("\n🔵 PRIMO ADDESTRAMENTO - Baseline stabilito")
    return drift_report  # No drift for first run
```

**Output Training #1**:
```
======================================================================
🔵 PRIMO ADDESTRAMENTO - Baseline stabilito
======================================================================
```

### 9.2 Load Previous Metrics (Training #2+)

```python
# Per training_number > 1
previous_metrics_path = f"outputs/metrics_training_{training_number - 1}.json"

if not exists(previous_metrics_path):
    logger.warning(f"Previous metrics not found: {previous_metrics_path}")
    return drift_report  # Early exit

with open(previous_metrics_path, "r") as f:
    previous_metrics = json.load(f)

logger.info(f"Loaded previous metrics from training #{training_number - 1}")
```

**File Structure**:
```
outputs/
├─ metrics_training_1.json  ← Training #1 metrics
├─ metrics_training_2.json  ← Training #2 metrics (previous per training #3)
└─ metrics_training_3.json  ← Current training #3
```

### 9.3 Drift Calculation

**P50 Score Comparison**:
```python
current_p50 = current_metrics["score_distribution"]["p50"]      # -0.1892
previous_p50 = previous_metrics["score_distribution"]["p50"]    # -0.1234

p50_diff = abs(current_p50 - previous_p50)                      # 0.0658
p50_diff_pct = (p50_diff / abs(previous_p50)) * 100             # 15.33%
```

**Anomaly Rate Comparison**:
```python
current_anomaly_rate = current_metrics["anomaly_percentage"]    # 5.45%
previous_anomaly_rate = previous_metrics["anomaly_percentage"]  # 5.00%

anomaly_diff = abs(5.45 - 5.00)                                 # 0.45%
anomaly_diff_pct = (0.45 / 5.00) * 100                          # 9.00%
```

**Score Mean Comparison**:
```python
current_mean = current_metrics["score_statistics"]["mean"]      # -0.1678
previous_mean = previous_metrics["score_statistics"]["mean"]    # -0.1456

mean_diff = abs(-0.1678 - (-0.1456))                            # 0.0222
mean_diff_pct = (0.0222 / abs(-0.1456)) * 100                   # 15.25%
```

**Latency Comparison**:
```python
current_latency = current_metrics["inference_latency_ms"]       # 1.68
previous_latency = previous_metrics["inference_latency_ms"]     # 1.45

latency_diff = abs(1.68 - 1.45)                                 # 0.23
latency_diff_pct = (0.23 / 1.45) * 100                          # 15.86%
```

### 9.4 Drift Level Determination

```python
max_drift_pct = max(p50_diff_pct,          # 15.33%
                    anomaly_diff_pct,      # 9.00%
                    mean_diff_pct,         # 15.25%
                    latency_diff_pct)      # 15.86%
                    # Result: 15.86%

if max_drift_pct > 30:
    drift_level = "CRITICO 🔴"
    action = "⚠️ RETRAINARE IMMEDIATAMENTE!"
elif max_drift_pct > 15:
    drift_level = "MODERATO 🟠"
    action = "⚠️ Monitorare attentamente - Retrainare a breve"
elif max_drift_pct > 5:
    drift_level = "LIEVE 🟡"
    action = "ℹ️ Informativo - Nessuna azione immediata"
else:
    drift_level = "NESSUNO ✅"
    action = "✅ Modello stabile"

# 15.86% → MODERATO 🟠
```

### 9.5 Drift Report Output

```
======================================================================
📊 DRIFT DETECTION - Training #2
======================================================================

🔍 Drift Level: MODERATO 🟠
📈 Max Drift %: 15.86%
🎯 Azione: ⚠️ Monitorare attentamente - Retrainare a breve

📊 Dettagli Metriche:
  P50 Score:      -0.1234 → -0.1892 (+15.33%)
  Anomaly Rate:   5.00% → 5.45% (+9.00%)
  Score Mean:     -0.1456 → -0.1678 (+15.25%)
  Latency (ms):   1.45 → 1.68 (+15.86%)

======================================================================
```

---

## 📍 FASE 10: SEVERITY CLASSIFICATION

### 10.1 Classify Anomalies

```python
# Per ogni anomalia rilevata:
anomaly_indices = np.where(pred_train == -1)[0][:5]

for idx, anomaly_idx in enumerate(anomaly_indices):
    score = scores_train[anomaly_idx]
    severity = evaluator.classify_anomaly_severity(score, thresholds)
    
    print(f"Anomalia #{idx}: Score={score:.4f} → {severity}")
```

### 10.2 Severity Rules

```python
def classify_anomaly_severity(score, thresholds):
    p01 = thresholds["p01"]      # -0.8923
    p05 = thresholds["p05"]      # -0.7234
    p50 = thresholds["p50"]      # -0.0812
    
    if score < p01:              # score < -0.8923
        return "ANOMALIA_GRAVISSIMA 🔴"
    elif score < p05:            # -0.8923 ≤ score < -0.7234
        return "ANOMALIA_GRAVE 🟠"
    elif score < p50:            # -0.7234 ≤ score < -0.0812
        return "ANOMALIA_LIEVE 🟡"
    else:                         # score ≥ -0.0812
        return "NORMALE 🟢"
```

### 10.3 Example Anomalies

```
Anomaly Scores:          Classification:
-0.8934 (< -0.8923)  →  ANOMALIA_GRAVISSIMA 🔴
-0.7834 (p01..p05)   →  ANOMALIA_GRAVE 🟠
-0.4234 (p05..p50)   →  ANOMALIA_LIEVE 🟡
-0.0234 (≥ p50)      →  NORMALE 🟢
+0.3456 (≥ p50)      →  NORMALE 🟢
```

### 10.4 Sample Output

```
======================================================================
📋 SAMPLE SEVERITY ANALYSIS (prime 5 anomalie)
======================================================================
  Anomalia #1: Score=-0.8934 → ANOMALIA_GRAVISSIMA 🔴
  Anomalia #2: Score=-0.6234 → ANOMALIA_GRAVE 🟠
  Anomalia #3: Score=-0.3456 → ANOMALIA_LIEVE 🟡
  Anomalia #4: Score=-0.8765 → ANOMALIA_GRAVISSIMA 🔴
  Anomalia #5: Score=-0.5123 → ANOMALIA_GRAVE 🟠
======================================================================
```

---

## 📍 FASE 11: MLFLOW LOGGING

### 11.1 Log Metrics

```python
mlflow.log_metrics({
    "n_anomalies_detected": 50012,
    "anomaly_rate": 5.0012,
    "latency_ms": 1.45,
    "score_mean": -0.0812,
    "score_std": 0.4210,
    "score_min": -0.9812,
    "score_max": 0.8876,
    "training_number": 2,
    "dataset_size": 1000000
})

logger.info("[MLFLOW] Metrics logged")
```

### 11.2 Log Parameters

```python
mlflow.log_param("score_distribution", json.dumps({
    "p1": -0.9234,
    "p5": -0.7523,
    "p50": -0.0812,
    "p95": 0.4512,
    "p99": 0.7234
}))

mlflow.log_param("training_number", "2")
mlflow.log_param("contamination", "0.05")
mlflow.log_param("drift_level", "MODERATO 🟠")
mlflow.log_param("is_first_training", "false")

logger.info("[MLFLOW] Parameters logged")
```

### 11.3 Create Signature

```python
# Inferisci schema input/output dal modello
signature = create_and_log_signature(x_train, pipe)

# signature mappa:
# Input: DataFrame columns (vibration, temperature, phase_id, ...)
# Output: Predictions (-1 or 1)

logger.info("[MLFLOW] Signature created")
```

**Signature YAML**:
```yaml
inputs:
  - name: vibration
    type: double
  - name: temperature
    type: double
  - name: pressure
    type: double
  - name: phase_id
    type: string
  # ... altre colonne

outputs:
  - name: predictions
    type: integer
```

### 11.4 Log Model

```python
mlflow.sklearn.log_model(
    pipe,
    "model",
    signature=signature,
    registered_model_name="isolation_forest_model"
)

logger.info("[MLFLOW] Model logged and registered")
```

**MLflow Registry State**:
```
Model Registry:
├─ isolation_forest_model
│  ├─ Version 1
│  │  ├─ Stage: None
│  │  ├─ Source: runs:/abc123/artifacts/model
│  │  └─ Created: 2024-01-15 10:15:00
│  │
│  └─ Version 2 (Latest)
│     ├─ Stage: None
│     ├─ Source: runs:/def456/artifacts/model
│     └─ Created: 2024-01-15 11:30:00
```

---

## 📍 FASE 12: ARTIFACTS SAVING

### 12.1 Save Thresholds

```python
os.makedirs(output_dir, exist_ok=True)

with open(f"{output_dir}/thresholds.json", "w") as f:
    json.dump(thresholds, f, indent=2)

mlflow.log_artifact(f"{output_dir}/thresholds.json")

logger.info("[ARTIFACTS] Thresholds saved")
```

**File Content**:
```json
{
  "p01": -0.8923,
  "p05": -0.7234,
  "p50": -0.0812,
  "observed_max_anomaly": 0.4521
}
```

### 12.2 Save Training Metrics

```python
def save_training_metrics(output_dir, training_number, metrics, thresholds, drift_report):
    
    # File specifico per questo training
    metrics_file = f"{output_dir}/metrics_training_{training_number}.json"
    
    training_data = {
        "training_number": 2,
        "timestamp": "2024-01-15 10:30:45",
        "metrics": { ... },
        "thresholds": { ... },
        "drift_report": { ... }
    }
    
    with open(metrics_file, "w") as f:
        json.dump(training_data, f, indent=2)
    
    # Aggiorna history cumulativa
    history_file = f"{output_dir}/training_history.json"
    
    if exists(history_file):
        with open(history_file, "r") as f:
            history = json.load(f)
    else:
        history = []
    
    history.append(training_data)
    
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)
```

**Output Files**:
```
outputs/
├─ metrics_training_1.json
│  └─ {training_number: 1, metrics: {...}, ...}
├─ metrics_training_2.json
│  └─ {training_number: 2, metrics: {...}, ...}
├─ training_history.json
│  └─ [{...}, {...}]  ← Cumulative history
└─ thresholds.json
   └─ {p01: -0.8923, ...}
```

---

## 🔄 Complete Workflow Diagram

```
START
  │
  ├─→ [1] Determine Training Number
  │      └─→ Get from training_history.json
  │
  ├─→ [2] Data Loading (Feast)
  │      ├─→ Load entity parquet
  │      ├─→ Query feature service
  │      ├─→ Merge features
  │      └─→ Normalize timezone (UTC)
  │
  ├─→ [3] Temporal Ordering (NO SPLIT!)
  │      └─→ Sort by timestamp, use ALL data
  │
  ├─→ [4] Feature Preparation
  │      ├─→ Drop timestamp, Machine_ID
  │      └─→ Identify numeric & categorical
  │
  ├─→ [5] Pipeline Building
  │      ├─→ ColumnTransformer (preprocessing)
  │      └─→ IsolationForest (model)
  │
  ├─→ [6] Model Training
  │      └─→ pipe.fit(x_train)
  │
  ├─→ [7] Inference
  │      ├─→ Transform features
  │      ├─→ Predict (-1/+1)
  │      ├─→ Score samples
  │      └─→ Measure latency
  │
  ├─→ [8] Metrics Calculation
  │      ├─→ Anomaly stats
  │      ├─→ Score distribution
  │      └─→ Get thresholds
  │
  ├─→ [9] Drift Detection (if training_number > 1)
  │      ├─→ Load previous metrics
  │      ├─→ Calculate % differences
  │      ├─→ Determine drift level
  │      └─→ Print report
  │
  ├─→ [10] Severity Classification
  │       └─→ Classify sample anomalies
  │
  ├─→ [11] MLflow Logging
  │       ├─→ Log metrics
  │       ├─→ Log parameters
  │       ├─→ Create signature
  │       └─→ Log model
  │
  ├─→ [12] Artifacts Saving
  │       ├─→ Save thresholds.json
  │       ├─→ Save metrics_training_N.json
  │       └─→ Update training_history.json
  │
  └─→ END
     ✅ Training Complete!
     📊 Model Registered in MLflow
     📁 Artifacts Saved Locally
```

---

## ⏱️ Timing Breakdown

```
Phase                              Time    % Total
─────────────────────────────────────────────────
1. Data Loading (Feast)            15s     15%
2. Temporal Ordering               2s      2%
3. Feature Prep                    3s      3%
4. Pipeline Building               1s      1%
5. Model Training                  45s     45%
6. Inference (1M records)          10s     10%
7. Metrics Calculation             2s      2%
8. Drift Detection                 1s      1%
9. Severity Classification         2s      2%
10. MLflow Logging                 5s      5%
11. Artifacts Saving               5s      5%
─────────────────────────────────────────────────
TOTAL                              ~91s    100%
```

---

## 📊 Key Metrics Summary

| Metrica | Valore | Soglia |
|---------|--------|--------|
| **Dataset Size** | 1,000,000 | Total |
| **Features** | 12 | Raw |
| **Anomalies** | 50,012 | 5% |
| **Latency** | 1.45 ms | per record |
| **Score Range** | [-0.98, 0.89] | -1 to +1 |
| **P50 (Median)** | -0.0812 | Reference |
| **Training Time** | 45.23 sec | Total |

---

## 🎯 Next Training (Iteration Loop)

```
Training #2 Completed
  │
  ├─→ outputs/metrics_training_2.json saved
  ├─→ outputs/training_history.json updated
  └─→ New version in MLflow Model Registry

Training #3 (Future)
  │
  ├─→ get_training_number() → 3
  ├─→ Load metrics_training_2.json
  ├─→ Compare metrics for DRIFT DETECTION
  └─→ [Workflow repeats...]
```


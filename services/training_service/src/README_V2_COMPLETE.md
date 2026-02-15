# Training Service V2 - Anomaly Detection with Drift Detection

## 📋 Overview

**Training Service V2** è un'applicazione completa per addestrare, monitorare e registrare modelli di **anomaly detection** basati su **Isolation Forest**. 

**Caratteristiche Principali:**
- ✅ Addestramento su **TUTTO il dataset** ordinato temporalmente (no split)
- ✅ **Drift Detection** automatico dal 2° training in poi
- ✅ **Severity Classification** per anomalie (4 livelli)
- ✅ **Model Versioning** automatico con MLflow
- ✅ **Training History** cumulativa per audit trail

**Use Case**: Rilevamento di anomalie in sensori elettrici (vibrazione, temperatura, corrente) con monitoraggio della salute del modello nel tempo.

---

## 🏗️ Architettura Completa

### Struttura Cartelle
```
training_service/
├── src/
│   ├── utils.py                    # Signature + Latency utilities
│   ├── train.py                    # Main orchestrator (V2)
│   ├── model.py                    # Pipeline builder (Sklearn)
│   ├── load_from_feast_split.py   # Data Manager (Feast integration)
│   └── evaluator.py                # Metrics Calculator + Drift Detection
├── config/
│   └── settings.py                 # Configuration
├── outputs/
│   ├── training_history.json       # Cumulative history
│   ├── metrics_training_1.json     # Training #1 metrics
│   ├── metrics_training_2.json     # Training #2 metrics
│   └── thresholds.json             # Latest thresholds
├── requirements.txt
└── README.md
```

### Stack Tecnologico
| Componente | Versione | Ruolo |
|-----------|----------|-------|
| **scikit-learn** | ≥1.0 | Isolation Forest + Pipeline |
| **MLflow** | ≥2.0 | Model Registry + Experiment Tracking |
| **Feast** | ≥0.30 | Feature Store |
| **Pandas** | ≥1.3 | Data manipulation |
| **NumPy** | ≥1.21 | Numerical operations |

---

## 🔄 Componenti Principali

### 1️⃣ **DataManager** (`load_from_feast_split.py`)
```python
class DataManager:
    - load_data()          → Carica da Feast Feature Store
    - prepare_features()   → Rimuove colonne non predittive
```

**Input**: Entity Parquet + Feast Feature Service  
**Output**: DataFrame ordinato temporalmente

---

### 2️⃣ **ModelFactory** (`model.py`)
```python
class ModelFactory:
    - build_pipeline()     → Crea pipeline sklearn completa
```

**Pipeline Stages**:
1. **ColumnTransformer**: Preprocessing numerico + categorico in parallelo
2. **Isolation Forest**: Modello anomaly detection unsupervised

**Output**: Pipeline sklearn pronta per fit/predict

---

### 3️⃣ **ProductionMetricsCalculator** (`evaluator.py`)
```python
class ProductionMetricsCalculator:
    - calculate_metrics()           → Metriche anomaly detection
    - get_thresholds()             → Soglie percentili per alerting
    - classify_anomaly_severity()  → 4 livelli severità
    - detect_drift()               → Drift detection automatico
```

**Key Features**:
- Calcola metriche su training data
- Percentili: p01, p05, p50, p95, p99
- Drift detection con trigger sensori elettrici
- Severity classification (gravissima/grave/lieve/normale)

---

### 4️⃣ **Training Orchestrator** (`train.py`)
```python
def main():
    1. Data loading + ordinamento
    2. Feature preparation
    3. Pipeline building
    4. Model training
    5. Inference + Metrics
    6. Drift detection
    7. MLflow logging
    8. Artifacts saving
```

**Output**:
- Modello registrato su MLflow
- Metriche e thresholds salvati
- Training history aggiornata

---

### 5️⃣ **Utility Functions** (`utils.py`)
```python
- create_and_log_signature()  → Infer input/output schema
- measure_latency()            → Benchmark performance
```

---

## 🎯 Workflow Completo

### Fase 1: Data Loading
```
┌─────────────────────────────────────┐
│ Caricamento da Feast                │
└──────────────┬──────────────────────┘
               │
        ┌──────▼────────┐
        │ Entity DF      │
        │ Parquet        │
        └──────┬─────────┘
               │
        ┌──────▼──────────────────┐
        │ Feast Query              │
        │ (feature service)        │
        └──────┬───────────────────┘
               │
        ┌──────▼──────────────────┐
        │ Merge + UTC Timezone     │
        │ ~1M righe               │
        └──────┬───────────────────┘
               │
        OUTPUT: DataFrame completo ordinato
```

**Dati di Input**:
- `entity_df_path`: Parquet con Machine_ID, timestamp
- `feature_service`: Features storiche da Feast

**Output**: DataFrame con timestamp UTC

---

### Fase 2: Feature Preparation

```
┌──────────────────────────────┐
│ DataFrame Grezzo             │
│ (timestamp, Machine_ID, ...) │
└──────────────┬───────────────┘
               │
        ┌──────▼────────────────────┐
        │ Drop Colonne Non Predittive│
        │ - timestamp               │
        │ - Machine_ID              │
        └──────┬─────────────────────┘
               │
        ┌──────▼───────────────────┐
        │ Identify Feature Types    │
        │ Numeric: vibration, temp  │
        │ Categorical: phase_id     │
        └──────┬──────────────────┘
               │
        OUTPUT: x_train (12 colonne)
```

**Drop Columns**:
- `timestamp`: metadata per split temporale, non feature
- `Machine_ID`: identificatore, non predittivo

**Feature Types**:
- Numeric: Processing vibrazioni, temperatura, pressione
- Categorical: Cycle_Phase_ID, component_type

---

### Fase 3: Pipeline Building

```
INPUT: x_train (raw features)
  │
  ├─── ColumnTransformer ──────────────────┐
  │                                         │
  ├─► NUMERIC PATH:                       │
  │   • Impute NaN (mediana)              │
  │   • StandardScale (μ=0, σ=1)          │
  │                                        │
  ├─► CATEGORICAL PATH:                   │
  │   • Impute NaN (moda)                 │
  │   • One-Hot Encode                    │
  │                                        │
  └────────────────┬──────────────────────┘
                   │
           ┌───────▼──────────┐
           │ Transformed Data  │
           │ (tutti numerici)  │
           └───────┬──────────┘
                   │
           ┌───────▼──────────────────┐
           │ Isolation Forest          │
           │ • n_estimators: 100       │
           │ • contamination: 0.05     │
           │ • n_jobs: -1              │
           └───────┬──────────────────┘
                   │
OUTPUT: predictions (-1/+1)
        scores (anomaly severity)
```

**Preprocessing Dettagliato**:

**Numeric**:
```
vibration: [10, 45, 1000, 30]  (raw)
           ↓ Impute NaN
           ↓ Scale μ=0, σ=1
           → [-0.5, 0.2, 3.1, 0.0]
```

**Categorical**:
```
phase_id: ["A", "B", None, "A"]  (raw)
          ↓ Impute NaN (moda="A")
          ↓ One-Hot Encode
          → [1,0,0], [0,1,0], [1,0,0], [1,0,0]
```

---

### Fase 4: Model Training

```
┌────────────────────────┐
│ Input: x_train         │
│ (raw features)         │
└────────────┬───────────┘
             │
      ┌──────▼──────────┐
      │ ColumnTransformer│
      │ .fit()           │
      │ (learn params)   │
      └──────┬───────────┘
             │
      ┌──────▼──────────┐
      │ Isolation Forest │
      │ .fit()           │
      │ (learn patterns) │
      └──────┬───────────┘
             │
OUTPUT: Modello addestrato
        (100 decision trees)
        (memorizzati su "normalità")
```

**Cosa Impara**:
- Pattern normali dai dati storici
- Outliers naturali → anomalie
- Threshold: contamination=5% (top 5% anomalies)

---

### Fase 5: Inference & Metrics

```
┌────────────────────────┐
│ Input: x_train         │
│ (dati di training)     │
└────────────┬───────────┘
             │
      ┌──────▼──────────────────┐
      │ Trasforma Features       │
      │ (applica preprocessing)  │
      └──────┬───────────────────┘
             │
      ┌──────▼──────────────────┐
      │ Predizioni               │
      │ pred_train: [-1, 1, -1]  │
      │ (anomaly/normal)         │
      └──────┬───────────────────┘
             │
      ┌──────▼──────────────────┐
      │ Anomaly Scores           │
      │ scores_train: [-0.8, ...]│
      │ (-1=anomalo, +1=normale) │
      └──────┬───────────────────┘
             │
OUTPUT: predictions, scores, latency
```

**Interpretazione Score**:
```
score ∈ [-1.0, +1.0]

-1.0 ◀────────────────► +1.0
  │                      │
Molto               Molto
Anomalo            Normale
```

---

### Fase 6: Metrics Calculation

```
┌──────────────────────────┐
│ Calcolo Metriche         │
│ su TRAINING DATA         │
└────────────┬─────────────┘
             │
      ┌──────▼─────────────────────────────┐
      │ Score Statistics:                   │
      │  - mean: -0.15                     │
      │  - std: 0.42                       │
      │  - min: -0.98                      │
      │  - max: 0.87                       │
      └──────┬──────────────────────────────┘
             │
      ┌──────▼──────────────────┐
      │ Anomaly Count & Rate:    │
      │  - n_anomalies: 10000    │
      │  - rate: 5.0%            │
      └──────┬───────────────────┘
             │
      ┌──────▼──────────────────┐
      │ Percentili Distribution: │
      │  - p01: -0.92            │
      │  - p05: -0.75            │
      │  - p50: -0.10            │
      │  - p95: 0.45             │
      │  - p99: 0.72             │
      └──────┬───────────────────┘
             │
      ┌──────▼──────────────────┐
      │ Inference Latency:       │
      │  - latency_ms: 1.5       │
      └──────┬───────────────────┘
             │
OUTPUT: metrics dict (completo)
```

---

### Fase 7: Drift Detection (NEW!)

```
┌──────────────────────────────┐
│ Training #1?                 │
└────┬───────────────────┬─────┘
     │ SÌ                │ NO
     │                   │
     │          ┌────────▼────────────────────┐
     │          │ Carica metrics training N-1  │
     │          └────────┬────────────────────┘
     │                   │
     │          ┌────────▼────────────────────┐
     │          │ Confronta Metriche:          │
     │          │ - p50 score                 │
     │          │ - anomaly_rate              │
     │          │ - score_mean                │
     │          │ - latency                   │
     │          └────────┬────────────────────┘
     │                   │
     │          ┌────────▼──────────────────────────┐
     │          │ Calcola % Differenza:             │
     │          │ |current - previous| / |previous| │
     │          └────────┬──────────────────────────┘
     │                   │
     │          ┌────────▼────────────────────────┐
     │          │ Determina Drift Level:           │
     │          │ > 30% → CRITICO 🔴              │
     │          │ > 15% → MODERATO 🟠            │
     │          │ > 5%  → LIEVE 🟡              │
     │          │ ≤ 5%  → NESSUNO ✅             │
     │          └────────┬────────────────────────┘
     │                   │
     └───────┬──────────┘
             │
    OUTPUT: drift_report
             (level, details, action)
```

**Trigger Drift (Sensori Elettrici)**:
```
CRITICO 🔴:  > 30% di differenza
├─ Possibile guasto sensore
├─ Pattern dati radicalmente cambiato
└─ AZIONE: Retrainare IMMEDIATAMENTE

MODERATO 🟠: 15-30% di differenza
├─ Lenta degradazione modello
├─ Possibile cambio stagionale/processo
└─ AZIONE: Monitorare, retrainare presto

LIEVE 🟡: 5-15% di differenza
├─ Variabilità naturale
├─ Niente di preoccupante
└─ AZIONE: Informativo

NESSUNO ✅: ≤ 5%
├─ Modello stabile
└─ AZIONE: Continua monitoraggio
```

---

### Fase 8: Severity Classification

```
┌──────────────────────────────┐
│ Per ogni anomalia rilevata:   │
│ score vs thresholds           │
└────────────┬─────────────────┘
             │
      ┌──────▼───────────────────────────┐
      │ score < p01 (-0.89)?              │
      │ YES → ANOMALIA_GRAVISSIMA 🔴     │
      │ NO  → NEXT CHECK                 │
      └──────┬───────────────────────────┘
             │
      ┌──────▼───────────────────────────┐
      │ score < p05 (-0.72)?              │
      │ YES → ANOMALIA_GRAVE 🟠          │
      │ NO  → NEXT CHECK                 │
      └──────┬───────────────────────────┘
             │
      ┌──────▼───────────────────────────┐
      │ score < p50 (-0.08)?              │
      │ YES → ANOMALIA_LIEVE 🟡          │
      │ NO  → NEXT CHECK                 │
      └──────┬───────────────────────────┘
             │
      ┌──────▼───────────────────────────┐
      │ score >= p50?                     │
      │ YES → NORMALE 🟢                 │
      └──────┬───────────────────────────┘
             │
OUTPUT: severity string
```

**Esempio**:
```
Anomalia #1: Score=-0.8934 → ANOMALIA_GRAVISSIMA 🔴
Anomalia #2: Score=-0.6234 → ANOMALIA_GRAVE 🟠
Anomalia #3: Score=-0.3456 → ANOMALIA_LIEVE 🟡
```

---

### Fase 9: MLflow Logging

```
┌──────────────────────────┐
│ Metriche Numeriche       │
│ (log_metrics)            │
├──────────────────────────┤
│ - anomaly_rate           │
│ - latency_ms             │
│ - score_mean, std, ...   │
│ - training_number        │
└────────────┬─────────────┘
             │
      ┌──────▼──────────────┐
      │ Parametri String     │
      │ (log_param)          │
      ├──────────────────────┤
      │ - score_distribution │
      │ - drift_level        │
      │ - contamination      │
      └────────────┬─────────┘
             │
      ┌──────▼──────────────┐
      │ Modello              │
      │ (log_model)          │
      ├──────────────────────┤
      │ - pipeline sklearn   │
      │ - signature          │
      │ - registered_name    │
      └────────────┬─────────┘
             │
      ┌──────▼──────────────┐
      │ Artifacts            │
      │ (log_artifact)       │
      ├──────────────────────┤
      │ - thresholds.json    │
      └────────────┬─────────┘
             │
OUTPUT: Run in MLflow
        ├─ Metrics
        ├─ Parameters
        ├─ Model artifact
        └─ Training history
```

---

### Fase 10: Artifacts Saving

```
┌──────────────────────────┐
│ Salva Artifacts Locali   │
└────────────┬─────────────┘
             │
      ┌──────▼────────────────────────┐
      │ metrics_training_N.json        │
      │ (metriche specifiche training) │
      └────────────┬───────────────────┘
             │
      ┌──────▼────────────────────────┐
      │ training_history.json          │
      │ (storia cumulativa)            │
      └────────────┬───────────────────┘
             │
      ┌──────▼────────────────────────┐
      │ thresholds.json                │
      │ (soglie per produzione)        │
      └────────────┬───────────────────┘
             │
OUTPUT: Files saved in output_dir
```

---

## 📊 Output Esempio Completo

### Console Output

```
======================================================================
🚀 TRAINING #2
======================================================================

[INFO] [MAIN] Avvio Training #2
[INFO] [DATA] Caricamento dati...
[INFO] [DATA] Dataset totale: 1,000,000 righe ordinate temporalmente
[INFO] [DATA] Features selezionate: 12 colonne
[INFO] [PIPELINE] Costruzione pipeline...
[INFO] [TRAIN] Fitting pipeline su TUTTO il dataset...
[INFO] [TRAIN] Training completato in 45.23s
[INFO] [INFERENCE] Calcolo predizioni e scores su training data...
[INFO] [INFERENCE] Latency media: 1.45ms per record
[INFO] [EVAL] Anomalie rilevate: 10012 (5.01%)
[INFO] [EVAL] Score distribution p50: -0.0812

======================================================================
📊 DRIFT DETECTION - Training #2
======================================================================

🔍 Drift Level: MODERATO 🟠
📈 Max Drift %: 18.35%
🎯 Azione: ⚠️ Monitorare attentamente - Retrainare a breve

📊 Dettagli Metriche:
  P50 Score:      -0.1234 → -0.1892 (+15.33%)
  Anomaly Rate:   5.00% → 5.45% (+9.00%)
  Score Mean:     -0.1456 → -0.1678 (+15.25%)
  Latency (ms):   1.45 → 1.68 (+15.86%)

======================================================================

======================================================================
📋 SAMPLE SEVERITY ANALYSIS (prime 5 anomalie)
======================================================================
  Anomalia #1: Score=-0.8934 → ANOMALIA_GRAVISSIMA 🔴
  Anomalia #2: Score=-0.6234 → ANOMALIA_GRAVE 🟠
  Anomalia #3: Score=-0.3456 → ANOMALIA_LIEVE 🟡
  Anomalia #4: Score=-0.8765 → ANOMALIA_GRAVISSIMA 🔴
  Anomalia #5: Score=-0.5123 → ANOMALIA_GRAVE 🟠

======================================================================

[INFO] [MLFLOW] Log metriche...
[INFO] [MLFLOW] Creazione signature...
[INFO] [MLFLOW] Log model...
[INFO] [ARTIFACTS] Salvataggio artifacts...
[INFO] [SAVE] Metriche training salvate: outputs/metrics_training_2.json
[INFO] [SAVE] Storia training aggiornata: outputs/training_history.json
✅ Training #2 completato con successo!
```

### MLflow Run

```
Experiment: anomaly_detection
├─ Run: 2024-01-15_10-30-45
│  ├─ Metrics:
│  │  ├─ anomaly_rate: 5.01%
│  │  ├─ latency_ms: 1.45
│  │  ├─ score_mean: -0.0812
│  │  ├─ score_std: 0.421
│  │  ├─ training_number: 2
│  │  └─ dataset_size: 200000
│  │
│  ├─ Parameters:
│  │  ├─ score_distribution: {...}
│  │  ├─ drift_level: MODERATO 🟠
│  │  ├─ contamination: 0.05
│  │  └─ is_first_training: false
│  │
│  ├─ Artifacts:
│  │  ├─ model/
│  │  │  ├─ MLmodel
│  │  │  ├─ model.pkl
│  │  │  └─ signature.yaml
│  │  │
│  │  └─ thresholds.json
│  │
│  └─ Tags:
│     └─ [auto-generated]
```

### JSON Output (metrics_training_2.json)

```json
{
  "training_number": 2,
  "timestamp": "2024-01-15 10:30:45",
  "metrics": {
    "n_anomalies_detected": 10012,
    "anomaly_percentage": 5.01,
    "score_statistics": {
      "mean": -0.0812,
      "std": 0.421,
      "min": -0.981,
      "max": 0.876
    },
    "score_distribution": {
      "p1": -0.9234,
      "p5": -0.7523,
      "p50": -0.0812,
      "p95": 0.4512,
      "p99": 0.7234
    },
    "inference_latency_ms": 1.45
  },
  "thresholds": {
    "p01": -0.8923,
    "p05": -0.7234,
    "p50": -0.0812,
    "observed_max_anomaly": 0.4521
  },
  "drift_report": {
    "is_first_training": false,
    "training_number": 2,
    "drift_detected": true,
    "drift_level": "MODERATO 🟠",
    "drift_details": {
      "p50_score": {
        "previous": -0.1234,
        "current": -0.1892,
        "diff_pct": 15.33
      },
      "anomaly_rate": {
        "previous": 5.00,
        "current": 5.45,
        "diff_pct": 9.00
      },
      "score_mean": {
        "previous": -0.1456,
        "current": -0.1678,
        "diff_pct": 15.25
      },
      "latency": {
        "previous": 1.45,
        "current": 1.68,
        "diff_pct": 15.86
      }
    }
  }
}
```

---

## 🔧 Configuration

### Settings (`config/settings.py`)

```python
class Settings:
    # Feast
    feast_repo_path: str = "/path/to/feast/repo"
    entity_df_path: str = "/path/to/entity.parquet"
    event_timestamp_column: str = "timestamp"
    feature_service_name: str = "my_feature_service"
    
    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment_name: str = "anomaly_detection"
    mlflow_model_name: str = "isolation_forest_model"
    
    # Training Config
    training:
        if_n_estimators: int = 100
        contamination: float = 0.05      # 5% anomalies expected
        random_state: int = 42
    
    # Output
    output_dir: str = "./outputs"
```

### Key Parameters

| Parametro | Valore | Descrizione |
|-----------|--------|-------------|
| **contamination** | 0.05 | % anomalie attese (sensori: 3-10%) |
| **if_n_estimators** | 100 | Numero alberi Isolation Forest |
| **drift_critico** | >30% | Trigger per retraining urgente |
| **drift_moderato** | >15% | Trigger per monitoraggio |
| **drift_lieve** | >5% | Trigger per info |

---

## 🚀 Come Usare

### 1. Installazione

```bash
cd training_service/
pip install -r requirements.txt
```

### 2. Configurazione

Modifica `config/settings.py` con i tuoi paths:
```python
settings.feast_repo_path = "/path/to/your/feast"
settings.entity_df_path = "/path/to/entity.parquet"
settings.output_dir = "./outputs"
```

### 3. Esecuzione

```bash
# Training #1
python src/train.py

# Output:
# 🚀 TRAINING #1
# [Processing...]
# ✅ TRAINING #1 COMPLETATO

# Training #2 (con drift detection)
python src/train.py

# Output:
# 🚀 TRAINING #2
# [Processing...]
# 📊 DRIFT DETECTION - Training #2
# 🔍 Drift Level: MODERATO 🟠
# [...]
# ✅ TRAINING #2 COMPLETATO
```

### 4. Visualizzare Risultati

```bash
mlflow ui --host 0.0.0.0 --port 5000
# → http://localhost:5000
```

---

## 📈 Monitoring & Alerting

### Quando Retrainare?

| Situazione | Azione | Timeline |
|-----------|--------|----------|
| Drift CRITICO 🔴 (>30%) | Retrainare subito | Immediatamente |
| Drift MODERATO 🟠 (15-30%) | Pianificare retraining | Entro 1 settimana |
| Drift LIEVE 🟡 (5-15%) | Monitorare | Routine |
| Nessun drift ✅ | Nulla | Continua monitoring |

### Interpretazione Severità

```
ANOMALIA_GRAVISSIMA 🔴
├─ Score < p01 (1° percentile)
├─ Altissimo rischio
└─ AZIONE: Controllare immediatamente

ANOMALIA_GRAVE 🟠
├─ Score < p05 (5° percentile)
├─ Rischio significativo
└─ AZIONE: Indagare

ANOMALIA_LIEVE 🟡
├─ Score < p50 (mediana)
├─ Anomalia rilevata
└─ AZIONE: Monitorare

NORMALE 🟢
├─ Score ≥ p50
├─ Comportamento normale
└─ AZIONE: Nulla
```

---

## 🐛 Troubleshooting

| Problema | Causa | Soluzione |
|----------|-------|-----------|
| "Feature not found" | Feast non disponibile | Verificare `feature_service_name` |
| "First training" stampato 2 volte | Normale per training #1 | Aspettare training #2 |
| Drift sempre NESSUNO | Metriche identiche | Dati molto stabili (buono!) |
| MLflow non logga | Server offline | `mlflow ui` e rieseguire |
| Memory error | Dataset troppo grande | Usare parallelizzazione Feast |

---

## 📝 Prossimi Step

- [ ] API REST per inference in produzione
- [ ] Dashboard Grafana per visualizzazione
- [ ] Automated retraining scheduler
- [ ] Data quality checks pre-training
- [ ] Model registry transitions (staging→prod)
- [ ] A/B testing tra versioni

---

## 📞 Support

Per domande o problemi:
1. Consulta CHANGELOG_V2.md
2. Consulta WORKFLOW.md
3. Controlla logs in `outputs/`
4. Verifica MLflow UI: http://localhost:5000


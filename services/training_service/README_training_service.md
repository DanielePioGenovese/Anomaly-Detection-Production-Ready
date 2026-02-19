# Training Service

Microservizio responsabile del training del modello di anomaly detection su dati industriali di lavatrici. Addestra una pipeline sklearn (preprocessing + Isolation Forest), registra tutto su MLflow e salva artifacts e metriche in locale.

---

## Struttura

```
training_service/
├── Dockerfile
├── config/
│   ├── __init__.py
│   └── settings.py         # Configurazione centralizzata (Pydantic)
└── src/
    ├── train.py             # Entry point: orchestra l'intero training
    ├── model.py             # ModelFactory: costruisce la pipeline sklearn
    ├── evaluator.py         # ProductionMetricsCalculator: calcola metriche e thresholds
    ├── load_from_feast.py   # DataManager: carica dati dal Feature Store (Feast)
    └── utils.py             # Utility: signature MLflow, misura latency
```

---

## Come funziona

Il training segue questi step in sequenza:

**1. Caricamento dati** — `DataManager` legge l'entity DataFrame da Parquet, poi chiede a Feast i dati storici tramite il FeatureService configurato.

**2. Costruzione pipeline** — `ModelFactory.build_pipeline()` assembla una Pipeline sklearn con:
- `ColumnTransformer`: preprocessing parallelo per colonne numeriche (mediana + StandardScaler) e categoriche (imputazione costante + OneHotEncoder)
- `IsolationForest`: anomaly detector non supervisionato

**3. Training** — La pipeline viene fittata sull'intero dataset (nessuno split train/test: il modello apprende la distribuzione normale).

**4. Inference & Evaluation** — Su tutto il dataset vengono calcolati predictions (`-1` anomalia, `1` normale) e anomaly scores (valori continui, più negativi = più anomali).

**5. Thresholds** — Vengono estratti i percentili chiave degli scores (`p01`, `p05`, `p50`, `observed_max_anomaly`) da usare in produzione per classificare nuovi record.

**6. Logging su MLflow** — Metriche, parametri, pipeline completa e `thresholds.json` vengono registrati nella run MLflow attiva.

**7. Persistenza locale** — Metriche e thresholds vengono salvati in `outputs/` con storia cumulativa tra training successivi.

---

## Configurazione

Tutte le impostazioni sono in `config/settings.py` tramite Pydantic Settings. Possono essere sovrascritte da variabili d'ambiente o file `.env`.

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | URI del server MLflow |
| `MLFLOW_EXPERIMENT_NAME` | `isolation_forest_prod` | Nome esperimento MLflow |
| `MLFLOW_MODEL_NAME` | `if_anomaly_detector` | Nome modello nel Model Registry |
| `ENTITY_DF_PATH` | `data/synthetic_datasets/industrial_washer_with_anomalies` | Path al Parquet con le entità |
| `FEAST_REPO_PATH` | `services/feature_store_service/config` | Path al repository Feast |
| `FEATURE_SERVICE_NAME` | `machine_anomaly_service_v1` | Nome del FeatureService Feast |
| `EVENT_TIMESTAMP_COLUMN` | `timestamp` | Nome colonna timestamp |
| `OUTPUT_DIR` | `outputs` | Directory per artifacts locali |

### Iperparametri Isolation Forest

Configurati in `TrainingConfig` dentro `settings.py`:

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `contamination` | `0.02` | Percentuale attesa di anomalie nel dataset |
| `if_n_estimators` | `100` | Numero di alberi nell'ensemble |
| `random_state` | `42` | Seed per riproducibilità |

---

## Output

Dopo ogni training vengono prodotti:

```
outputs/
├── thresholds.json                  # Soglie per classificazione in produzione
├── metrics_training_<N>.json        # Metriche del training N
└── training_history.json            # Storia cumulativa di tutti i training
```

Su MLflow viene registrato:
- **Metriche**: `n_anomalies_detected`, `anomaly_rate`, `latency_ms`, `score_mean/std/min/max`, `dataset_size`, `training_number`
- **Parametri**: `contamination`, `training_number`, `score_distribution`
- **Modello**: intera pipeline sklearn (preprocessing + IsolationForest) con signature
- **Artifact**: `thresholds.json`

---

## Esecuzione

### Con Docker Compose (consigliato)

Il servizio usa il profile `training` e dipende da MLflow:

```bash
docker compose --profile training up training_pipeline
```

### Standalone (sviluppo locale)

```bash
# Dalla root del progetto
PYTHONPATH=services/training_service uv run -m src.train
```

---

## Dipendenze principali

Gestite tramite `uv` nel gruppo `training-pipeline` del `pyproject.toml`:

- `scikit-learn` — Pipeline, preprocessing, IsolationForest
- `mlflow` — Experiment tracking e Model Registry
- `feast` — Feature Store per il caricamento dati
- `pandas` / `numpy` — Manipolazione dati
- `pydantic-settings` — Configurazione tipizzata

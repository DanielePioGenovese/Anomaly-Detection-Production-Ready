# MLflow Service

Server MLflow per il tracking degli esperimenti, la gestione del Model Registry e la visualizzazione delle metriche del progetto di anomaly detection.

---

## Struttura

```
mlflow/
├── __init__.py
└── Dockerfile
```

Il servizio è volutamente minimale: contiene solo il Dockerfile per avviare il server MLflow. Tutta la configurazione avviene tramite variabili d'ambiente e volumi Docker.

---

## Come funziona

MLflow viene avviato come server HTTP sulla porta `5000`. I dati vengono salvati nel filesystem del container e persistiti tramite volume Docker.

- **Backend store** (`/mlruns`): salva metadata di runs, esperimenti, parametri e metriche
- **Artifact root** (`/mlruns/artifacts`): salva i file binari (modelli, JSON, ecc.)
- **UI**: accessibile su `http://localhost:5000` dal browser

Il `training_service` si connette a questo server per registrare ogni run di training.

---

## Configurazione

| Variabile d'ambiente | Default nel Dockerfile | Descrizione |
|----------------------|----------------------|-------------|
| `MLFLOW_BACKEND_STORE_URI` | `file:///mlruns` | Dove salvare metadata (runs, metriche, parametri) |
| `MLFLOW_DEFAULT_ARTIFACT_ROOT` | `/mlruns/artifacts` | Dove salvare artifacts binari (modelli, file) |
| `PYTHONUNBUFFERED` | `1` | Log in tempo reale senza buffering |

---

## Persistenza

I dati MLflow vengono salvati nel volume `mlflow_data` mappato su `/mlruns` nel container:

```yaml
volumes:
  - mlflow_data:/mlruns
```

Questo garantisce che esperimenti, runs e modelli sopravvivano al riavvio del container. Senza questo volume ogni riavvio azzera tutto lo storico.

---

## Esecuzione

Il servizio si avvia automaticamente con Docker Compose:

```bash
docker compose up mlflow
```

Una volta avviato, la UI è disponibile su:

```
http://localhost:5000
```

### Healthcheck

Il Dockerfile include un healthcheck automatico:
- Intervallo: ogni 30 secondi
- Timeout: 10 secondi
- Tentativi: 3 prima di marcare il container come `unhealthy`

Il `training_service` ha `depends_on: mlflow` nel compose, quindi attende che MLflow sia healthy prima di avviarsi.

---

## Cosa viene registrato dal Training Service

Ogni run di training registra su MLflow:

**Metriche**
- `n_anomalies_detected` — numero di anomalie rilevate
- `anomaly_rate` — percentuale di anomalie sul dataset
- `latency_ms` — latenza media per record in ms
- `score_mean`, `score_std`, `score_min`, `score_max` — statistiche degli anomaly scores
- `dataset_size` — dimensione del dataset usato
- `training_number` — numero progressivo del training

**Parametri**
- `contamination` — soglia di contaminazione Isolation Forest
- `training_number` — numero progressivo del training
- `score_distribution` — distribuzione percentili degli scores (p1, p5, p50, p95, p99)

**Artifacts**
- Pipeline sklearn completa (preprocessing + IsolationForest) con signature
- `thresholds.json` — soglie per classificazione in produzione

---

## Dipendenze

Gestite tramite `uv` nel gruppo `mlflow` del `pyproject.toml`:

- `mlflow` — server, UI e Model Registry

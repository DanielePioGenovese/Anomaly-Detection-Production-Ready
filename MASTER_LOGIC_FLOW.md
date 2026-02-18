# 🧠 MASTER LOGIC FLOW: Washing Machines Anomaly Detection

> **Documento Definitivo di Spiegazione Logica**
> Questo file descrive il flusso dati e decisionale del progetto "dalla A alla Z", unendo la visione infrastrutturale (Docker) a quella algoritmica (Machine Learning).
> Usalo come copione per raccontare "la storia" del dato, dalla sua nascita fino alla predizione.

---

# 📚 Indice del Flusso
1.  [LA GENESI: Generazione Dati (Batch)](#1-la-genesi-generazione-dati-batch)
2.  [L'APPRENDIMENTO: Feature Engineering & Training](#2-lapprendimento-feature-engineering--training)
3.  [LA MEMORIA: Il Feature Store (Offline -> Online)](#3-la-memoria-il-feature-store-offline---online)
4.  [IL TEMPO REALE: Streaming & Ingestion](#4-il-tempo-reale-streaming--ingestion)
5.  [LA DECISIONE: Inferenza & Predizione](#5-la-decisione-inferenza--predizione)
6.  [IL CERVELLO: Monitoring & Retraining](#6-il-cervello-monitoring--retraining)

---

# 1. LA GENESI: Generazione Dati (Batch)
**Attori**: `create_datasets_service`, `PySpark`, `SDV`

Tutto inizia nel silenzio. Non abbiamo sensori reali collegati a vere lavatrici, quindi dobbiamo crearne una simulazione credibile.
1.  **Avvio**: Il comando `make pipeline` sveglia il container `create_datasets`.
2.  **Sintesi**: Usiamo una libreria chiamata **SDV (Synthetic Data Vault)**. È un modello generativo che ha "studiato" come si comportano le lavatrici (corrente, voltaggio, vibrazioni).
3.  **Il Dato**: Viene creato un DataFrame PySpark massivo che simula **mesi di operatività** per 5 macchine diverse. Ogni riga è un "heartbeat" ogni 30 secondi:
    *   *Machine_ID*: 1, 2, 3...
    *   *Timestamp*: 2024-01-01 08:00:00...
    *   *Sensori*: Current_L1 (Ampere), Vibration (mm/s), etc.
4.  **Destinazione**: I dati vengono salvati in formato **Parquet** (`data/synthetic_data_creation/result.parquet`). Usiamo Parquet perché conserva i "tipi" (sa che 10.5 è un *float*, non una stringa) ed è compresso.

> **Logica**: Senza dati storici coerenti, non potremmo addestrare nulla. SDV garantisce che le correlazioni (es. *Alta Vibrazione + Alta Corrente = Guasto*) siano rispettate.

---

# 2. L'APPRENDIMENTO: Feature Engineering & Training
**Attori**: `hist_ingestion_service`, `training_service`, `Sklearn`, `MLflow`

Ora abbiamo i dati grezzi. Ma un modello AI non mangia dati grezzi, mangia **Features**.

### 2.1 L'Ingestione Storica (`hist_ingestion`)
Il container si sveglia e legge il Parquet generato prima. Il suo compito è duplice:
1.  **Calcolo delle Rolling Windows**:
    *   Una vibrazione di "50" è alta? Dipende. Se la media degli ultimi 10 minuti era "5", allora sì.
    *   Usiamo Spark per calcolare finestre mobili: aggiungiamo colonne come `Vibration_rollingMean_10min` e `Current_rollingStd_10min`. Queste sono le vere "feature" predittive.
2.  **Fitting del Preprocessor**:
    *   Le reti neurali e gli algoritmi come Isolation Forest odiano i numeri grandi. Vogliono tutto tra -1 e 1 (o simile).
    *   Creiamo uno **StandardScaler**. Chiamiamo `.fit()` su tutto il dataset storico. Lui impara: "La media della vibrazione è 12.5, la deviazione standard è 3.2".
    *   **CRUCIALE**: Salviamo questa "conoscenza" in un file: `preprocessor.joblib`. Questo file è sacro. Lo useremo ovunque per garantire che *tutti* trattino i dati allo stesso modo.

### 2.2 Il Training (`training_service`)
Il container di training parte solo quando l'ingestione ha finito.
1.  **Caricamento**: Legge i dati processati e carica il sacro `preprocessor.joblib`.
2.  **Trasformazione**: Applica `.transform()` (Nota: NON `.fit()`) ai dati. Usa le regole imparate prima per scalare i numeri.
3.  **Addestramento**: Diamo i dati all'algoritmo **Isolation Forest**.
    *   *Logica*: "Impara come è fatta una lavatrice normale. Tutto ciò che è diverso, chiamalo Anomalia".
    *   È un modello **Non Supervisionato** (non gli diciamo noi cos'è un guasto, lo capisce dalla "stranezza" dei dati).
4.  **Logging**: Il training service parla con **MLflow** (porta 5000).
    *   Gli dice: "Ho usato 100 estimatori. Ho ottenuto una Precision del 95%".
    *   Gli invia il modello fisico (`model.pkl`).
    *   MLflow lo registra nel suo **Model Registry** come "AnomalyForest versione X".

---

# 3. LA MEMORIA: Il Feature Store (Offline -> Online)
**Attori**: `Feast`, `Redis`, `feature_loader_service`

Abbiamo addestrato il modello sui dati storici (Offline). Ma ora dobbiamo prepararci per il tempo reale (Online). Qui entra in gioco **Feast**, che non è un semplice database, ma un **Feature Router** intelligente.

### 3.1 La Definizione (Infrastructure as Code)
Tutto nasce dal file `feature_repo/feature_definitions.py`. Qui definiamo i metadati:
*   **Entity**: La chiave primaria del business (`machine_id`). Feast impara che tutte le feature (es. temperatura) "appartengono" a una specifica lavatrice.
*   **Feature View**: Raggruppamento logico. Diciamo a Feast: "Esiste la view `machine_stats` che contiene `Vibration_rollingMean` e deriva da questa sorgente dati".
*   **Registry (`registry.db`)**: Con `feast apply`, creiamo il catalogo centrale che mappa nomi logici a dati fisici.

### 3.2 Il Ponte: Materialization & Point-in-Time
Feast gestisce due mondi:
1.  **Offline Store (Il Passato - Parquet)**: Lento, massivo. Qui Feast esegue magie come il **Point-in-Time Join**: se vuoi addestrare un modello per predire un guasto alle 10:00, Feast recupera esattamente il valore della temperatura delle 9:59 (il momento *prima* dell'evento), evitando il *Data Leakage* (barare guardando il futuro).
2.  **Online Store (Il Presente - Redis)**: Velocissimo. Con il comando `feast materialize`, Feast prende le feature più recenti dal Parquet e le carica in Redis.
    *   *Nota Bene*: In Redis non finisce tutto lo storico, ma solo **l'ultima "fotografia" valida** per ogni `machine_id`.
    *   Quando l'inferenza chiederà "Stato Macchina 1", Redis risponderà in 2 millisecondi con l'ultimo valore caricato.

> **Logica**: Redis è la "memoria a breve termine" ultra-veloce. Senza la materializzazione di Feast, l'inferenza real-time dovrebbe leggere file Parquet giganti, impiegando secondi invece di millisecondi.

---

# 4. IL TEMPO REALE: Streaming & Ingestion
**Attori**: `producer_service`, `Redpanda`, `streaming_service` (Quix), `Redis`

Il sistema è addestrato e caldo. Ora accendiamo la simulazione (`make simulate`).

1.  **Il Produttore**: Simula i sensori IoT. Ogni secondo invia un JSON a **Redpanda** (topic `telemetry-data`).
    *   `{"machine_id": 1, "vibration": 55.2, "timestamp": ...}`
2.  **Il Broker (Redpanda)**: Riceve il messaggio e lo mette in coda. Garantisce l'ordine (FIFO).
3.  **Lo Stream Processor (Quix)**: È sempre in ascolto su Redpanda.
    *   Riceve il messaggio grezzo.
    *   **Stateful Processing**: Deve calcolare la media mobile. Ma un messaggio singolo non basta. Quindi Quix mantiene in memoria uno "stato" (gli ultimi N messaggi).
    *   Calcola le feature (Rolling Mean).
    *   Usa il `preprocessor.joblib` (lo stesso del training!) per normalizzare i dati.
    *   **Scrittura**: Spinge le feature calcolate dentro **Redis** (sovrascrivendo i valori vecchi per quella macchina) e pubblica un messaggio "arricchito" sul topic `processed-telemetry`.

---

# 5. LA DECISIONE: Inferenza & Predizione
**Attori**: `inference_service` (FastAPI), `Redis`, `MLflow`

Siamo al dunque. Un client esterno (o una dashboard) vuole sapere: "La macchina 1 sta rompendosi?".

1.  **La Richiesta**: Arriva una POST HTTP a `localhost:8000/predict` con `{"machine_id": 1}`.
2.  **Recupero Dati**: L' Inference Service non si fida dei dati inviati dal client. Si fida del suo sistema.
    *   Chiama Feast: "Dammi le feature *attuali* (online) per la macchina 1".
    *   Feast interroga **Redis** e restituisce il vettore di feature calcolato un millisecondo fa dallo Streaming Service.
3.  **Il Modello**:
    *   All'avvio, il servizio ha scaricato l'ultima versione "Production" del modello da **MLflow**.
    *   Prende il vettore da Redis, lo passa al modello.
4.  **Il Verdetto**:
    *   Il modello sputa un numero: `-1` (Anomalia) o `1` (Normale), e uno score (`-0.15`).
5.  **Risposta**: L'API restituisce il JSON al client: `{"is_anomaly": true, "confidence": 0.85}`.

---

# 6. IL CERVELLO: Monitoring & Retraining
**Attori**: `MLflow`, `Redpanda Console`

Mentre tutto questo accade:
1.  **MLflow Tracking**: Monitoriamo se le prestazioni del modello degradano nel tempo (Model Drift).
2.  **Redpanda Console**: Vediamo il flusso dei messaggi scorrere come un fiume.
3.  **Loop**: Se rileviamo troppi falsi positivi, scarichiamo i nuovi dati, li aggiungiamo al dataset storico, e rilanciamo `make pipeline` (Punto 1).

**Questo è MLOps.** Non è solo codice, è un organismo vivente che respira dati, impara, ricorda e decide.

import logging
import time
import json
import numpy as np
import os
import mlflow
from config.settings import Settings
from src.load_from_feast_split import DataManager
from src.model import ModelFactory
from src.evaluator import ProductionMetricsCalculator
from src.utils import create_and_log_signature

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_training_number(output_dir: str) -> int:
    """
    Determina il numero del training corrente.
    Incrementale basato sui file di metriche salvati.
    """
    metrics_history_file = os.path.join(output_dir, "training_history.json")
    
    if not os.path.exists(metrics_history_file):
        return 1
    
    try:
        with open(metrics_history_file, "r") as f:
            history = json.load(f)
            return len(history) + 1
    except:
        return 1

def save_training_metrics(output_dir: str, training_number: int, metrics: dict, thresholds: dict, drift_report: dict):
    """
    Salva metriche di questo training nella storia.
    Usate per confronto drift al prossimo training.
    """
    metrics_history_file = os.path.join(output_dir, "training_history.json")
    metrics_by_training_file = os.path.join(output_dir, f"metrics_training_{training_number}.json")
    
    # Salva metriche per questo specifico training
    training_data = {
        "training_number": training_number,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "metrics": metrics,
        "thresholds": thresholds,
        "drift_report": drift_report
    }
    
    os.makedirs(output_dir, exist_ok=True)
    
    with open(metrics_by_training_file, "w") as f:
        json.dump(training_data, f, indent=2)
    logger.info(f"[SAVE] Metriche training salvate: {metrics_by_training_file}")
    
    # Mantieni storia cumulativa
    history = []
    if os.path.exists(metrics_history_file):
        try:
            with open(metrics_history_file, "r") as f:
                history = json.load(f)
        except:
            history = []
    
    history.append(training_data)
    
    with open(metrics_history_file, "w") as f:
        json.dump(history, f, indent=2)
    logger.info(f"[SAVE] Storia training aggiornata: {metrics_history_file}")

def main():
    s = Settings()
    mlflow.set_tracking_uri(s.mlflow_tracking_uri)
    mlflow.set_experiment(s.mlflow_experiment_name)

    # Determina numero training
    training_number = get_training_number(s.output_dir)
    logger.info(f"[MAIN] Avvio Training #{training_number}")
    print("\n" + "="*70)
    print(f"🚀 TRAINING #{training_number}")
    print("="*70 + "\n")

    # 1. DATA
    logger.info("[DATA] Caricamento dati...")
    dm = DataManager(s)
    df = dm.load_data()
    
    # Ordina per timestamp ma NON splitta - usa tutto per training
    logger.info("[DATA] Ordinamento temporale (senza split)...")
    ts = s.event_timestamp_column
    df = df.sort_values(ts).reset_index(drop=True)
    logger.info(f"[DATA] Dataset totale: {len(df)} righe ordinate temporalmente")
    
    drop_cols = [s.event_timestamp_column, "Machine_ID"]  # Colonne da escludere
    x_train = df.drop(columns=[c for c in drop_cols if c in df.columns])
    
    logger.info(f"[DATA] Features selezionate: {x_train.shape[1]} colonne")

    # 2. PIPELINE
    logger.info("[PIPELINE] Costruzione pipeline...")
    num_cols = x_train.select_dtypes(include=["number", "bool"]).columns.tolist()
    cat_cols = [c for c in x_train.columns if c not in num_cols]
    
    logger.info(f"[PIPELINE] Colonne numeriche: {len(num_cols)} | Categoriche: {len(cat_cols)}")
    pipe = ModelFactory.build_pipeline(num_cols, cat_cols, s)

    with mlflow.start_run():
        # 3. TRAINING
        logger.info("[TRAIN] Fitting pipeline su TUTTO il dataset...")
        start_train = time.time()
        pipe.fit(x_train)
        train_time = time.time() - start_train
        logger.info(f"[TRAIN] Training completato in {train_time:.2f}s")

        # 4. INFERENCE & EVALUATION su tutto il dataset (training data)
        logger.info("[INFERENCE] Calcolo predizioni e scores su training data...")
        start_inference = time.time()
        x_train_pre = pipe.named_steps["pre"].transform(x_train)
        pred_train = pipe.predict(x_train)
        scores_train = pipe.named_steps["model"].score_samples(x_train_pre)
        inference_time = time.time() - start_inference
        latency = (inference_time * 1000) / len(x_train)
        
        logger.info(f"[INFERENCE] Inference completato in {inference_time:.2f}s")
        logger.info(f"[INFERENCE] Latency media: {latency:.3f}ms per record")

        # 5. EVALUATION - Calcola metriche SU TRAINING DATA
        logger.info("[EVAL] Calcolo metriche...")
        evaluator = ProductionMetricsCalculator(s.training.contamination)
        metrics = evaluator.calculate_metrics(x_train_pre, pred_train, scores_train, latency, "training")
        
        # Get thresholds
        thresholds = evaluator.get_thresholds(scores_train, pred_train)
        
        logger.info(f"[EVAL] Anomalie rilevate: {metrics['n_anomalies_detected']} ({metrics['anomaly_percentage']:.2f}%)")
        logger.info(f"[EVAL] Score distribution p50: {metrics['score_distribution']['p50']:.4f}")

        # 6. DRIFT DETECTION (dal 2° training in poi)
        logger.info(f"[DRIFT] Inizio drift detection (training #{training_number})...")
        previous_metrics_path = os.path.join(s.output_dir, f"metrics_training_{training_number - 1}.json")
        drift_report = evaluator.detect_drift(metrics, previous_metrics_path, training_number)

        # 7. LOGGING - Metriche complete su MLflow
        logger.info("[MLFLOW] Log metriche...")
        mlflow.log_metrics({
            "n_anomalies_detected": metrics["n_anomalies_detected"],
            "anomaly_rate": metrics["anomaly_percentage"],
            "latency_ms": metrics["inference_latency_ms"],
            "score_mean": metrics["score_statistics"]["mean"],
            "score_std": metrics["score_statistics"]["std"],
            "score_min": metrics["score_statistics"]["min"],
            "score_max": metrics["score_statistics"]["max"],
            "training_number": training_number,
            "dataset_size": len(x_train),
        })
        
        # Log parametri
        mlflow.log_param("score_distribution", json.dumps(metrics["score_distribution"]))
        mlflow.log_param("training_number", str(training_number))
        mlflow.log_param("contamination", str(s.training.contamination))
        
        # Log drift report
        mlflow.log_param("drift_level", drift_report["drift_level"])
        mlflow.log_param("is_first_training", str(drift_report["is_first_training"]))

        # Signature con dati RAW (DataFrame)
        logger.info("[MLFLOW] Creazione signature...")
        signature = create_and_log_signature(x_train, pipe)
        
        # Log model
        logger.info("[MLFLOW] Log model...")
        mlflow.sklearn.log_model(
            pipe, 
            "model", 
            signature=signature, 
            registered_model_name=s.mlflow_model_name
        )
        
        # 8. SALVATAGGIO ARTIFACTS
        logger.info("[ARTIFACTS] Salvataggio artifacts...")
        os.makedirs(s.output_dir, exist_ok=True)
        
        # Salva thresholds
        with open(f"{s.output_dir}/thresholds.json", "w") as f:
            json.dump(thresholds, f, indent=2)
        mlflow.log_artifact(f"{s.output_dir}/thresholds.json")
        
        # Salva metriche nella storia
        save_training_metrics(s.output_dir, training_number, metrics, thresholds, drift_report)

    # 9. SEVERITY CLASSIFICATION - Esempio su anomalie
    logger.info("[SEVERITY] Analisi severità anomalie...")
    print("\n" + "="*70)
    print("📋 SAMPLE SEVERITY ANALYSIS (prime 5 anomalie)")
    print("="*70)
    
    anomaly_indices = np.where(pred_train == -1)[0][:5]
    for idx, anomaly_idx in enumerate(anomaly_indices, 1):
        score = scores_train[anomaly_idx]
        severity = evaluator.classify_anomaly_severity(score, thresholds)
        print(f"  Anomalia #{idx}: Score={score:.4f} → {severity}")
    
    print("="*70 + "\n")

    logger.info(f"✅ Training #{training_number} completato con successo!")
    print(f"\n✅ TRAINING #{training_number} COMPLETATO CON SUCCESSO!\n")

if __name__ == "__main__":
    main()
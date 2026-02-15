import numpy as np
import logging
from typing import Dict, Any
import json
import os

logger = logging.getLogger(__name__)

class ProductionMetricsCalculator:
    def __init__(self, contamination: float):
        self.contamination = contamination

    def calculate_metrics(self, x_data_pre, predictions, scores, latency_ms, name="set") -> Dict[str, Any]:
        """
        Calcola metriche basandosi sui dati passati.
        IMPORTANTE: Viene chiamato con scores_train per il set di training!
        """
        metrics = {
            "n_anomalies_detected": int((predictions == -1).sum()),
            "anomaly_percentage": float(((predictions == -1).sum() / len(predictions)) * 100),
            "score_statistics": {
                "mean": float(np.mean(scores)),
                "std": float(np.std(scores)),
                "min": float(np.min(scores)),
                "max": float(np.max(scores))
            },
            "score_distribution": {f"p{p}": float(np.percentile(scores, p)) for p in [1, 5, 50, 95, 99]},
            "inference_latency_ms": latency_ms
        }
        return metrics

    def get_thresholds(self, scores, predictions) -> Dict[str, float]:
        """
        Estrae i parametri di riferimento dal training.
        Usati come soglie per alerting in produzione.
        """
        return {
            "p01": float(np.percentile(scores, 1)),
            "p05": float(np.percentile(scores, 5)),
            "p50": float(np.percentile(scores, 50)),
            "observed_max_anomaly": float(np.max(scores[predictions == -1])) if any(predictions == -1) else 0.0
        }

    def classify_anomaly_severity(self, score: float, thresholds: Dict[str, float]) -> str:
        """
        Classifica il livello di severità di un'anomalia basandosi sui percentili.
        
        Livelli:
        - ANOMALIA_GRAVISSIMA 🔴: score < p01
        - ANOMALIA_GRAVE 🟠: p01 ≤ score < p05
        - ANOMALIA_LIEVE 🟡: p05 ≤ score < p50
        - NORMALE 🟢: score ≥ p50
        """
        if score < thresholds["p01"]:
            return "ANOMALIA_GRAVISSIMA 🔴"
        elif score < thresholds["p05"]:
            return "ANOMALIA_GRAVE 🟠"
        elif score < thresholds["p50"]:
            return "ANOMALIA_LIEVE 🟡"
        else:
            return "NORMALE 🟢"

    def detect_drift(self, 
                     current_metrics: Dict[str, Any], 
                     previous_metrics_path: str,
                     training_number: int) -> Dict[str, Any]:
        """
        Rileva drift confrontando metriche attuali con quelle precedenti.
        
        Eseguito SOLO dal 2° training in poi.
        Focussato su sensori elettrici (vibrazione, temperatura, corrente).
        
        Trigger per drift:
        - CRITICO 🔴: |p50_prod - p50_train| > 30%
        - MODERATO 🟠: |p50_prod - p50_train| > 15%
        - LIEVE 🟡: |p50_prod - p50_train| > 5%
        """
        drift_report = {
            "is_first_training": training_number == 1,
            "training_number": training_number,
            "drift_detected": False,
            "drift_level": "NESSUNO",
            "drift_details": {}
        }
        
        # Se è il primo addestramento, non confrontare
        if training_number == 1:
            logger.info("[DRIFT] Primo addestramento - nessun confronto possibile.")
            print("\n" + "="*70)
            print("🔵 PRIMO ADDESTRAMENTO - Baseline stabilito")
            print("="*70)
            return drift_report
        
        # Carica metriche precedenti
        try:
            if not os.path.exists(previous_metrics_path):
                logger.warning(f"[DRIFT] File metriche precedenti non trovato: {previous_metrics_path}")
                return drift_report
            
            with open(previous_metrics_path, "r") as f:
                previous_metrics = json.load(f)
            
            logger.info(f"[DRIFT] Confronto con metriche precedenti del training #{training_number - 1}")
            
        except Exception as e:
            logger.error(f"[DRIFT] Errore nel carico metriche precedenti: {e}")
            return drift_report
        
        # Calcola differenze percentuali su metriche chiave
        print("\n" + "="*70)
        print(f"📊 DRIFT DETECTION - Training #{training_number}")
        print("="*70)
        
        # 1. P50 Score Distribution (metrica principale per sensori)
        current_p50 = current_metrics["score_distribution"]["p50"]
        previous_p50 = previous_metrics["score_distribution"]["p50"]
        p50_diff_pct = abs((current_p50 - previous_p50) / abs(previous_p50)) * 100 if previous_p50 != 0 else 0
        
        drift_report["drift_details"]["p50_score"] = {
            "previous": previous_p50,
            "current": current_p50,
            "diff_pct": p50_diff_pct
        }
        
        # 2. Anomaly Rate
        current_anomaly_rate = current_metrics["anomaly_percentage"]
        previous_anomaly_rate = previous_metrics["anomaly_percentage"]
        anomaly_rate_diff_pct = abs((current_anomaly_rate - previous_anomaly_rate) / previous_anomaly_rate) * 100 if previous_anomaly_rate != 0 else 0
        
        drift_report["drift_details"]["anomaly_rate"] = {
            "previous": previous_anomaly_rate,
            "current": current_anomaly_rate,
            "diff_pct": anomaly_rate_diff_pct
        }
        
        # 3. Latency (performance degradation)
        current_latency = current_metrics["inference_latency_ms"]
        previous_latency = previous_metrics["inference_latency_ms"]
        latency_diff_pct = abs((current_latency - previous_latency) / previous_latency) * 100 if previous_latency != 0 else 0
        
        drift_report["drift_details"]["latency"] = {
            "previous": previous_latency,
            "current": current_latency,
            "diff_pct": latency_diff_pct
        }
        
        # 4. Score Mean (shift nella distribuzione)
        current_mean = current_metrics["score_statistics"]["mean"]
        previous_mean = previous_metrics["score_statistics"]["mean"]
        mean_diff_pct = abs((current_mean - previous_mean) / abs(previous_mean)) * 100 if previous_mean != 0 else 0
        
        drift_report["drift_details"]["score_mean"] = {
            "previous": previous_mean,
            "current": current_mean,
            "diff_pct": mean_diff_pct
        }
        
        # Determina il livello di drift (focus su p50 come principale)
        max_drift_pct = max(p50_diff_pct, anomaly_rate_diff_pct, mean_diff_pct)
        
        if max_drift_pct > 30:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "CRITICO 🔴"
            action = "⚠️ RETRAINARE IMMEDIATAMENTE!"
        elif max_drift_pct > 15:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "MODERATO 🟠"
            action = "⚠️ Monitorare attentamente - Retrainare a breve"
        elif max_drift_pct > 5:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "LIEVE 🟡"
            action = "ℹ️ Informativo - Nessuna azione immediata"
        else:
            drift_report["drift_detected"] = False
            drift_report["drift_level"] = "NESSUNO ✅"
            action = "✅ Modello stabile"
        
        # Stampa report dettagliato
        print(f"\n🔍 Drift Level: {drift_report['drift_level']}")
        print(f"📈 Max Drift %: {max_drift_pct:.2f}%")
        print(f"🎯 Azione: {action}")
        
        print("\n📊 Dettagli Metriche:")
        print(f"  P50 Score:      {previous_p50:.4f} → {current_p50:.4f} ({p50_diff_pct:+.2f}%)")
        print(f"  Anomaly Rate:   {previous_anomaly_rate:.2f}% → {current_anomaly_rate:.2f}% ({anomaly_rate_diff_pct:+.2f}%)")
        print(f"  Score Mean:     {previous_mean:.4f} → {current_mean:.4f} ({mean_diff_pct:+.2f}%)")
        print(f"  Latency (ms):   {previous_latency:.2f} → {current_latency:.2f} ({latency_diff_pct:+.2f}%)")
        
        print("="*70 + "\n")
        
        return drift_report
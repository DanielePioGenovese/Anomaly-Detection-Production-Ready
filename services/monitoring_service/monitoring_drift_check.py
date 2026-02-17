"""
monitoring.py (LUNEDI' 06:00) 
Leggi metrics_training_N.json (salvato da main.py)
Calcoli metriche attuali (7 giorni batch)
CALCOLI detect_drift() QUI
Salvi drift_report_monday.json
IF drift > 30%: trigger_training_job()
"""

from typing import Dict, Any
import logging
import json
import os
logger = logging.getLogger(__name__)

class DriftDetector:
    """
    Rileva drift confrontando metriche attuali con quelle precedenti.
    Eseguito SOLO dal 2° training (Test Future) in poi.
    """
    def __init__(self, contamination: float):
        self.contamination = contamination      # salvato per context e potenziale uso futuro
    def detect_drift(self, 
                     current_metrics: Dict[str, Any], 
                     previous_metrics_path: str,
                     training_number: int) -> Dict[str, Any]:
        """
        Rileva drift confrontando metriche attuali con quelle precedenti.
        
        Eseguito SOLO dal 2° training (Test Future) in poi.

        Args:
            current_metrics: metrics calcolate ORA (training attuale)
            previous_metrics_path: path a metrics_training_{n-1}.json
            training_number: numero del training attuale (1, 2, 3, ...)
        
        Returns:
            dict con drift_level, detected, details
        
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
            print("PRIMO ADDESTRAMENTO - Baseline stabilito")
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
        print(f"DRIFT DETECTION - Training #{training_number}")
        print("="*70)
        
        # 1. P50 Score Distribution (metrica principale per sensori)
        current_p50 = current_metrics["score_distribution"]["p50"]
        previous_p50 = previous_metrics["score_distribution"]["p50"]
        p50_diff_pct = abs((current_p50 - previous_p50) / abs(previous_p50)) * 100 if previous_p50 != 0 else 0
        
        # Esempio:
        #   current_p50 = 0.5234
        #   previous_p50 = 0.4521
        #   diff = |0.5234 - 0.4521| / 0.4521 * 100 = 15.75%
        # 
        # Interpretazione: mediana è salita di 15.75%
        # Possibile significato: dati stanno diventando meno anomali

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

        # Esempio:
        #   current = 12.5%
        #   previous = 10.2%
        #   diff = |12.5 - 10.2| / 10.2 * 100 = 22.55%
        # 
        # Interpretazione: numero anomalie è aumentato del 22.55%
        # Possibile significato: dati stanno peggiorando (più anomalous)
        
        # 3. Latency (performance degradation)
        current_latency = current_metrics["inference_latency_ms"]
        previous_latency = previous_metrics["inference_latency_ms"]
        latency_diff_pct = abs((current_latency - previous_latency) / previous_latency) * 100 if previous_latency != 0 else 0
        
        drift_report["drift_details"]["latency"] = {
            "previous": previous_latency,
            "current": current_latency,
            "diff_pct": latency_diff_pct
        }
        
        # Monitoraggio performance del modello
        # Se latency aumenta di molto → modello sta slowing down

        # 4. Score Mean (shift nella distribuzione)
        current_mean = current_metrics["score_statistics"]["mean"]
        previous_mean = previous_metrics["score_statistics"]["mean"]
        mean_diff_pct = abs((current_mean - previous_mean) / abs(previous_mean)) * 100 if previous_mean != 0 else 0
        
        drift_report["drift_details"]["score_mean"] = {
            "previous": previous_mean,
            "current": current_mean,
            "diff_pct": mean_diff_pct
        }

        # Media score è cambiata?
        # Se media è salita (meno anomalo) o scesa (più anomalo) di molto → possibile drift nella natura dei dati 
        
        # Determina il livello di drift (focus su p50 come principale)
        # Prendi il peggiore dei 4 metriche
        max_drift_pct = max(p50_diff_pct, anomaly_rate_diff_pct, mean_diff_pct)
        
        # Trigger di drift basato su differenza percentuale (soglie arbitrarie da definire in base al dominio)
        # Più del 30% di variazione = situazione critica
        # Modello probabilmente inefficace
        if max_drift_pct > 30:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "CRITICO 🔴"
            action = " RETRAINARE IMMEDIATAMENTE!"
        
        # Tra 15% e 30% = situazione moderata
        elif max_drift_pct > 15:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "MODERATO 🟠"
            action = " Monitorare attentamente - Retrainare a breve"

        # Tra 5% e 15% = situazione lieve (small drift)
        elif max_drift_pct > 5:
            drift_report["drift_detected"] = True
            drift_report["drift_level"] = "LIEVE 🟡"
            action = " ℹ Informativo - Nessuna azione immediata"
        
        # Meno del 5% = nessun drift significativo (modello stabile)
        else:
            drift_report["drift_detected"] = False
            drift_report["drift_level"] = "NESSUNO "
            action = "Modello stabile"
        
        # Stampa report dettagliato
        print(f"\nDrift Level: {drift_report['drift_level']}")
        print(f"Max Drift %: {max_drift_pct:.2f}%")
        print(f"Azione: {action}")
        
        print("\nDettagli Metriche:")
        print(f"  P50 Score:      {previous_p50:.4f} → {current_p50:.4f} ({p50_diff_pct:+.2f}%)")
        print(f"  Anomaly Rate:   {previous_anomaly_rate:.2f}% → {current_anomaly_rate:.2f}% ({anomaly_rate_diff_pct:+.2f}%)")
        print(f"  Score Mean:     {previous_mean:.4f} → {current_mean:.4f} ({mean_diff_pct:+.2f}%)")
        print(f"  Latency (ms):   {previous_latency:.2f} → {current_latency:.2f} ({latency_diff_pct:+.2f}%)")
        
        print("="*70 + "\n")
        
        return drift_report
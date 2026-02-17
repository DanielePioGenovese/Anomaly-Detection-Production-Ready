from typing import Dict

class AnomalySeverityClassifier: 
    """
    Classifica il livello di severità di un'anomalia basandosi sui percentili dei punteggi.
    Utilizzato in produzione per etichettare ogni record anomalo con un livello di gravità.
    """
    def __init__(self, thresholds: Dict[str, float]):
        """
        Constructor: riceve i percentili calcolati sui dati di training.
        
        Args:
            thresholds: dict con p01, p05, p50 (es. {"p01": -2.134, "p05": -1.567, "p50": 0.234})
        """
        self.thresholds = thresholds
        
    def classify_anomaly_severity(self, score: float) -> str:
        """
        Classifica il livello di severità di un'anomalia basandosi sui percentili.
        
        Livelli:
        - ANOMALIA_GRAVISSIMA 🔴: score < p01
        - ANOMALIA_GRAVE 🟠: p01 ≤ score < p05
        - ANOMALIA_LIEVE 🟡: p05 ≤ score < p50
        - NORMALE 🟢: score ≥ p50

        Args:
            score: anomaly score continuo (tipicamente < 0)
        
        Returns:
            stringa con livello di severità 
        """
        if score < self.thresholds["p01"]:  
            return "ANOMALIA_GRAVISSIMA 🔴"
        elif score < self.thresholds["p05"]:  
            return "ANOMALIA_GRAVE 🟠"
        elif score < self.thresholds["p50"]: 
            return "ANOMALIA_LIEVE 🟡"
        else:
            return "NORMALE 🟢"
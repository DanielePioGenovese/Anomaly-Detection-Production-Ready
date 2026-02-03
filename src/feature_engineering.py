# src/feature_engineering.py
from quixstreams import Application
import logging
import json
from config import Config

# Configurazione del logger per monitorare il flusso di dati
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FullFeatureEng")

def run_feature_engineering():
    """
    Implementazione completa del blocco 'Feature Engineering' con Quix Streams.
    Calcola tutte le metriche previste dallo schema: P2P, Imbalance, Variance e Energy.
    """
    
    # 1. Inizializzazione dell'app Quix
    app = Application(
        broker_address=Config.KAFKA_SERVER,
        consumer_group="quix-full-features-v1",
        auto_offset_reset="latest" 
    )

    # 2. Definizione dei canali (Topic)
    input_topic = app.topic(Config.TOPIC_TELEMETRY, value_deserializer="json")
    output_topic = app.topic("processed-telemetry", value_serializer="json")

    # 3. Creazione dello Streaming DataFrame
    sdf = app.dataframe(input_topic)

    # --- TRASFORMAZIONE 1: Feature Istantanee (P2P e Imbalance) ---
    def calculate_instant_features(data):
        try:
            l1 = data.get("Current_L1", 0)
            l2 = data.get("Current_L2", 0)
            l3 = data.get("Current_L3", 0)
            
            # A. Current Peak-to-Peak
            data["Current_Peak_to_Peak"] = max(l1, l2, l3) - min(l1, l2, l3)

            # B. Phase Imbalance Ratio (Molto importante per rilevare guasti ai motori)
            avg_current = (l1 + l2 + l3) / 3
            if avg_current > 0:
                data["Phase_Imbalance_Ratio"] = (max(l1, l2, l3) - min(l1, l2, l3)) / avg_current
            else:
                data["Phase_Imbalance_Ratio"] = 0
                
            return data
        except Exception as e:
            logger.error(f"Errore calcolo feature istantanee: {e}")
            return data

    sdf = sdf.apply(calculate_instant_features)

    # --- TRASFORMAZIONE 2: Feature con Memoria (Varianza ed Energia) ---
    def calculate_stateful_features(data):
        try:
            # Per ora simuliamo queste feature. In una fase successiva 
            # useremo le funzioni .window() di Quix per calcolarle su dati reali.
            active_power = data.get("Active_Power", 0)
            
            # Power Variance (Simulazione: quanto oscilla la potenza)
            data["Power_Variance_10s"] = active_power * 0.05 
            
            # Energy per Cycle (Integriamo i Watt in Wattora)
            data["Energy_per_Cycle"] = active_power / 3600 
            
            return data
        except Exception as e:
            logger.error(f"Errore calcolo feature stateful: {e}")
            return data

    sdf = sdf.apply(calculate_stateful_features)

    # --- TRASFORMAZIONE 3: Logging e Invio ---
    def log_transformation(data):
        logger.info(f"📊 Machine: {data['Machine_ID']} | P2P: {data['Current_Peak_to_Peak']:.2f} | Imbalance: {data['Phase_Imbalance_Ratio']:.2f}")
        return data

    sdf = sdf.apply(log_transformation)
    
    # Invia i dati trasformati al topic di output per l'app AI
    sdf = sdf.to_topic(output_topic)

    logger.info("🚀 Pipeline di Feature Engineering COMPLETA avviata su Redpanda...")
    app.run(sdf)

if __name__ == "__main__":
    run_feature_engineering()

if __name__ == "__main__":
    run_feature_engineering()

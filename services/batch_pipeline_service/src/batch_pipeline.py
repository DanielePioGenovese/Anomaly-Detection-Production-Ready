import os
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from data_engineering_service.data_engineering import FeatureEngineering 

# --- 1. CONFIGURAZIONE ---

@dataclass(frozen=True)
class WasherSettings:
    """Configurazioni centralizzate per la Batch Pipeline."""
    input_dir: str        # Path dei dati storici grezzi
    offline_dir: str      # Path per l'Offline Store di Feast (Parquet)
    spark_partitions: int # Numero di partizioni per l'output

def load_settings() -> WasherSettings:
    return WasherSettings(
        input_dir=os.getenv("HISTORICAL_DIR", "/app/data/historical_data"),
        offline_dir=os.getenv("OFFLINE_DIR", "/app/data/feature_store"),
        spark_partitions=int(os.getenv("SPARK_PARTITIONS", "4")),
    )

# --- 2. LOGICA DI CALCOLO BATCH (CORE) ---

def compute_machine_batch_features(df: DataFrame, aggregation_type: str = 'daily') -> DataFrame:
    """
    Calcola il 'Daily_Vibration_PeakMean_Ratio' (max/mean).
    Include il First-Period Guard per invalidare i dati parziali del primo giorno.
    """
    
    # A. Preparazione: Cast timestamp e troncamento alla granularità scelta
    df = df.withColumn("event_timestamp", F.col("timestamp").cast("timestamp"))
    
    if aggregation_type == 'daily':
        df = df.withColumn("period", F.date_trunc('day', F.col("event_timestamp")))
    elif aggregation_type == 'weekly':
        df = df.withColumn("period", F.date_trunc('week', F.col("event_timestamp")))
    else:
        raise ValueError(f"Aggregation type {aggregation_type} non supportato.")

    # B. Aggregazione Giornaliera per Macchina
    # Usiamo groupBy + agg per massima efficienza in Spark
    agg_df = df.groupBy("Machine_ID", "period").agg(
        F.max("Vibration_mm_s").alias("max_vib"),
        F.mean("Vibration_mm_s").alias("mean_vib"),
        F.max("event_timestamp").alias("event_timestamp") # Necessario per Feast
    )

    # C. Calcolo Ratio e First-Period Guard
    # Identifichiamo il primo periodo registrato per ogni macchina per evitare dati troncati
    window_spec = Window.partitionBy("Machine_ID")
    agg_df = agg_df.withColumn("first_period", F.min("period").over(window_spec))

    # Formula: (max / mean)
    # Applichiamo NULL se il periodo corrente è il primo della macchina
    result_df = agg_df.withColumn(
        "Daily_Vibration_PeakMean_Ratio",
        F.when(F.col("period") == F.col("first_period"), F.lit(None).cast("float"))
         .otherwise((F.col("max_vib") / F.col("mean_vib")).cast("float"))
    )

    # D. Selezione Colonne Finali (Schema Feast)
    return result_df.select(
        "Machine_ID",
        "event_timestamp",
        "Daily_Vibration_PeakMean_Ratio"
    )

# --- 3. HELPER DI SCRITTURA ---

def write_to_offline_store(df: DataFrame, output_path: Path, partitions: int):
    """Scrive i dati in formato Parquet sovrascrivendo i precedenti."""
    output_path.mkdir(parents=True, exist_ok=True)
    (
        df.repartition(partitions)
        .write.mode("overwrite")
        .parquet(str(output_path))
    )

# --- 4. MAIN JOB ---

def main():
    s = load_settings()

    # Inizializzazione Spark Session
    spark = SparkSession.builder \
        .appName("WashingMachine-Batch-Feature-Pipeline") \
        .master("local[*]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        print(f"[*] Lettura dati grezzi da: {s.input_dir}")
        # Caricamento di tutti i file Parquet nella directory
        raw_df = spark.read.parquet(f"{s.input_dir}/*.parquet")

        print("[*] Calcolo delle feature batch (Peak-to-Mean Ratio)...")
        features_df = compute_machine_batch_features(raw_df, aggregation_type='daily')

        output_path = Path(s.offline_dir) / "machine_batch_features"
        print(f"[*] Scrittura nell'Offline Store: {output_path}")
        write_to_offline_store(features_df, output_path, s.spark_partitions)

        # Debug/Log finale
        print("\n--- ESEMPIO FEATURE PRODOTTE ---")
        features_df.filter(F.col("Daily_Vibration_PeakMean_Ratio").isNotNull()).show(5)
        
        print(f"[*] Pipeline completata con successo.")
        print(f"[*] ISO Date per 'feast materialize-incremental': {datetime.now(timezone.utc).isoformat()}")

    except Exception as e:
        print(f"[!] Errore durante l'esecuzione del job: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

# ----------------------------------


import os
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import della tua classe di engineering
from data_engineering_service.data_engineering import FeatureEngineering

def run_batch_pipeline():
    spark = SparkSession.builder \
        .appName("WashingMachine-Batch-Append-Clean") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Configurazione percorsi
    input_path = os.getenv("HISTORICAL_DIR", "/app/data/historical_data")
    output_path = Path(os.getenv("OFFLINE_DIR", "/app/data/feature_store")) / "machine_batch_features"

    try:
        # 1. Caricamento dati grezzi (Data Lake)
        print(f"[*] Caricamento dati da {input_path}")
        raw_df = spark.read.parquet(f"{input_path}/*.parquet")

        # 2. Esecuzione Feature Engineering
        # L'output qui ha TUTTE le colonne originali + la nuova feature
        engineer = FeatureEngineering(config_path="config/feature_config.yaml")
        enriched_df = engineer._apply_batch_features(raw_df)

        # 3. SELEZIONE E PULIZIA (Drop delle colonne inutili)
        # Teniamo solo Machine_ID, il timestamp troncato al giorno e la feature.
        # Usiamo .distinct() per collassare le righe duplicate del join interno.
        print("[*] Pulizia schema e rimozione duplicati...")
        
        final_batch_df = enriched_df.select(
            "Machine_ID",
            F.date_trunc('day', F.col("timestamp")).alias("event_timestamp"),
            "Daily_Vibration_PeakMean_Ratio"
        ).distinct()

        # 4. Filtro anti-NULL (First-Period Guard e Check qualità)
        final_batch_df = final_batch_df.filter(F.col("Daily_Vibration_PeakMean_Ratio").isNotNull())

        # 5. Scrittura Incrementale (APPEND)
        print(f"[*] Esecuzione Append nel parquet: {output_path}")
        (
            final_batch_df.repartition(1) 
            .write
            .mode("append") 
            .parquet(str(output_path))
        )
        
        print(f"[*] Task completato. Righe aggiunte allo storico: {final_batch_df.count()}")
        final_batch_df.show(5)

    except Exception as e:
        print(f"[!] Errore durante la pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_batch_pipeline()
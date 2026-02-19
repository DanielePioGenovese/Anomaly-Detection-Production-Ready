import pandas as pd
import numpy as np
import time
from typing import Tuple
from mlflow.models import infer_signature, ModelSignature

def create_and_log_signature(x_sample: pd.DataFrame, model_pipe) -> ModelSignature:
    """
    Crea la firma del modello basandosi sui dati GREZZI (DataFrame).
    È fondamentale passare il DataFrame originale, non l'array trasformato,
    perché il modello in produzione riceverà i dati via API (JSON).
    """
    # Effettuiamo una predizione di prova per inferire l'output
    sample_output = model_pipe.predict(x_sample.head(5))
    
    # infer_signature mappa i nomi delle colonne e i tipi di dati (int, float, object)
    signature = infer_signature(
        model_input=x_sample.head(5), 
        model_output=sample_output
    )
    
    return signature

def measure_latency(pipe, x_data: pd.DataFrame) -> float:
    """
    Misura la latenza media per record in millisecondi.
    """
    start_time = time.time()
    _ = pipe.predict(x_data)
    total_time_ms = (time.time() - start_time) * 1000
    
    return total_time_ms / len(x_data)
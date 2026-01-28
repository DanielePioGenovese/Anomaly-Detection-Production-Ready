import pandas as pd
import numpy as np
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import Metadata
from sdv.evaluation.single_table import get_column_plot
import os

# Definizione dei percorsi
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(DATA_DIR, 'test_data_3.csv')
CSV_OUTPUT = os.path.join(DATA_DIR, 'synthetic_data_sdv.csv')
PARQUET_OUTPUT = os.path.join(DATA_DIR, 'synthetic_data_sdv.parquet')
PLOT_OUTPUT = os.path.join(DATA_DIR, 'visual_evaluation.html')

def generate_synthetic_data(num_rows=1000):      # ho impostato 1000 righe come default per non esagerare, poichè faccio anche il salvataggio in CSV 
    print(f"Caricamento dati da {INPUT_FILE}...")
    data = pd.read_csv(INPUT_FILE)
    
    # PULIZIA E GESTIONE DATI PER SDV
    # Gestione Timestamp
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    
    # Gestiamo i valori mancanti nelle categorie (causa comune di OverflowError) e li convertiamo in stringa 
    if 'Anomaly_Type' in data.columns:
        data['Anomaly_Type'] = data['Anomaly_Type'].fillna('None').astype(str)
    
    # ci assicuriamo che Machine_ID sia stringa
    if 'Machine_ID' in data.columns:
        data['Machine_ID'] = data['Machine_ID'].astype(str)

    # rilevamento metadati
    print("Rilevamento metadati...")
    metadata = Metadata.detect_from_dataframe(data)
    
    # Forzatura sui metadati per evitare problemi a sdv
    metadata.update_column(column_name='timestamp', sdtype='datetime')
    metadata.update_column(column_name='Machine_ID', sdtype='categorical')
    metadata.update_column(column_name='Anomaly_Type', sdtype='categorical')
    
    # inizializzazione del modello di sdv, pare sia il migliore in questo caso
    synthesizer = GaussianCopulaSynthesizer(metadata)
    
    # Fitting
    synthesizer.fit(data)
    
    # generazione dei dati con il numero di righe specificato 
    synthetic_data = synthesizer.sample(num_rows=num_rows)
    
    # TEST DI AUTOVALUTAZIONE PER VEDERE SE I DATI SINTETICI GENERATI SONO BUONI
    try:
        fig = get_column_plot(
            real_data=data,
            synthetic_data=synthetic_data,
            column_name='Active_Power',
            metadata=metadata
        )
        # salva il grafico in HTML
        fig.write_html(PLOT_OUTPUT)
        print(f"Grafico di valutazione salvato in: {PLOT_OUTPUT}")
    except Exception as e:
        print(f"Errore nella generazione del grafico: {e}")

    # salvataggio in Parquet
    synthetic_data.to_parquet(PARQUET_OUTPUT, index=False)
    
    # salvataggio in CSV (in questo caso solo per le prove, per vedere come si comportano i dati sintetici)
    synthetic_data.to_csv(CSV_OUTPUT, index=False)

    print("Generazione completata con successo.")
    return synthetic_data

if __name__ == "__main__":
    generate_synthetic_data()

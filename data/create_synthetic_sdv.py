import pandas as pd
import numpy as np
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import Metadata
from sdv.evaluation.single_table import evaluate_quality
import os

# Path configuration
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(DATA_DIR, 'test_data.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'synthetic_data_sdv.parquet')

def generate_synthetic_data(num_rows=100000):
    print("⏳ Data loading and preparation...")
    data = pd.read_csv(INPUT_FILE)
    
    # minimal cleaning required for SDV
    if 'Anomaly_Type' in data.columns:
        data['Anomaly_Type'] = data['Anomaly_Type'].fillna('None')

    # training only on sensors (prevents OverflowError crash)
    train_cols = [c for c in data.columns if c not in ['timestamp', 'Machine_ID']]
    metadata = Metadata.detect_from_dataframe(data[train_cols])
    
    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(data[train_cols])
    
    # Sensor values generation
    synthetic_data = synthesizer.sample(num_rows=num_rows)
    
    # Timeline synchronization (5 machines every 30 seconds)
    # This ensures logical order and solves the 1970 date bug
    num_machines = 5
    num_steps = num_rows // num_machines
    
    start_time = pd.to_datetime(data['timestamp']).min()
    timeline = pd.date_range(start=start_time, periods=num_steps, freq='30s')
    
    synthetic_data['timestamp'] = np.repeat(timeline, num_machines)[:num_rows]
    synthetic_data['Machine_ID'] = (['WM_01', 'WM_02', 'WM_03', 'WM_04', 'WM_05'] * num_steps)[:num_rows]
    
    # quality evaluation (overall score)
    full_metadata = Metadata.detect_from_dataframe(data) 
    report = evaluate_quality(data, synthetic_data, full_metadata)
    print(f"\n✅ DATASET QUALITY: {report.get_score() * 100:.2f}%")
    
    # saving
    synthetic_data.to_parquet(OUTPUT_FILE, index=False)
    print(f"🚀 File created successfully: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_synthetic_data()
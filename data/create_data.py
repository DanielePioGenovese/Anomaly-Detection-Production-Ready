import pandas as pd
from openai import OpenAI
import io
import time
import re
from api_config import NVIDIA_API

# Configuration
NVIDIA_API_KEY = NVIDIA_API
BASE_URL = "https://integrate.api.nvidia.com/v1"
MODEL = "meta/llama-3.1-405b-instruct"
TOTAL_ROWS_NEEDED = 100 
BATCH_SIZE = 50          

client = OpenAI(base_url=BASE_URL, api_key=NVIDIA_API_KEY)

def generate_batch(reference_df, num_rows, start_time):
    # Use a specific machine context for this batch to keep it realistic
    machine_id = reference_df['Machine_ID'].iloc[-1]
    sample_data = reference_df.sample(100).to_csv(index=False)
    
    prompt = f"""
    You are an expert Reliability Engineer. Generate {num_rows} new rows of industrial sensor data.
    
    ### CONTEXT:
    - Machine_ID: {machine_id}
    - Start Timestamp: {start_time} (Increment by 1 minute per row)
    
    ### PHYSICAL CONSTRAINTS:
    1. Phase 1 (Idle): ~4-5A, ~1.5kW. Phase 2 (Heating): ~25-35A, ~10-12kW. 
    2. Active_Power must roughly match: √3 * (Voltage/1000) * Avg(Current) * Power_Factor.
    3. Inrush_Current_Peak should only be > 0 at the start of a new Cycle_Phase_ID.
    4. If Is_Anomaly is 0, Anomaly_Type MUST be empty (,,).
    5. If Is_Anomaly is 1, Anomaly_Type is one of: [Motor_Overheating, Voltage_Sag, Bearing_Failure, Unbalanced_Load].

    ### FORMATTING:
    - NO headers. NO markdown backticks. NO explanation. 
    - Provide ONLY raw CSV lines.

    ### SAMPLE FOR DISTRIBUTION REFERENCE:
    {sample_data}
    """
    
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
    )
    return response.choices[0].message.content

# --- Execution ---
df_real = pd.read_csv("test_data.csv")
all_batches = [df_real] # Start with the real data as the base
current_total = 0

# Get the initial starting point
current_last_time = pd.to_datetime(df_real['timestamp'].iloc[-1])

print(f"Starting generation...")

while current_total < TOTAL_ROWS_NEEDED:
    try:
        # 1. Update timestamp for the next batch
        next_time_str = (current_last_time + pd.Timedelta(minutes=1)).isoformat()
        
        # 2. Call API
        raw_output = generate_batch(df_real, BATCH_SIZE, next_time_str)
        
        # 3. Clean output (remove markdown if the LLM added it)
        clean_csv = re.sub(r'```csv|```', '', raw_output).strip()
        
        # 4. Parse
        batch_df = pd.read_csv(io.StringIO(clean_csv), names=df_real.columns, header=None)
        
        # 5. Update state
        all_batches.append(batch_df)
        current_total += len(batch_df)
        
        # Update the time tracker to the end of the NEW batch
        current_last_time = pd.to_datetime(batch_df['timestamp'].iloc[-1])
        
        print(f"Generated {current_total}/{TOTAL_ROWS_NEEDED} rows. Last timestamp: {current_last_time}")
        time.sleep(1) # Safety for Rate Limits
        
    except Exception as e:
        print(f"Error: {e}. Retrying...")
        time.sleep(5)

# Save final result
final_df = pd.concat(all_batches, ignore_index=True)
final_df.to_csv("synthetic_dataset_final_2.csv", index=False)
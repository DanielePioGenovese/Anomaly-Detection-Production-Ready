"""
Corrected Kafka consumer with flexible telemetry data model
"""
import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

from config import Config

# ------------------------------
# Updated Pydantic model based on actual data structure
# ------------------------------
class TelemetryData(BaseModel):
    # --- Identifiers ---
    timestamp: str
    Machine_ID: str
    Cycle_Phase_ID: Optional[int] = None

    # --- Electrical Metrics (L1, L2, L3) ---
    Current_L1: Optional[float] = None
    Current_L2: Optional[float] = None
    Current_L3: Optional[float] = None
    Voltage_L_L: Optional[float] = None
    
    # --- Power Metrics ---
    Active_Power: Optional[float] = None
    Reactive_Power: Optional[float] = None
    Power_Factor: Optional[float] = None
    Power_Variance_10s: Optional[float] = None
    Energy_per_Cycle: Optional[float] = None

    # --- Quality & Anomalous Indicators ---
    THD_Current: Optional[float] = None
    THD_Voltage: Optional[float] = None
    Current_Peak_to_Peak: Optional[float] = None
    Inrush_Current_Peak: Optional[float] = None
    Inrush_Duration: Optional[float] = None
    Phase_Imbalance_Ratio: Optional[float] = None

    # --- Labels (Optional during real-time inference) ---
    Is_Anomaly: Optional[int] = None
    Anomaly_Type: Optional[str] = None

    class Config:
        extra = "allow"

# ------------------------------
# Kafka consumer setup
# ------------------------------
def get_consumer() -> Consumer:
    """Create a Kafka consumer to read telemetry messages."""
    conf = {
        'bootstrap.servers': 'localhost:9092',  # Updated from Config
        'group.id': 'anomaly-detector-v1',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

# ------------------------------
# Main loop to consume and process messages
# ------------------------------
def start_consuming():
    consumer = get_consumer()
    topic = Config.TOPIC_TELEMETRY
    consumer.subscribe([topic])
    print(f"📥 Listening on topic '{topic}'... (CTRL+C to stop)\n")

    message_count = 0
    error_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    break

            # ------------------------------
            # Decode JSON message
            # ------------------------------
            try:
                data_dict = json.loads(msg.value().decode('utf-8'))
                data = TelemetryData(**data_dict)
                message_count += 1
                
            except (json.JSONDecodeError, ValidationError) as e:
                error_count += 1
                print(f"⚠️ Invalid message #{error_count}: {e}")
                print(f"   Raw data: {msg.value().decode('utf-8')[:100]}...")
                continue

            # ------------------------------
            # Process valid data
            # ------------------------------
            # Convert to DataFrame for analysis (optional)
            df_row = pd.DataFrame([data.model_dump()])

            # Log received data
            if data.Temperature is not None:
                print(f"✅ [{message_count}] {data.Machine_ID}: "
                      f"Temp={data.Temperature}°C, "
                      f"Phase={data.Cycle_Phase_ID}, "
                      f"Current={data.Current}")
            else:
                print(f"📊 [{message_count}] {data.Machine_ID}: "
                      f"Phase={data.Cycle_Phase_ID}, "
                      f"Current={data.Current} "
                      f"(no temperature)")

            # ------------------------------
            # Add your anomaly detection here
            # ------------------------------
            # if data.Temperature is not None:
            #     anomaly_score = model.predict(df_row)
            #     if anomaly_score > threshold:
            #         print(f"🚨 ANOMALY DETECTED: {anomaly_score}")

    except KeyboardInterrupt:
        print(f"\n🛑 Consumer stopped manually.")
        print(f"📊 Statistics: {message_count} valid messages, {error_count} errors")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consuming()
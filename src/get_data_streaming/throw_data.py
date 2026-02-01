"""
Corrected Kafka consumer with flexible telemetry data model
"""
import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

# ------------------------------
# Updated Pydantic model based on actual data structure
# ------------------------------
class TelemetryData(BaseModel):
    """
    Flexible telemetry data model.
    Based on the error, we know the data includes:
    - Machine_ID (required)
    - Cycle_Phase_ID (present in messages)
    - Current (present in messages)
    - Temperature (OPTIONAL - not always present!)
    """
    Machine_ID: str
    Cycle_Phase_ID: Optional[int] = None
    Current: Optional[float] = None
    Temperature: Optional[float] = None  # Make it optional!
    Pressure: Optional[float] = None
    Vibration: Optional[float] = None
    Humidity: Optional[float] = None
    Power_Consumption: Optional[float] = None
    Operational_Status: Optional[str] = None
    Error_Code: Optional[str] = None
    Production_Count: Optional[int] = None
    
    # Add any other fields you discover
    class Config:
        extra = "allow"  # Allow extra fields without error

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
    topic = 'telemetry-data'
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
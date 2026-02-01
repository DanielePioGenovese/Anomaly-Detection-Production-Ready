import time
import pandas as pd
import json
from kafka import KafkaProducer
from config import Config  


def get_producer():
    """Initialize the Kafka producer with low-latency optimized settings."""
    return KafkaProducer(
        bootstrap_servers=[Config.KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Optimizations for immediate sending (every 30s)
        linger_ms=0,
        acks=1
    )

def simulate_sensor():
    producer = get_producer()

    # Load the dataset (CSV or Parquet depending on what you have)
    try:
        df = pd.read_parquet(Config.RAW_DATA_PATH)
    except Exception as e:
        print(f"Error while loading the file: {e}")
        return

    print(f"📡 Simulation started. Total records: {len(df)}")

    for index, row in df.iterrows():
        data = row.to_dict()

        try:
            # Send to the topic defined in Config
            future = producer.send(Config.TOPIC_TELEMETRY, value=data)

            # Wait for delivery confirmation
            record_metadata = future.get(timeout=10)

            print(
                f"✅ [{index}] Sent: ID {data.get('Machine_ID', 'N/A')} "
                f"to Partition {record_metadata.partition}"
            )

            # Make sure the message is sent now (not buffered)
            producer.flush()

        except Exception as e:
            print(f"❌ Error while sending record {index}: {e}")

        # Required 30-second interval
        print("--- Waiting for 30 seconds ---")
        time.sleep(30)

if __name__ == "__main__":
    simulate_sensor()

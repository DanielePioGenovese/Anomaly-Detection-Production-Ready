import time
import pandas as pd
import json
from confluent_kafka import Producer
from config import Config

def delivery_report(err, msg):
    """ Callback to confirm if the message reached Kafka. """
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent: {msg.topic()} [Partition: {msg.partition()}]")

def get_producer():
    # Confluent-kafka uses 'bootstrap.servers' (with a dot) inside a dict
    conf = {
        'bootstrap.servers': Config.KAFKA_SERVER,
        'linger.ms': 0,
        'acks': 1
    }
    return Producer(conf)

def start_streaming():
    producer = get_producer()

    try:
        df = pd.read_parquet(Config.STREAMING_DATASET)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    print(f"📡 Simulation started. Records: {len(df)}")

    for _, row in df.iterrows():
        # 1. Prepare data
        data = row.to_dict()
        payload = json.dumps(data, default=str).encode('utf-8')

        # 2. Produce (Asynchronous)
        producer.produce(
            Config.TOPIC_TELEMETRY, 
            value=payload, 
            callback=delivery_report
        )

        # 3. Flush & Sleep
        producer.flush() # Forces the message out immediately
        print(f"Waiting 1s before next record...") # remember to change it to 30 sec!!
        time.sleep(1)


if __name__ == "__main__":
    start_streaming()
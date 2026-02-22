import os
from pathlib import Path

class Config:
    """Internal configuration for the Streaming Service"""
    
    # Kafka/Redpanda config
    KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'redpanda:9092')
    TOPIC_TELEMETRY = os.getenv('TOPIC_TELEMETRY', 'telemetry-data')

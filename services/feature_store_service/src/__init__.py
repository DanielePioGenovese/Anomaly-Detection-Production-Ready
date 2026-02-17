"""
Feast Feature Store Repository

This module exposes all Feast objects (entities, data sources, 
feature views, and feature services) so they can be discovered
and registered by `feast apply`.
"""

# Import entities
from src.entity import machine

# Import data sources
from src.data_sources import machines_batch_source, machines_stream_source

# Import feature views
from src.features import machine_streaming_features, machine_batch_features

# Import feature services
from src.feature_services import machine_anomaly_service_v1

# Expose all objects for feast apply to discover
__all__ = [
    # Entities
    'machine',
    
    # Data Sources
    'machines_batch_source',
    'machines_stream_source',
    
    # Feature Views
    'machine_streaming_features',
    'machine_batch_features',
    
    # Feature Services
    'machine_anomaly_service_v1'
]

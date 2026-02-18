"""
Feature View Definitions for Feast Feature Store

This module defines feature views that group related features together.
Feature views specify:
- Which features to track
- How long to retain them (TTL)
- Which entity they belong to
- Which data source to use
"""

from datetime import timedelta
from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from entity import machine
from data_sources import stream_source


# ============================================================================
# MACHINE FEATURES VIEW
# ============================================================================
# Groups all sensor readings and engineered features for washing machines
machine_view = FeatureView(
    name="machine_stream_features_v1",
    entities=[machine],
    ttl=timedelta(days=7),  # Features expire after 7 days in online store (Redis)
    
    schema=[
        # ====================================================================
        # RAW SENSOR READINGS - Electrical Measurements
        # ====================================================================
        Field(
            name="Current_L1", 
            dtype=Float32,
            description="Current on Phase L1 in Amperes"
        ),
        Field(
            name="Current_L2", 
            dtype=Float32,
            description="Current on Phase L2 in Amperes"
        ),
        Field(
            name="Current_L3", 
            dtype=Float32,
            description="Current on Phase L3 in Amperes"
        ),
        Field(
            name="Voltage_L_L", 
            dtype=Float32,
            description="Line-to-Line Voltage in Volts"
        ),
        
        # ====================================================================
        # RAW SENSOR READINGS - Operational Parameters
        # ====================================================================
        Field(
            name="Water_Temp_C", 
            dtype=Float32,
            description="Water temperature in Celsius"
        ),
        Field(
            name="Motor_RPM", 
            dtype=Float32,
            description="Motor rotation speed in RPM"
        ),
        Field(
            name="Water_Flow_L_min", 
            dtype=Float32,
            description="Water flow rate in Liters per minute"
        ),
        Field(
            name="Water_Pressure_Bar", 
            dtype=Float32,
            description="Water pressure in Bar"
        ),
        
        # ====================================================================
        # RAW SENSOR READINGS - Vibration
        # ====================================================================
        Field(
            name="Vibration_mm_s", 
            dtype=Float32,
            description="Current vibration measurement in mm/s"
        ),
        
        # ====================================================================
        # ENGINEERED FEATURES - Vibration Aggregates
        # ====================================================================
        Field(
            name="Vibration_RollingMax_10min", 
            dtype=Float32,
            description="Rolling maximum vibration over last 10 minutes (engineered feature)"
        ),
        
        # ====================================================================
        # CONTEXTUAL FEATURES - Cycle Information
        # ====================================================================
        Field(
            name="Cycle_Phase_ID", 
            dtype=String,
            description="Current phase of the wash cycle (e.g., Fill, Wash, Rinse, Spin)"
        ),
    ],
    
    source=stream_source,
    online=True,  # Enable serving from Redis for real-time predictions
    description="Real-time and batch features for washing machine anomaly detection"
)
from feast import FeatureService 
# Import the FeatureService class from Feast to define features that can be served to models during training and inference
from feature_definitions import machine_view
# Import the machine_view FeatureView that we defined in feature_definitions.py, which contains the features related to machines

# Create a feature service for the machine view 
machine_feature_service_v1 = FeatureService(
    name="machine_feature_service_v1",
    features=[machine_view],
    tags={"team": "ml", "project": "predictive_maintenance", "domain":"electrical_monitoring", "version": "1.0"}  # Optional metadata tags
)
create_datasets:
	uv run --group dataset-creation -m data.prepare_data  
	
test_hist_ingestion:
	uv run --group hist_ingestion -m services.historical_ingestion_service.src.test_feature_engineering --group hist_ingestion
	
hist_service:
	docker compose up hist_ingestion

hist_service_down:
	docker compose down hist_ingestion


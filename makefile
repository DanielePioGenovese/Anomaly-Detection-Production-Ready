
build_ingestions:
	docker build -f services/ingestion_service/Dockerfile -t ingestion .

run_ingestion:
	docker run -d --name ingestion_container ingestion

rm_ingestion:
	docker rm ingestion_container

stop_ingestion:
	docker stop ingestion_container

all:
	docker compose up -d
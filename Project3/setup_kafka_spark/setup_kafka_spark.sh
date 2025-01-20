#!/bin/bash

# Create Kafka topic
docker exec -it kafka kafka-topics --create --topic test_topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

# Build Kafka producer Docker image
docker build -t kafka-producer .

# Send streaming.py to the container
docker cp streaming.py spark-master:/streaming.py

#Run Kafka producer container
docker run --network docker_compose_setup_pipeline-network kafka-producer


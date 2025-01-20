### Run the docker compsse to set up container
docker-compose -f ./docker_compose_setup/docker-compose.yml up -d

### Set up kafka and spark
chmod +x ./setup_kafka_spark/setup_kafka_spark.sh

./setup_kafka_spark/setup_kafka_spark.sh

### Set up Cassandra table
chmod +x ./setup_cassandra/setup_cassandra_table.sh

./setup_cassandra/setup_cassandra_table.sh

### Set up superset 
chmod +x ./superset_setup/superset_setup.sh

./superset_setup/superset_setup.sh

###Run the spark-master: Open the powershell to run this (When running window, there are some error in bash)
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /streaming.py

### After waiting for finishing set up spark-master, comback to the git bash then run following to run kafka producer:
docker run --network cassandra-trino-superset_pipeline-network kafka-producer

### Come the url: localhost:8090 to access the superset UI 

### To down the docker-compose:
# docker-compose -f ./cassandra-trino-superset/docker-compose.yml down

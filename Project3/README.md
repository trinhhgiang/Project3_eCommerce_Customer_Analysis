# Big_data_20241_Ecommerce_Analysis

## Prequisites
- pip install pyspark
- Install docker deskstop
- Window 10/11
## How to run
- Build containers and install libraries
  
```
docker-compose -f ./docker_compose_setup/docker-compose.yml up -d
```
- Set up kafka and spark

```
chmod +x ./setup_kafka_spark/setup_kafka_spark.sh

./setup_kafka_spark/setup_kafka_spark.sh
```
- Set up Cassandra table

```
chmod +x ./setup_cassandra/setup_cassandra_table.sh

./setup_cassandra/setup_cassandra_table.sh
```

- Set up superset 

```
chmod +x ./superset_setup/superset_setup.sh

./superset_setup/superset_setup.sh
```

- Run the spark-master: Open the `powershell` to run this (When running window, there are some error in bash)
  
```
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /streaming.py
```

- After waiting for finishing set up spark-master, comback to the `git bash` then run following to run kafka producer:

```
docker run --network docker_compose_setup_pipeline-network kafka-producer
```

- Open another bash to run this to setup consumer to write data to hdfs
 ```
docker cp consumer.py spark-master:/consumer.py
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /consumer.py
```

- For batch processing, run:
 ```
docker cp sparkML.py spark-master:/sparkML.py
docker exec -it spark-master pip install numpy pandas
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /sparkML.py
```

- Browse the url: `localhost:8090` to access the superset UI.

- Connect to the database through trino in superset
  ![image](https://github.com/user-attachments/assets/c84a6b35-f105-4ae6-be3c-96c686c58b1c)
  ![image](https://github.com/user-attachments/assets/d48a0de3-118b-4641-b275-fdf82f288ed1)
  
  Then add SQLALCHEMY URI: `trino://'':@trino:8080/cassandra` then select `connect`

-   From here you can build your own dashboard

- (Optional) You can use my dashboard format we specify in advance by following step:
  ![image](https://github.com/user-attachments/assets/1716a63a-0ecf-4ab7-bc15-4a5e4b8b5ccf)
  ![image](https://github.com/user-attachments/assets/78d18223-2d9d-4de7-91a0-d734f6ffda02)
  Then add file zip `dashboard_template.zip` .

  Here is the results of dashboard:
  ![image](https://github.com/user-attachments/assets/55eb4455-abd9-48f2-bbde-67963420141b)





  

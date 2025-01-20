from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType
from pyspark.sql.functions import from_json, expr, window, col, split, regexp_replace, sum, when
from pyspark.sql import functions as F
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra')
CASSANDRA_PORT = os.getenv('CASSANDRA_PORT', '9042')
CHECKPOINT_DIR_CATEGORY = os.getenv('CHECKPOINT_DIR_CATEGORY', '/tmp/checkpoint_category2')
CHECKPOINT_DIR_BRAND = os.getenv('CHECKPOINT_DIR_BRAND', '/tmp/checkpoint_brand2')

# Packages for Spark
packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {packages} pyspark-shell"

def create_spark_session():
    """Create and configure Spark Session"""
    try:
        spark = SparkSession.builder \
            .appName("Spark Cassandra Streaming Integration") \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.shuffle.partitions", 10) \
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def create_streaming_dataframe(spark):
    """Create streaming DataFrame from Kafka"""
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", "test_topic") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("groupId", "batch") \
            .load()
        
        json_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as msg_value")
        
        json_schema = StructType([
            StructField('Unnamed', IntegerType(), True),
            StructField('event_time', StringType(), True),
            StructField('event_type', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('category_id', StringType(), True),
            StructField('category_code', StringType(), True),
            StructField('brand', StringType(), True),
            StructField('price', FloatType(), True),
            StructField('user_id', IntegerType(), True),
            StructField('user_session', StringType(), True),
        ])
        
        streaming = json_df.withColumn("msg_value", from_json(json_df["msg_value"], json_schema)).select("msg_value.*")
        
        # Pre-processing
        streaming = streaming.withColumn("event_time", expr("cast(cast(event_time as double)/1000 as timestamp) as event_time"))
        
        # Fill null values
        fill_null_vals = {"category_code": "others.others", "brand": "unknown"}
        streaming = streaming.na.fill(fill_null_vals)
        
        # Split category
        categories_split = split(streaming["category_code"], "\\.", 2)
        streaming = streaming.withColumn("category", categories_split[0]) \
                             .withColumn("sub_category", regexp_replace(categories_split[1], "\\.", "_")) \
                             .drop("category_code")

        streaming = streaming.select("event_time", "event_type", "product_id", "category_id", "category", "sub_category", "brand", "price", "user_id", "user_session") 
        
        return streaming
    except Exception as e:
        logger.error(f"Error creating streaming DataFrame: {e}")
        raise


def main():
    spark = create_spark_session()
    streaming = create_streaming_dataframe(spark)

    hdfs_query = streaming.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "hdfs://namenode:9000/data/test_topic/data.csv") \
        .option("checkpointLocation", "hdfs://namenode:9000/data/test_topic/checkpoint") \
        .start()
    
    # Wait for termination with a way to handle interruption
    try:
        hdfs_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
        hdfs_query.stop()

if __name__ == "__main__":
    main()
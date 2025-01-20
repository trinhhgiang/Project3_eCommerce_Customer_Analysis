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

            # .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/kafka-to-hdfs") \
            # .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
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
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("groupId", "streaming") \
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
        
        return streaming
    except Exception as e:
        logger.error(f"Error creating streaming DataFrame: {e}")
        raise

def write_to_cassandra(batch_df, batch_id, keyspace, table, end_time_var):
    """Write batch DataFrame to Cassandra"""
    try:
        logger.info(f"Processing batch {batch_id}")
        
        # Move end time forward
        end_time_var = end_time_var + F.expr("INTERVAL 1 MINUTE")
        
        # Filter data
        filtered_batch = batch_df.filter(F.col("end_time") <= end_time_var)
        
        filtered_batch.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table) \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id} written to Cassandra")
    except Exception as e:
        logger.error(f"Error writing batch to Cassandra: {e}")

def main():
    spark = create_spark_session()
    streaming = create_streaming_dataframe(spark)
    
    # Initialize global end time variables
    end_time_var_category = F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    end_time_var_brand = F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    
    # Category query
    query_count_category = streaming.withWatermark("event_time", "3 minutes") \
        .groupby(window(col("event_time"), "1 minutes"), col("category"), col("sub_category")) \
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("event_type") == "view", col("price")).otherwise(0)).alias("view_revenue"),
            sum(when(col("event_type") == "cart", col("price")).otherwise(0)).alias("cart_revenue"),
            sum(when(col("event_type") == "remove_from_cart", col("price")).otherwise(0)).alias("remove_cart_revenue"),
            sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("purchase_revenue")
        ) \
        .selectExpr("window.start as start_time", "window.end as end_time", "category", "sub_category", "view_count",
                    "cart_count", "remove_cart_count", "purchase_count", 
                    "view_revenue", "cart_revenue", "remove_cart_revenue", "purchase_revenue",
                    "CASE WHEN view_count > 0 THEN view_revenue / view_count ELSE 0 END AS average_price") \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_cassandra(batch_df, batch_id, "category", "prices_countings_category", end_time_var_category)) \
        .trigger(processingTime="1 minutes") \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR_CATEGORY) \
        .start()
    
    # Brand query
    query_count_brand = streaming.withWatermark("event_time", "3 minutes") \
        .groupby(window(col("event_time"), "1 minutes"), col("brand")) \
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("event_type") == "view", col("price")).otherwise(0)).alias("view_revenue"),
            sum(when(col("event_type") == "cart", col("price")).otherwise(0)).alias("cart_revenue"),
            sum(when(col("event_type") == "remove_from_cart", col("price")).otherwise(0)).alias("remove_cart_revenue"),
            sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("purchase_revenue")
        ) \
        .selectExpr("window.start as start_time", "window.end as end_time", "brand", "view_count",
                    "cart_count", "remove_cart_count", "purchase_count", 
                    "view_revenue", "cart_revenue", "remove_cart_revenue", "purchase_revenue",
                    "CASE WHEN view_count > 0 THEN view_revenue / view_count ELSE 0 END AS average_price") \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_cassandra(batch_df, batch_id, "brand", "prices_countings_brand", end_time_var_brand)) \
        .trigger(processingTime="1 minutes") \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR_BRAND) \
        .start()
    # streaming.write.csv("hdfs://namenode:9000/data/test_topic/data.csv")
    # hdfs_query = streaming.writeStream \
    #     .trigger(processingTime="10 seconds") \
    #     .format("parquet") \
    #     .outputMode("append") \
    #     .option("path", "hdfs://namenode:9000/data/test_topic") \
    #     .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/kafka-to-hdfs") \
    #     .start()

    # hdfs_query = streaming.writeStream \
    #     .outputMode("append") \
    #     .format("csv") \
    #     .option("path", "hdfs://namenode:9000/data/test_topic/data.csv") \
    #     .option("checkpointLocation", "hdfs://namenode:9000/data/test_topic/checkpoint") \
    #     .start()
    
    # Wait for termination with a way to handle interruption
    try:
        query_count_category.awaitTermination()
        query_count_brand.awaitTermination()
        # hdfs_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming stopped by user")
        query_count_category.stop()
        query_count_brand.stop()
        # hdfs_query.stop()

if __name__ == "__main__":
    main()
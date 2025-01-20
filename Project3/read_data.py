from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read HDFS Data") \
    .getOrCreate()

# Path to the HDFS file
hdfs_path = "hdfs://namenode:9000/data/test_topic/data.csv"

# Read the CSV file from HDFS
df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .load(hdfs_path)

# Show the data
df.show()

# Print the schema
df.printSchema()

# Stop the SparkSession
spark.stop()
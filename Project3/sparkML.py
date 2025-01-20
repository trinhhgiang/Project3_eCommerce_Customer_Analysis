from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import vector_to_array
# from pyspark.ml.stat import PCA

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = '9042'

spark = SparkSession.builder \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.shuffle.partitions", 10) \
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true") \
    .appName("Read HDFS Data") \
    .getOrCreate()

# Path to the HDFS file
hdfs_path = "hdfs://namenode:9000/data/test_topic/data.csv"

# schema = StructType([
#     StructField("event_time", StringType(), True),
#     StructField("event_type", StringType(), True),
#     StructField("product_id", IntegerType(), True),
#     StructField("category_id", StringType(), True),
#     StructField("category", StringType(), True),
#     StructField("sub_category", StringType(), True),
#     StructField("brand", StringType(), True),
#     StructField("price", FloatType(), True),
#     StructField("user_id", IntegerType(), True),
#     StructField("user_session", StringType(), True)
# ])

# Read the CSV file from HDFS
df = spark.read.csv(hdfs_path, inferSchema=True)
# Load data (replace with your actual data loading process)
# data = spark.read.csv("path_to_your_data.csv", header=True, inferSchema=True)

df = df.withColumnRenamed("_c0", "event_time") \
    .withColumnRenamed("_c1", "event_type") \
    .withColumnRenamed("_c2", "product_id") \
    .withColumnRenamed("_c3", "category_id") \
    .withColumnRenamed("_c4", "category") \
    .withColumnRenamed("_c5", "sub_category") \
    .withColumnRenamed("_c6", "brand") \
    .withColumnRenamed("_c7", "price") \
    .withColumnRenamed("_c8", "user_id") \
    .withColumnRenamed("_c9", "user_session")

df = df.drop("event_time", "user_session")

# Encode 'event_type'
indexer = StringIndexer(inputCol="event_type", outputCol="event_type_indexed")
df = indexer.fit(df).transform(df)

# One-hot encode 'category_code'
indexer_category = StringIndexer(inputCol="category", outputCol="category_indexed")
df = indexer_category.fit(df).transform(df)
encoder = OneHotEncoder(inputCol="category_indexed", outputCol="category_onehot")
df = encoder.fit(df).transform(df)

# Aggregate user features
user_features = df.groupBy("user_id").agg(
    F.count("event_type").alias("activity_frequency"),
    F.countDistinct("product_id").alias("unique_products"),
    F.countDistinct("brand").alias("unique_brands"),
    F.mean("price").alias("average_spend")
)

# Assemble features into a vector
assembler = VectorAssembler(
    inputCols=["activity_frequency", "unique_products", "unique_brands", "average_spend"],
    outputCol="features"
)
user_features = assembler.transform(user_features)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
user_features = scaler.fit(user_features).transform(user_features)

# Apply K-means
kmeans = KMeans(k=4, seed=42, featuresCol="scaled_features", predictionCol="cluster")
model = kmeans.fit(user_features)
user_features = model.transform(user_features)


final_df = user_features.select("user_id", "activity_frequency", "unique_products", "unique_brands", "average_spend", "cluster")

# Group by cluster and calculate aggregate metrics
cluster_summary = final_df.groupBy("cluster").agg(
    F.count("user_id").alias("user_count"),
    F.avg("activity_frequency").alias("avg_activity_frequency"),
    F.avg("unique_products").alias("avg_unique_products"),
    F.avg("unique_brands").alias("avg_unique_brands"),
    F.avg("average_spend").alias("avg_spending")
)

# Save the cluster summary to Cassandra
cluster_summary.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="cluster_summary", keyspace="ecommerce") \
    .save()

final_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="user_clusters", keyspace="ecommerce") \
    .save()


# Show the grouped results
cluster_summary.show(truncate=False)

# Assemble the features into a single vector
assembler = VectorAssembler(inputCols=["activity_frequency", "unique_products", "unique_brands", "average_spend"], outputCol="features")
assembled_data = assembler.transform(final_df)

# Apply PCA to reduce to 2 components
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(assembled_data)
pca_result = pca_model.transform(assembled_data)

pca_result = pca_result.withColumn("pca_x", vector_to_array(F.col("pca_features"))[0]) \
    .withColumn("pca_y", vector_to_array(F.col("pca_features"))[1])


# Show the reduced features
pca_result.select("user_id", "pca_x", "pca_y", "cluster").show(truncate=False)

pca_result.select("user_id", "pca_x", "pca_y", "cluster").write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="user_clusters_pca", keyspace="ecommerce") \
    .save()
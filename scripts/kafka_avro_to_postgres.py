from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import requests
import json

# Function to get schema from Schema Registry
def get_avro_schema_from_registry(schema_registry_url, subject_name):
    url = f"{schema_registry_url}/subjects/{subject_name}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.json()['schema'])

# Configuration
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
SUBJECT_NAME = "user-topic-value"

# Get schema from registry
schema_json = get_avro_schema_from_registry(SCHEMA_REGISTRY_URL, SUBJECT_NAME)
print(f"Retrieved schema: {schema_json}")

# Create Spark session with Avro support
spark = SparkSession.builder \
    .appName("KafkaAvroToPostgreSQL") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    ])) \
    .getOrCreate()

# Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Remove Confluent wire format header (first 5 bytes) and deserialize Avro
df_avro = df_kafka.withColumn(
    "avro_payload", 
    expr("substring(value, 6, length(value)-5)")
).select(
    col("key").cast("string").alias("message_key"),
    from_avro("avro_payload", json.dumps(schema_json)).alias("user_data"),
    col("topic").alias("topic"),
    col("partition").alias("partition_id"),
    col("offset").alias("offset_value"),
    col("timestamp").alias("timestamp_value")
).select(
    "message_key",
    "user_data.*",  # Expand the user_data struct
    "topic",
    "partition_id", 
    "offset_value",
    "timestamp_value"
)

# Function to write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/sparkdb") \
            .option("dbtable", "kafka_messages") \
            .option("user", "sparkuser") \
            .option("password", "sparkpass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id}: Wrote {batch_df.count()} Avro records to PostgreSQL")

# Write stream to PostgreSQL
query = df_avro.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kafka-avro-postgres-checkpoint") \
    .start()

print("Avro streaming started. Reading from Kafka and writing to PostgreSQL...")
query.awaitTermination()

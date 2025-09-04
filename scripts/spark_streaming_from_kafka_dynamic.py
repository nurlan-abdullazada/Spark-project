from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro
import requests
import json

# -----------------------------------------
# Helper: Get latest Avro schema from registry
# -----------------------------------------
def get_latest_avro_schema(subject, registry_url):
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return json.loads(response.json()['schema'])

# Fetch the schema dynamically
schema_registry_url = "http://91.99.130.17:8081"  # Or your actual registry IP
subject_name = "test_topic-value"             # Kafka topic + "-value"
schema_json = get_latest_avro_schema(subject_name, schema_registry_url)

# -----------------------------------------
# Spark session with Kafka + Avro support
# -----------------------------------------
spark = SparkSession.builder \
    .appName("KafkaAvroAutoSchema") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    ])) \
    .getOrCreate()

# -----------------------------------------
# Read Kafka stream
# -----------------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "91.99.130.17:9094,91.99.130.17:9095,91.99.130.17:9096") \
    .option("subscribe", "testtopic") \
    .option("startingOffsets", "earliest") \
    .load()

# -----------------------------------------
# Strip 5-byte Confluent wire format header
# -----------------------------------------
df_stripped = df_raw.withColumn("avro_payload", expr("substring(value, 6, length(value)-5)"))

# -----------------------------------------
# Deserialize using schema from registry
# -----------------------------------------
df_parsed = df_stripped.select(
    from_avro("avro_payload", json.dumps(schema_json)).alias("data")
).select("data.*")

# -----------------------------------------
# Print to console
# -----------------------------------------
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-kafka-avro-checkpoint") \
    .start()

query.awaitTermination()


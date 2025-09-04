from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

# ------------------------
# Avro schema (must match the one in Schema Registry)
# ------------------------
schema_json = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
"""

# ------------------------
# Spark Session
# ------------------------
spark = SparkSession.builder \
    .appName("KafkaAvroManualDeserialization") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.repositories", "https://packages.confluent.io/maven/") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0"
    ])) \
    .getOrCreate()

# ------------------------
# Read Kafka stream
# ------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "91.99.130.17:9094,91.99.130.17:9095,91.99.130.17:9096") \
    .option("subscribe", "testtopic") \
    .option("startingOffsets", "earliest") \
    .load()

# ------------------------
# Drop Confluent's magic byte + schema ID (first 5 bytes)
# ------------------------
df_stripped = df_raw.withColumn("avro_payload", expr("substring(value, 6, length(value)-5)"))

# ------------------------
# Deserialize Avro payload
# ------------------------
df_parsed = df_stripped.select(
    from_avro("avro_payload", schema_json).alias("data")
).select("data.*")

# ------------------------
# Output to console
# ------------------------
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-kafka-avro-checkpoint") \
    .start()

query.awaitTermination()


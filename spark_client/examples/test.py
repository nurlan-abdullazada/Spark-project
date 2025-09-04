import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JupyterToStandaloneSpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.createDataFrame([
    ("Alice", 23),
    ("Bob", 34),
    ("Charlie", 45)
], ["name", "age"])

df.show()

#Keep the app (and Spark UI) alive
print("Sleeping to keep Spark UI alive at http://<host>:4040 ...")
time.sleep(300)  # Sleep for 5 minutes

spark.stop()


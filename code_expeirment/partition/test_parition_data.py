from pyspark.sql import SparkSession

appName = "PySpark Example - Partition Dynamic Overwrite"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()
df2 = spark.read.parquet("./data")
print(df2.show())





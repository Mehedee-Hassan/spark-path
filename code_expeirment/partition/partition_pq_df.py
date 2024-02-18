from pyspark.sql import SparkSession

appName = "PySpark Example - Partition Dynamic Overwrite"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

data = [{
    'dt': '2022-01-01',
    'id': 1
}, {
    'dt': '2022-01-01',
    'id': 2
}, {
    'dt': '2022-01-01',
    'id': 3
}, {
    'dt': '2022-02-01',
    'id': 1
}]

df = spark.createDataFrame(data)
print(df.schema)
df.show()
df.repartition('dt') \
    .write.mode('overwrite') \
    .partitionBy('dt') \
    .format('parquet') \
    .save('./data')

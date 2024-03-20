from pyspark.sql import SparkSession

appName = "PySpark Example - Partition Dynamic Overwrite"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

data = [{
    'dt': '2022-01-02',
    'id': 44
}, {
    'dt': '2022-01-02',
    'id': 44
}, {
    'dt': '2022-04-02',
    'id': 33
}, {
    'dt': '2022-04-02',
    'id': 12
}]

df = spark.createDataFrame(data)
print(df.schema)
df.show()
df.repartition('dt') \
    .write.mode('overwrite') \
    .partitionBy('dt') \
    .format('parquet') \
    .save('./data')




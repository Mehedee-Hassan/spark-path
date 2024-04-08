"""
dataframe

User
dataframe1

a,b,mid,date
1,5,  1,2023-01-01
2,6,  1,2023-01-02
3,7,  1,2023-01-03
4,8,  2,2023-01-04
44,81,2,2023-01-04
45,81,2,2023-01-04
46,81,2,2023-01-04


datafrake2
n,f,mid,date
1,5, 1,2023-01-01
2,6, 2,2023-01-02

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataFrame Join Example") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

from pyspark.sql import Row

# Sample data for dataframe1
data1 = [
    Row(a=1, b=5, mid=1, date="2023-01-01"),
    Row(a=2, b=6, mid=1, date="2023-01-02"),
    Row(a=3, b=7, mid=1, date="2023-01-03"),
    Row(a=4, b=8, mid=2, date="2023-01-04"),
    Row(a=44, b=81, mid=2, date="2023-01-04"),
    Row(a=45, b=81, mid=2, date="2023-01-04"),
    Row(a=46, b=81, mid=2, date="2023-01-04")
]

# Sample data for dataframe2
data2 = [
    Row(n=1, f=5, mid=1, date="2023-01-01"),
    Row(n=2, f=6, mid=2, date="2023-01-02")
]

dataframe1 = spark.createDataFrame(data1)
dataframe2 = spark.createDataFrame(data2)

dataframe1.show()
dataframe2.show()



joined_df = dataframe1.join(dataframe2, on=["mid"], how="inner")
joined_df.show()




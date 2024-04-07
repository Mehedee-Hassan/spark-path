from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# Initialize SparkSession
spark = SparkSession.builder.appName("dateFormatExample").getOrCreate()

# Sample data with a date column
data = [("2023-01-01",), ("2024-12-31",)]
columns = ["Date"]

# Creating DataFrame
df = spark.createDataFrame(data, schema=columns)

# Format the date to only show month and year (%m-%y)
formatted_df = df.withColumn("FormattedDate", date_format("Date", "MM-yy"))

# Show the original and formatted date columns
formatted_df.show()

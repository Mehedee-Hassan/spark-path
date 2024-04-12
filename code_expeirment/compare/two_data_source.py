from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Parquet Comparison Sorted") \
    .getOrCreate()

# Load the directories as DataFrames
df1 = spark.read.parquet("/path/to/first/directory")
df2 = spark.read.parquet("/path/to/second/directory")

# Specify the columns used for sorting
sorting_columns = ['column1', 'column2', 'column3']  # Replace with your actual column names

# Sort both DataFrames
df1_sorted = df1.sort(*[col(c) for c in sorting_columns])
df2_sorted = df2.sort(*[col(c) for c in sorting_columns])

# Compare the sorted DataFrames
are_identical = df1_sorted.subtract(df2_sorted).union(df2_sorted.subtract(df1_sorted)).count() == 0
print("Dataframes are identical:", are_identical)

# Identify and display differences if any
differences_df1 = df1_sorted.subtract(df2_sorted)
differences_df2 = df2_sorted.subtract(df1_sorted)

differences_df1.show()  # Show entries unique to df1_sorted
differences_df2.show()  # Show entries unique to df2_sorted

# Optionally, save these differences for further analysis
differences_df1.write.parquet("/path/to/output/differences_from_df1")
differences_df2.write.parquet("/path/to/output/differences_from_df2")

# Stop the Spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample sales data
data = [
    ("A", 100),
    ("B", 200),
    ("A", 300),
    ("B", 400),
    ("A", 200),
    ("B", 100),
    ("A", 400),
    ("B", 300),
]
columns = ["Category", "Amount"]

# Creating DataFrame
df = spark.createDataFrame(data, schema=columns)

df.show()


# Define the window specification
# Partition by "Category" and order by "Amount" in descending order
windowSpec = Window.partitionBy("Category").orderBy(df["Amount"].desc())

# Apply the row_number() function over the defined window specification
ranked_df = df.withColumn("Rank", row_number().over(windowSpec))

# Show the ranked DataFrame
ranked_df.show()

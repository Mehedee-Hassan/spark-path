import random
from pyspark.sql import SparkSession, DataFrame
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

prophet_df_val = [("2023-01-01", 123.12312, 1),("2023-01-02", 22.12312, 2)]
columns_prophet = ["ds", "daily_GMV","merchant_id"]


prophet_df = spark.createDataFrame(prophet_df_val, schema=columns_prophet)

data = [("1", "John Doe"), ("2", "Jane Doe")]

columns = ["id", "name"]

df = spark.createDataFrame(data, schema=columns)
# Assuming spark session is initialized as 'spark' and 'df' is your initial DataFrame

# Sample list of columns that should not be included in oversampling
info_columns = []  # Example: ['id', 'timestamp']

# Number of forecast paths for oversampling calculation
num_forecast_paths = 100  # Example value
print("here")
df_columns = [item for item in df.columns if item not in info_columns]

oversampling_factor = int(num_forecast_paths / len(df_columns))

# Ensure random module is imported at the beginning
random.seed(42)  # Optional: for reproducibility

column_alias_dict = {}
for i in range(oversampling_factor):
    for j, name in enumerate(df_columns):
        random_column = random.choice(df_columns)
        column_alias_dict[f"{random_column}_{i}_{j}"] = random_column

# Correctly format the selectExpr statement
df_transformed = df.selectExpr(*[f"`{original}` AS `{alias}`" for alias, original in column_alias_dict.items()])

# Assuming this code is part of a function, otherwise use df_transformed.show() to see the result
print( df_transformed)


df_transformed.show()

"""
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [("1", "John Doe"), ("2", "Jane Doe")]

# Columns for the DataFrame
columns = ["id", "name"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Dictionary mapping original column names to their new aliases
column_alias_dict = {"user_id": "id", "user_name": "name"}

# Renaming the columns using selectExpr
df_transformed = df.selectExpr(*[f"{original} AS {alias}" for alias, original in column_alias_dict.items()])

# Show the result
df_transformed.show()
"""



# print("compute_expected_metrics_across_policy_parameters_per_mid_given_pd")
#
# final_gmv_path = gmv_to_defaulted_final_gmv(prophet_paths_df, num samples, unique_dates, max default, info_columns)
#
# final gmv spark.read.parquet(final_gmv_path)
#
# metrics compute_metrics_per_policy_parameter_grid(policy_parameter_grid_by_dim, final_gmv, METRICS_NAMES, num_samples, tail_percent, offer_combinations)
#
# print("metrics computed")
#
# metrics metrics.withColumn("EP_pct", col("EP") / col("advanced_amount") * 100)
#
# metrics metrics.withColumn("EL_pt", col ("EL") / col("advanced_amount") * 100)
#
# metrics metrics.withColumn("spread_pct", col("commission_rate") col("EL_pct"))
#
# metrics metrics.withColumn('horizon_in_days", F.expr('horizon_in_days / 38').cast('int'))
#
# metrics metrics.withColumnRenamed("horizon_in_days", "settlement_period_in_months") print("netrics columns added")
#
# netrics metrics.withColumn("expected_payoff_time_in_months", F.expr("""
#
# CASE WHEN size(filter(mean_gmv_cumsum, x-> (advanced_amount (repayment_percent / 108x)) <B)) > 8
#
# THEN array position (transform(mean_gmv_cumsum, x-> IF((advanced_amount ELSE NULL END (repayment_percent/180x)) < 0, 1, 0)), 1) +1
#
# print(metrics.select("merchant id", "expected payoff time in months").show(1))
#
# metrics.write.parquet("./metrics", mode="append")
prophet_df.show()

unique_dates = prophet_df.select("ds")\
    .sample(fraction=0.05, seed=42)\
    .distinct()\
    # \
    # .rdd.map(lambda row: row.ds).collect()

print(unique_dates)
print("gmv_to_defaulted_final gmv")

print(unique_dates)

# print(max default)

# default dates = random.choices(unique_dates, k=max_default) # random choice from all dates
#
# of_columns= df.columns
#
# df_columns = [item for item in of columns if item not in info_columns]
#
# print(len(df_columns))
#
# conditions = [
#     (col("ds") >= lit(date_value)) & (col("num_default")> index)
#     for index, (column_name, date_value) in enumerate (zip(df_columns[:len(default_dates)], default_dates))]
#
# df = df.select(info_columns, *df_columns[len(default_dates):]
#                ,*[F.when(condition, lit(0))\
#                .otherwise(col(column_name))\
#                .alias(column_name) for condition,
#                         column_name in zip(conditions, df_columns)])
#
# paid_gmv = df.groupBy("month_year", "merchant_id", "annual_pd_percent", "horizon_in_days").agg([F.sum(col).alias(col) for col in df_columns],
#
# F.sum(col("mean_gmv")).alias("mean_gmv"))
#
# paid_gmv = paid_gmv.orderBy("month_year")
#
# window spec_cumsum = Window.partitionBy("merchant_id", "annual_pd_percent").orderBy("month_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
#
# I
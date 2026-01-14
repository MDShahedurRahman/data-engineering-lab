from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Zipcodes_Q5").master("local[*]").enableHiveSupport().getOrCreate()

# Read the CSV file into a DataFrame
df = (spark.read.option("header", "true").option("inferSchema", "true")
      .csv("/user/test/data/simple-zipcodes.csv"))

# 50% sample data
sample_size_df = df.sample(withReplacement=False, fraction=0.5, seed=42)

# Create and use database
spark.sql("CREATE DATABASE IF NOT EXISTS zipcodes")
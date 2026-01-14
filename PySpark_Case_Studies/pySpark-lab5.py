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
spark.sql("USE zipcodes")
spark.sql("DROP TABLE IF EXISTS zipcodes_part")

# Repartition data to enforce max records per file within state and city partitions
(sample_size_df.repartition(col("state"), col("city")).write.mode("overwrite").format("parquet")
 .option("maxRecordsPerFile", 3).partitionBy("state", "city").saveAsTable("zipcodes_part"))

# Hive SQL in PySpark
result = spark.sql("SELECT * FROM zipcodes_part WHERE state <> 'AL' AND city  <> 'SPRINGVILLE'")

# Show result to verify
result.show(truncate=False)

spark.stop()
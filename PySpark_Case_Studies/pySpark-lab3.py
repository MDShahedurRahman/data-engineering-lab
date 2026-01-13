from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Employee_Q3").master("local[*]").getOrCreate()

# Read the JSON file into a DataFrame
df = spark.read.json("/user/test/data/employee.json")
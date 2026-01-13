from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Department_Q1").master("local[*]").getOrCreate()
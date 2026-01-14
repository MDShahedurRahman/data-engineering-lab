from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, count

spark = SparkSession.builder.appName("Employee-Department_Q4").master("local[*]").enableHiveSupport().getOrCreate()
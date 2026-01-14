from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Zipcodes_Q5").master("local[*]").enableHiveSupport().getOrCreate()
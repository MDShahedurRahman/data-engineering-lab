from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, count

spark = SparkSession.builder.appName("Employee-Department_Q4").master("local[*]").enableHiveSupport().getOrCreate()

# Read the JSON file into a DataFrame
employee_df = spark.read.json("/user/data/employee.json")
department_df = spark.read.json("/user/data/department.json")
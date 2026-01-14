from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, count

spark = SparkSession.builder.appName("Employee-Department_Q4").master("local[*]").enableHiveSupport().getOrCreate()

# Read the JSON file into a DataFrame
employee_df = spark.read.json("/user/data/employee.json")
department_df = spark.read.json("/user/data/department.json")

# join employee and department dataframes
joined_df = (employee_df.join(department_df, employee_df["emp_dept_id"].cast("int") == department_df["dept_id"], "inner")
            .select(department_df["dept_name"], employee_df["salary"].cast("long").alias("salary")))

# Group by department and calculate maximum salary and total employees
dept_summary = (joined_df.groupBy("dept_name").agg(spark_max(col("salary")).alias("maxSalary"),
                     count("*").alias("employeesCount")))


# Create and use database
spark.sql("CREATE DATABASE IF NOT EXISTS employer")
spark.sql("USE employer")
spark.sql("DROP TABLE IF EXISTS part_department")

# Save results to partitioned Hive table as Parquet
(dept_summary.write.mode("overwrite").format("parquet").partitionBy("dept_name").saveAsTable("part_department"))

# Output verification
spark.sql("SELECT dept_name, maxSalary, employeesCount FROM part_department").show()

spark.stop()
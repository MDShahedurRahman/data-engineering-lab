from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Department_Q1").master("local[*]").getOrCreate()

# Read Department.txt from HDFS and create a DataFrame with inferred schema
df = (spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("/user/test/data/Department.txt")
      .toDF("dept_name", "dept_id", "salary"))


# Cast salary to long type and add doubleSalary column with twice the salary value
df2 = (df.withColumn("salary", col("salary").cast("long"))
         .withColumn("doubleSalary", (col("salary") * 2).cast("long")))

# Write parquet in HDFS
df2.write.mode("overwrite").format("parquet").save("/user/test/output/department.parquet")
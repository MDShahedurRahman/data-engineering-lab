from pyspark.sql import SparkSession
from pyspark.sql import functions as Func

if __name__ == "__main__":
    spark = SparkSession.builder.appName("covid19").master("local[*]").enableHiveSupport().getOrCreate()

    # Read from Hadoop
    df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
        .csv("/data/spark/covid19/CovidCases.csv")

    # Section 1
    # Rename the column, Select required columns, Cast confirmed to integer
    df1 = (df.withColumnRenamed("infection_case", "infection_source")
           .select("Province", "city", "infection_source", "confirmed")
           .withColumn("confirmed", Func.col("confirmed").cast("int")))
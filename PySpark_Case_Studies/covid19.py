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

    # Section 1
    # Return the TotalConfirmed and MaxFromOneConfirmedCase for each "province","city" pair
    # Sort the output in asc order on the basis of confirmed
    result1 = (df1.groupBy("Province", "city").agg(
            Func.sum("confirmed").alias("TotalConfirmed"),
            Func.max("confirmed").alias("MaxFromOneConfirmedCase")).orderBy(Func.asc("TotalConfirmed")))

    print("\nSection 1 Output")
    df1.printSchema()
    result1.show(truncate=False)

    # Section 2
    # Return the top 2 provinces on the basis of confirmed cases.
    result2 = (df1.groupBy("Province").agg(Func.sum("confirmed").alias("TotalConfirmed"))
        .orderBy(Func.desc("TotalConfirmed"))
        .limit(2)
    )

    print("\nSection 2 Output (Top 2 Provinces)")
    result2.show(truncate=False)
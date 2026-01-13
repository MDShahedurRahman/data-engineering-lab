from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Car Power").master("local[*]").getOrCreate()
    data = [
        ("Ford Torino", 140, 3449, "US"),
        ("Chevrolet Monte Carlo", 150, 3761, "US"),
        ("BMW 2002", 113, 2234, "Europe")
    ]

    columns = ["carr", "horsepower", "weight", "origin"]

    df = spark.createDataFrame(data, columns)
    df.show()

    #Rename the Mis-Spelled Column
    df = df.withColumnRenamed("carr", "car")
    df.show()

    #Add Constant Column AvgWeight = 200
    df = df.withColumn("AvgWeight", lit(200))
    df.show()

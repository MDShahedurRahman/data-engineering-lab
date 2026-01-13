from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Car Power").master("local[*]").getOrCreate()
    data = [
        ("Ford Torino", 140, 3449, "US"),
        ("Chevrolet Monte Carlo", 150, 3761, "US"),
        ("BMW 2002", 113, 2234, "Europe")
    ]

    columns = ["carr", "horsepower", "weight", "origin"]
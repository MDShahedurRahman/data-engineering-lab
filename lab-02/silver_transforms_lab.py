from pyspark.sql import SparkSession                      # Spark entry point
from pyspark.sql.functions import col, to_date, trim     # Common Spark SQL functions

def main():
    # Define the Hive database name
    DB_NAME = "iphone_analytics"

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder
        .master("local[*]")                     # Run locally using all available cores
        .appName("iphone-silver-transforms")    # Application name
        .enableHiveSupport()                    # Enable Hive metastore support
        .getOrCreate()                          # Create or reuse SparkSession
    )


# Script entry point
if __name__ == "__main__":
    main()

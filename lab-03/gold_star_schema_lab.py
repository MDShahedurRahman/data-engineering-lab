from pyspark.sql import SparkSession                             # Spark entry point
from pyspark.sql.functions import col, year, month, dayofmonth   # Date and column functions


def main():
    # Hive database name
    DB_NAME = "iphone_analytics"

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder
        .master("local[*]")  # Use all available local cores
        .appName("iphone-gold-layer")  # Application name
        .enableHiveSupport()  # Enable Hive support
        .getOrCreate()  # Create or reuse SparkSession
    )

    # Set active database
    spark.sql(f"USE {DB_NAME}")

    # Create gold-layer dimension and fact tables
    create_gold_tables(spark)

# Script entry point
if __name__ == "__main__":
    main()

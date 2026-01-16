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

    # Set the active database
    spark.sql(f"USE {DB_NAME}")

    # Execute silver-layer transformations and log results
    print("Created table:", silver_customers_transform(spark))
    print("Created table:", silver_products_transform(spark))
    print("Created table:", silver_stores_transform(spark))
    print("Created table:", silver_sales_transform(spark))

    # Stop SparkSession and free resources
    spark.stop()


# Script entry point
if __name__ == "__main__":
    main()

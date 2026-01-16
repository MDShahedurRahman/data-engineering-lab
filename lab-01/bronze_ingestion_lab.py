from pyspark.sql import SparkSession

def bronze_ingestion(spark, csv_path, table_name):

    # Return the name of the created bronze table
    return f"bronze_{table_name}"

def main():
    # Name of the Hive database to store tables
    DB_NAME = "iphone_analytics"

    # Base directory where raw CSV files are stored
    BASE_PATH = "/user/md/iphone/raw_data"

    # Create a SparkSession
    # local[*] uses all available local cores
    # enableHiveSupport allows use of Hive metastore and saveAsTable
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("iphone-bronze-ingestion")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Create the database if it does not already exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # Set the current database context
    spark.sql(f"USE {DB_NAME}")

    # Dictionary mapping table names to their source CSV file paths
    sources = {
        "customers": f"{BASE_PATH}/customers.csv",
        "products":  f"{BASE_PATH}/products.csv",
        "stores":    f"{BASE_PATH}/stores.csv",
        "sales":     f"{BASE_PATH}/sales.csv",
    }

    # Loop through each source and ingest it into the bronze layer
    for name, path in sources.items():
        table = bronze_ingestion(spark, path, name)
        print(f"Created table: {table}")

    # Stop the SparkSession and release resources
    spark.stop()

# Entry point of the script
if __name__ == "__main__":
    main()

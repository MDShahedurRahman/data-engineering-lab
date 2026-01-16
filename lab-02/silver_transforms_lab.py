from pyspark.sql import SparkSession  # Import SparkSession to create and manage Spark applications

def bronze_ingestion(spark, csv_path, table_name):

    # Return the name of the created bronze table
    return f"bronze_{table_name}"

def main():
    # Define the Hive database name
    DB_NAME = "iphone_analytics"

    # Define the base directory where raw CSV files are stored
    BASE_PATH = "/user/md/iphone/raw_data"

    # Create a SparkSession
    # local[*] uses all available CPU cores
    # enableHiveSupport allows interaction with Hive metastore
    spark = (
        SparkSession.builder            # Initialize SparkSession builder
        .master("local[*]")              # Run Spark locally using all cores
        .appName("iphone-bronze-ingestion")  # Set application name
        .enableHiveSupport()             # Enable Hive support
        .getOrCreate()                   # Create or retrieve existing SparkSession
    )

    # Create the database if it does not already exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # Set the current working database
    spark.sql(f"USE {DB_NAME}")

    # Map logical table names to their corresponding CSV file paths
    sources = {
        "customers": f"{BASE_PATH}/customers.csv",  # Customer data source
        "products":  f"{BASE_PATH}/products.csv",   # Product data source
        "stores":    f"{BASE_PATH}/stores.csv",     # Store data source
        "sales":     f"{BASE_PATH}/sales.csv",      # Sales transaction data source
    }

    # Loop through each data source and ingest it into the bronze layer
    for name, path in sources.items():
        table = bronze_ingestion(spark, path, name) # Call ingestion function
        print(f"Created table: {table}")             # Log table creation

    # Stop the SparkSession and release resources
    spark.stop()

# Entry point of the Python script
if __name__ == "__main__":
    main()

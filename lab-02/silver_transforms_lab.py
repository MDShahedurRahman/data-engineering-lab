from pyspark.sql import SparkSession  # Import SparkSession to create and manage Spark applications

def bronze_ingestion(spark, csv_path, table_name):
    # Read the CSV file into a Spark DataFrame
    # option("header", "true") treats the first row as column names
    df = (
        spark.read                  # Initialize DataFrameReader
        .option("header", "true")    # Enable CSV header parsing
        .csv(csv_path)               # Load data from the CSV file path
    )

    # Write the DataFrame as a Hive-managed table in Parquet format
    # mode("overwrite") replaces the table if it already exists
    (
        df.write                                    # Initialize DataFrameWriter
        .mode("overwrite")                          # Overwrite existing table data
        .format("parquet")                          # Store data in Parquet format
        .saveAsTable(f"bronze_{table_name}")        # Save as a Hive table in the bronze layer
    )

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

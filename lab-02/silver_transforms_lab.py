from pyspark.sql import SparkSession  # Import SparkSession to create and manage Spark applications

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


# Entry point of the Python script
if __name__ == "__main__":
    main()

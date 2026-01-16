from pyspark.sql import SparkSession

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

# Entry point of the script
if __name__ == "__main__":
    main()
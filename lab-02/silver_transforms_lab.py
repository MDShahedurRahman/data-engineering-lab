from pyspark.sql import SparkSession  # Import SparkSession to create and manage Spark applications

def main():
    # Define the Hive database name
    DB_NAME = "iphone_analytics"

    # Define the base directory where raw CSV files are stored
    BASE_PATH = "/user/md/iphone/raw_data"


# Entry point of the Python script
if __name__ == "__main__":
    main()

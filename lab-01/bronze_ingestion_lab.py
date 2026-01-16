from pyspark.sql import SparkSession

def main():
    # Name of the Hive database to store tables
    DB_NAME = "iphone_analytics"

    # Base directory where raw CSV files are stored
    BASE_PATH = "/user/md/iphone/raw_data"

# Entry point of the script
if __name__ == "__main__":
    main()

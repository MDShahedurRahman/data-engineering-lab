from pyspark.sql import SparkSession

def main():
    DB_NAME = "iphone_analytics"
    BASE_PATH = "/user/md/iphone/raw_data"

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("iphone-bronze-ingestion")
        .enableHiveSupport()
        .getOrCreate()
    )

if __name__ == "__main__":
    main()
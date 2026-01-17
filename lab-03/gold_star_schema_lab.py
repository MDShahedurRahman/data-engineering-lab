from pyspark.sql import SparkSession                             # Spark entry point
from pyspark.sql.functions import col, year, month, dayofmonth   # Date and column functions


def main():
    # Hive database name
    DB_NAME = "iphone_analytics"

# Script entry point
if __name__ == "__main__":
    main()

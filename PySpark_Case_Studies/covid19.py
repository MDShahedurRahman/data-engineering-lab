from pyspark.sql import SparkSession
from pyspark.sql import functions as Func

if __name__ == "__main__":
    spark = SparkSession.builder.appName("covid19").master("local[*]").enableHiveSupport().getOrCreate()
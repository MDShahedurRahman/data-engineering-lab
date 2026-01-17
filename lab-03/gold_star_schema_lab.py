from pyspark.sql import SparkSession                             # Spark entry point
from pyspark.sql.functions import col, year, month, dayofmonth   # Date and column functions


def create_gold_tables(spark):
    # Create customer dimension table if it does not already exist
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_customer (
          customer_id INT,
          customer_name STRING,
          city STRING,
          state STRING
        )
        STORED AS PARQUET
    """)

    # Create product dimension table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_product (
          product_id INT,
          product_name STRING,
          category STRING,
          unit_price INT
        )
        STORED AS PARQUET
    """)

    # Create store dimension table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_store (
          store_id INT,
          store_name STRING,
          city STRING,
          state STRING
        )
        STORED AS PARQUET
    """)

    # Create date dimension table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_date (
          date_key DATE,
          year INT,
          month INT,
          day INT
        )
        STORED AS PARQUET
    """)

    # Create sales fact table partitioned by date_key
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fact_sales (
          sale_id INT,
          customer_id INT,
          product_id INT,
          store_id INT,
          quantity INT,
          total_amount INT
        )
        PARTITIONED BY (date_key DATE)
        STORED AS PARQUET
    """)


def load_dim_customer(spark):
    # Read cleaned customer data from silver layer
    df = spark.table("silver_customers").select(
        "customer_id", "customer_name", "city", "state"
    )

    # Load data into customer dimension table
    (
        df.write
        .mode("overwrite")             # Replace existing data
        .format("parquet")             # Store as Parquet
        .saveAsTable("dim_customer")   # Save to dim_customer table
    )


def main():
    # Hive database name
    DB_NAME = "iphone_analytics"

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder
        .master("local[*]")  # Use all available local cores
        .appName("iphone-gold-layer")  # Application name
        .enableHiveSupport()  # Enable Hive support
        .getOrCreate()  # Create or reuse SparkSession
    )

    # Set active database
    spark.sql(f"USE {DB_NAME}")

    # Create gold-layer dimension and fact tables
    create_gold_tables(spark)

    # Load dimension tables
    load_dim_customer(spark)

# Script entry point
if __name__ == "__main__":
    main()

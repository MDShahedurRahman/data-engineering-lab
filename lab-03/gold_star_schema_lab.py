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


def load_dim_product(spark):
    # Read cleaned product data from silver layer
    df = spark.table("silver_products").select(
        "product_id", "product_name", "category", "unit_price"
    )

    # Load data into product dimension table
    (
        df.write
        .mode("overwrite")            # Replace existing data
        .format("parquet")            # Use Parquet format
        .saveAsTable("dim_product")   # Save to dim_product table
    )


def load_dim_store(spark):
    # Read cleaned store data from silver layer
    df = spark.table("silver_stores").select(
        "store_id", "store_name", "city", "state"
    )

    # Load data into store dimension table
    (
        df.write
        .mode("overwrite")           # Overwrite existing data
        .format("parquet")           # Store as Parquet
        .saveAsTable("dim_store")    # Save to dim_store table
    )


def load_dim_date(spark):
    # Read sales data from silver layer
    sales = spark.table("silver_sales")

    # Build date dimension from unique sale dates
    dim_date_df = (
        sales.select(col("sale_date").alias("date_key"))   # Rename sale_date to date_key
             .dropna(subset=["date_key"])                  # Remove null dates
             .dropDuplicates(["date_key"])                 # Keep unique dates only
             .withColumn("year", year(col("date_key")))    # Extract year
             .withColumn("month", month(col("date_key")))  # Extract month
             .withColumn("day", dayofmonth(col("date_key")))  # Extract day
             .select("date_key", "year", "month", "day")   # Final column selection
    )

    # Load data into date dimension table
    (
        dim_date_df.write
        .mode("overwrite")          # Overwrite existing data
        .format("parquet")          # Store as Parquet
        .saveAsTable("dim_date")    # Save to dim_date table
    )


def load_fact_sales(spark):
    # Read sales data from silver layer
    sales = spark.table("silver_sales")

    # Read product data to calculate total sales amount
    products = spark.table("silver_products")

    # Join sales with products and calculate total_amount
    fact_df = (
        sales.join(products, "product_id")                 # Join on product_id
             .withColumn("total_amount",                   # Calculate revenue per sale
                         col("quantity") * col("unit_price"))
             .select(
                    "sale_id",
                    "customer_id",
                    "product_id",
                    "store_id",
                    col("sale_date").alias("date_key"),    # Rename sale_date for partitioning
                    "quantity",
                    "total_amount"
        )
    )

    # Load data into fact_sales table partitioned by date
    (
        fact_df.write
        .mode("overwrite")           # Replace existing fact data
        .partitionBy("date_key")     # Partition for efficient date-based queries
        .format("parquet")           # Store as Parquet
        .saveAsTable("fact_sales")   # Save to fact_sales table
    )


def main():
    # Hive database name
    DB_NAME = "iphone_analytics"

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder
        .master("local[*]")                 # Use all available local cores
        .appName("iphone-gold-layer")       # Application name
        .enableHiveSupport()                # Enable Hive support
        .getOrCreate()                      # Create or reuse SparkSession
    )

    # Set active database
    spark.sql(f"USE {DB_NAME}")

    # Create gold-layer dimension and fact tables
    create_gold_tables(spark)

    # Load dimension tables
    load_dim_customer(spark)
    load_dim_product(spark)
    load_dim_store(spark)
    load_dim_date(spark)

    # Load fact table
    load_fact_sales(spark)

    # Log successful completion
    print("Gold layer completed: all dims and fact_sales created and loaded.")

    # Stop SparkSession and release resources
    spark.stop()


# Script entry point
if __name__ == "__main__":
    main()

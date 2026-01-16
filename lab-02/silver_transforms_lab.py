from pyspark.sql import SparkSession                      # Spark entry point
from pyspark.sql.functions import col, to_date, trim     # Common Spark SQL functions


def silver_customers_transform(spark):
    # Read data from the bronze customers table
    df = spark.table("bronze_customers")

    # Clean and standardize customer data
    clean_df = (
        df.withColumn("customer_id", col("customer_id").cast("int"))  # Cast customer_id to integer
        .withColumn("customer_name", trim(col("customer_name")))  # Remove extra spaces from name
        .withColumn("city", trim(col("city")))  # Clean city field
        .withColumn("state", trim(col("state")))  # Clean state field
        .dropna(subset=["customer_id"])  # Remove records with null customer_id
        .dropDuplicates(["customer_id"])  # Deduplicate by customer_id
    )

    # Write cleaned data to the silver customers table in Parquet format
    (
        clean_df.write
        .mode("overwrite")  # Overwrite existing silver table
        .format("parquet")  # Store data as Parquet
        .saveAsTable("silver_customers")  # Save as Hive-managed table
    )

    # Return created table name
    return "silver_customers"

def silver_products_transform(spark):
    # Read data from the bronze products table
    df = spark.table("bronze_products")

    # Clean and standardize product data
    clean_df = (
        df.withColumn("product_id", col("product_id").cast("int"))  # Cast product_id to integer
        .withColumn("product_name", trim(col("product_name")))  # Trim product name
        .withColumn("category", trim(col("category")))  # Trim category field
        .withColumn("unit_price", col("unit_price").cast("int"))  # Cast unit_price to integer
        .dropna(subset=["product_id"])  # Remove rows with null product_id
        .dropDuplicates(["product_id"])  # Deduplicate by product_id
    )

    # Write cleaned data to the silver products table
    (
        clean_df.write
        .mode("overwrite")  # Overwrite existing table
        .format("parquet")  # Use Parquet storage
        .saveAsTable("silver_products")  # Save as Hive table
    )

    # Return created table name
    return "silver_products"

def silver_stores_transform(spark):
    # Read data from the bronze stores table
    df = spark.table("bronze_stores")

    # Clean and standardize store data
    clean_df = (
        df.withColumn("store_id", col("store_id").cast("int"))  # Cast store_id to integer
        .withColumn("store_name", trim(col("store_name")))  # Trim store name
        .withColumn("city", trim(col("city")))  # Clean city field
        .withColumn("state", trim(col("state")))  # Clean state field
        .dropna(subset=["store_id"])  # Remove null store_id records
        .dropDuplicates(["store_id"])  # Deduplicate by store_id
    )

    # Write cleaned data to the silver stores table
    (
        clean_df.write
        .mode("overwrite")  # Overwrite existing table
        .format("parquet")  # Store as Parquet
        .saveAsTable("silver_stores")  # Save as Hive table
    )

    # Return created table name
    return "silver_stores"

def main():
    # Define the Hive database name
    DB_NAME = "iphone_analytics"

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder
        .master("local[*]")                     # Run locally using all available cores
        .appName("iphone-silver-transforms")    # Application name
        .enableHiveSupport()                    # Enable Hive metastore support
        .getOrCreate()                          # Create or reuse SparkSession
    )

    # Set the active database
    spark.sql(f"USE {DB_NAME}")

    # Execute silver-layer transformations and log results
    print("Created table:", silver_customers_transform(spark))
    print("Created table:", silver_products_transform(spark))
    print("Created table:", silver_stores_transform(spark))
    print("Created table:", silver_sales_transform(spark))

    # Stop SparkSession and free resources
    spark.stop()


# Script entry point
if __name__ == "__main__":
    main()

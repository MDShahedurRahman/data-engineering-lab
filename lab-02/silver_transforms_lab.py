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

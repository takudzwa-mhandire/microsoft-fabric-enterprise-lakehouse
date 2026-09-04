# Microsoft Fabric Enterprise Lakehouse
# Bronze Layer Ingestion
#
# Purpose:
# Ingest raw CSV source files from the Lakehouse Files area
# and persist them as Bronze Delta tables.
#
# Bronze principles used:
# - Preserve source data without business transformations
# - Keep source columns as strings
# - Add ingestion metadata
# - Store data in Delta format

from pyspark.sql.functions import current_timestamp, lit

# ---------------------------------------------------------
# 1. Create Bronze schema
# ---------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")


# ---------------------------------------------------------
# 2. Define source location and source datasets
# ---------------------------------------------------------

source_path = "Files/bronze/raw/"

source_files = {
    "customers": "customers.csv",
    "products": "products.csv",
    "suppliers": "suppliers.csv",
    "business_locations": "business_locations.csv",
    "sales_orders": "sales_orders.csv",
    "sales_order_lines": "sales_order_lines.csv",
    "inventory": "inventory.csv",
    "payments": "payments.csv"
}


# ---------------------------------------------------------
# 3. Ingest each source file into the Bronze layer
# ---------------------------------------------------------

total_records = 0

for table_name, file_name in source_files.items():

    file_path = f"{source_path}{file_name}"

    # Read raw CSV.
    # inferSchema is disabled so Bronze preserves source values
    # without applying business-level data typing.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
    )

    # Add ingestion metadata
    df = (
        df
        .withColumn("_source_file", lit(file_name))
        .withColumn("_ingested_at", current_timestamp())
    )

    # Persist as managed Delta table
    target_table = f"bronze.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    record_count = df.count()
    total_records += record_count

    print(
        f"Loaded {record_count} records "
        f"from {file_name} into {target_table}"
    )


# ---------------------------------------------------------
# 4. Ingestion summary
# ---------------------------------------------------------

print("---------------------------------------------")
print(f"Bronze ingestion completed successfully.")
print(f"Tables loaded: {len(source_files)}")
print(f"Total records loaded: {total_records}")
print("---------------------------------------------")

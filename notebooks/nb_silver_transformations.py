# Microsoft Fabric Enterprise Lakehouse
# Silver Layer Transformations
#
# Purpose:
# Clean, standardise and type the Bronze data before making it
# available for business modelling in the Gold layer.
#
# Silver principles used:
# - Apply correct data types
# - Trim text fields
# - Remove duplicate business keys
# - Validate mandatory identifiers
# - Preserve ingestion metadata
# - Store cleansed data as Delta tables

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ---------------------------------------------------------
# 1. Create Silver schema
# ---------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")


# ---------------------------------------------------------
# 2. Customers
# ---------------------------------------------------------

customers = (
    spark.table("bronze.customers")
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("customer_type", F.trim(F.col("customer_type")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("province", F.trim(F.col("province")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"), "yyyy-MM-dd")
    )
    .withColumn("customer_status", F.trim(F.col("customer_status")))
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.customers")


# ---------------------------------------------------------
# 3. Products
# ---------------------------------------------------------

products = (
    spark.table("bronze.products")
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("supplier_id", F.col("supplier_id").cast("int"))
    .withColumn(
        "unit_cost",
        F.col("unit_cost").cast(DecimalType(18, 2))
    )
    .withColumn(
        "selling_price",
        F.col("selling_price").cast(DecimalType(18, 2))
    )
    .withColumn("product_status", F.trim(F.col("product_status")))
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.products")


# ---------------------------------------------------------
# 4. Suppliers
# ---------------------------------------------------------

suppliers = (
    spark.table("bronze.suppliers")
    .withColumn("supplier_id", F.col("supplier_id").cast("int"))
    .withColumn("supplier_name", F.trim(F.col("supplier_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("supplier_status", F.trim(F.col("supplier_status")))
    .filter(F.col("supplier_id").isNotNull())
    .dropDuplicates(["supplier_id"])
)

suppliers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.suppliers")


# ---------------------------------------------------------
# 5. Business Locations
# ---------------------------------------------------------

business_locations = (
    spark.table("bronze.business_locations")
    .withColumn("location_id", F.col("location_id").cast("int"))
    .withColumn("location_name", F.trim(F.col("location_name")))
    .withColumn("location_type", F.trim(F.col("location_type")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("province", F.trim(F.col("province")))
    .withColumn("country", F.trim(F.col("country")))
    .filter(F.col("location_id").isNotNull())
    .dropDuplicates(["location_id"])
)

business_locations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.business_locations")


# ---------------------------------------------------------
# 6. Sales Orders
# ---------------------------------------------------------

sales_orders = (
    spark.table("bronze.sales_orders")
    .withColumn("order_id", F.col("order_id").cast("int"))
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("location_id", F.col("location_id").cast("int"))
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"), "yyyy-MM-dd")
    )
    .withColumn("order_status", F.trim(F.col("order_status")))
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

sales_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.sales_orders")


# ---------------------------------------------------------
# 7. Sales Order Lines
# ---------------------------------------------------------

sales_order_lines = (
    spark.table("bronze.sales_order_lines")
    .withColumn(
        "order_line_id",
        F.col("order_line_id").cast("int")
    )
    .withColumn("order_id", F.col("order_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn(
        "unit_price",
        F.col("unit_price").cast(DecimalType(18, 2))
    )
    .withColumn(
        "discount_amount",
        F.col("discount_amount").cast(DecimalType(18, 2))
    )
    .withColumn(
        "line_total",
        F.col("line_total").cast(DecimalType(18, 2))
    )
    .filter(F.col("order_line_id").isNotNull())
    .dropDuplicates(["order_line_id"])
)

sales_order_lines.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.sales_order_lines")


# ---------------------------------------------------------
# 8. Inventory
# ---------------------------------------------------------

inventory = (
    spark.table("bronze.inventory")
    .withColumn("inventory_id", F.col("inventory_id").cast("int"))
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("location_id", F.col("location_id").cast("int"))
    .withColumn(
        "quantity_on_hand",
        F.col("quantity_on_hand").cast("int")
    )
    .withColumn(
        "reorder_level",
        F.col("reorder_level").cast("int")
    )
    .withColumn(
        "last_updated",
        F.to_timestamp(
            F.col("last_updated"),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )
    .filter(F.col("inventory_id").isNotNull())
    .filter(F.col("quantity_on_hand") >= 0)
    .filter(F.col("reorder_level") >= 0)
    .dropDuplicates(["inventory_id"])
)

inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.inventory")


# ---------------------------------------------------------
# 9. Payments
# ---------------------------------------------------------

payments = (
    spark.table("bronze.payments")
    .withColumn("payment_id", F.col("payment_id").cast("int"))
    .withColumn("order_id", F.col("order_id").cast("int"))
    .withColumn(
        "payment_date",
        F.to_date(F.col("payment_date"), "yyyy-MM-dd")
    )
    .withColumn(
        "payment_amount",
        F.col("payment_amount").cast(DecimalType(18, 2))
    )
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    .withColumn("payment_status", F.trim(F.col("payment_status")))
    .withColumn(
        "payment_reference",
        F.trim(F.col("payment_reference"))
    )
    .filter(F.col("payment_id").isNotNull())
    .filter(F.col("payment_amount") >= 0)
    .dropDuplicates(["payment_id"])
)

payments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.payments")


# ---------------------------------------------------------
# 10. Silver Layer Validation Summary
# ---------------------------------------------------------

silver_tables = [
    "customers",
    "products",
    "suppliers",
    "business_locations",
    "sales_orders",
    "sales_order_lines",
    "inventory",
    "payments"
]

total_records = 0

print("--------------------------------------------------")
print("Silver transformation completed successfully.")
print("--------------------------------------------------")

for table_name in silver_tables:

    record_count = spark.table(
        f"silver.{table_name}"
    ).count()

    total_records += record_count

    print(
        f"silver.{table_name}: "
        f"{record_count} records"
    )

print("--------------------------------------------------")
print(f"Tables created: {len(silver_tables)}")
print(f"Total Silver records: {total_records}")
print("--------------------------------------------------")

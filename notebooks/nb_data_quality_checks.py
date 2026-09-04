# Microsoft Fabric Enterprise Lakehouse
# Silver Layer Data Quality Checks
#
# Purpose:
# Validate the cleansed Silver layer before data is promoted
# into business-ready Gold models.
#
# Results are written to:
# audit.silver_data_quality_results

from pyspark.sql import functions as F
from datetime import datetime


# ---------------------------------------------------------
# 1. Create Audit schema
# ---------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS audit")


# ---------------------------------------------------------
# 2. Load Silver tables
# ---------------------------------------------------------

customers = spark.table("silver.customers")
products = spark.table("silver.products")
suppliers = spark.table("silver.suppliers")
locations = spark.table("silver.business_locations")
orders = spark.table("silver.sales_orders")
order_lines = spark.table("silver.sales_order_lines")
inventory = spark.table("silver.inventory")
payments = spark.table("silver.payments")


# ---------------------------------------------------------
# 3. Helper function for recording results
# ---------------------------------------------------------

quality_results = []


def add_check(check_name, table_name, failed_records):
    """
    Add one data-quality result to the audit list.
    A check passes when no invalid records are found.
    """

    status = "PASS" if failed_records == 0 else "FAIL"

    quality_results.append(
        (
            check_name,
            table_name,
            failed_records,
            status,
            datetime.now()
        )
    )

    print(
        f"{check_name}: "
        f"{status} "
        f"(failed records = {failed_records})"
    )


# ---------------------------------------------------------
# CHECK 1
# Customers must have unique customer IDs
# ---------------------------------------------------------

customer_duplicates = (
    customers
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

add_check(
    "Customer ID uniqueness",
    "silver.customers",
    customer_duplicates
)


# ---------------------------------------------------------
# CHECK 2
# Products must have unique product IDs
# ---------------------------------------------------------

product_duplicates = (
    products
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

add_check(
    "Product ID uniqueness",
    "silver.products",
    product_duplicates
)


# ---------------------------------------------------------
# CHECK 3
# Sales orders must have unique order IDs
# ---------------------------------------------------------

order_duplicates = (
    orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

add_check(
    "Order ID uniqueness",
    "silver.sales_orders",
    order_duplicates
)


# ---------------------------------------------------------
# CHECK 4
# Mandatory primary identifiers must not be null
# ---------------------------------------------------------

null_primary_keys = (
    customers.filter(F.col("customer_id").isNull()).count()
    + products.filter(F.col("product_id").isNull()).count()
    + suppliers.filter(F.col("supplier_id").isNull()).count()
    + locations.filter(F.col("location_id").isNull()).count()
    + orders.filter(F.col("order_id").isNull()).count()
    + order_lines.filter(F.col("order_line_id").isNull()).count()
    + inventory.filter(F.col("inventory_id").isNull()).count()
    + payments.filter(F.col("payment_id").isNull()).count()
)

add_check(
    "Mandatory primary keys not null",
    "silver",
    null_primary_keys
)


# ---------------------------------------------------------
# CHECK 5
# Product prices and costs must be valid
# ---------------------------------------------------------

invalid_product_prices = (
    products
    .filter(
        (F.col("unit_cost") < 0)
        | (F.col("selling_price") <= 0)
    )
    .count()
)

add_check(
    "Valid product prices",
    "silver.products",
    invalid_product_prices
)


# ---------------------------------------------------------
# CHECK 6
# Sales line totals must match:
# quantity × unit price − discount
# ---------------------------------------------------------

invalid_line_totals = (
    order_lines
    .filter(
        F.abs(
            F.col("line_total")
            - (
                F.col("quantity") * F.col("unit_price")
                - F.col("discount_amount")
            )
        ) > 0.01
    )
    .count()
)

add_check(
    "Sales line total calculation",
    "silver.sales_order_lines",
    invalid_line_totals
)


# ---------------------------------------------------------
# CHECK 7
# Inventory quantities must not be negative
# ---------------------------------------------------------

invalid_inventory = (
    inventory
    .filter(
        (F.col("quantity_on_hand") < 0)
        | (F.col("reorder_level") < 0)
    )
    .count()
)

add_check(
    "Valid inventory quantities",
    "silver.inventory",
    invalid_inventory
)


# ---------------------------------------------------------
# CHECK 8
# Sales orders must reference valid customers and locations
# ---------------------------------------------------------

invalid_order_references = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .join(
        locations.alias("l"),
        F.col("o.location_id") == F.col("l.location_id"),
        "left"
    )
    .filter(
        F.col("c.customer_id").isNull()
        | F.col("l.location_id").isNull()
    )
    .count()
)

add_check(
    "Sales order referential integrity",
    "silver.sales_orders",
    invalid_order_references
)


# ---------------------------------------------------------
# CHECK 9
# Payments must reference valid sales orders
# ---------------------------------------------------------

invalid_payment_references = (
    payments.alias("p")
    .join(
        orders.alias("o"),
        F.col("p.order_id") == F.col("o.order_id"),
        "left"
    )
    .filter(F.col("o.order_id").isNull())
    .count()
)

add_check(
    "Payment referential integrity",
    "silver.payments",
    invalid_payment_references
)


# ---------------------------------------------------------
# 4. Create DataFrame containing audit results
# ---------------------------------------------------------

quality_results_df = spark.createDataFrame(
    quality_results,
    [
        "check_name",
        "table_name",
        "failed_records",
        "status",
        "checked_at"
    ]
)


# ---------------------------------------------------------
# 5. Save results as Delta audit table
# ---------------------------------------------------------

quality_results_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("audit.silver_data_quality_results")


# ---------------------------------------------------------
# 6. Validation summary
# ---------------------------------------------------------

total_checks = quality_results_df.count()

passed_checks = (
    quality_results_df
    .filter(F.col("status") == "PASS")
    .count()
)

failed_checks = total_checks - passed_checks


print("--------------------------------------------------")
print("Silver Data Quality Validation Complete")
print("--------------------------------------------------")
print(f"Total checks:  {total_checks}")
print(f"Passed:        {passed_checks}")
print(f"Failed:        {failed_checks}")
print("--------------------------------------------------")

quality_results_df.show(
    truncate=False
)

# Microsoft Fabric Enterprise Lakehouse
# Gold Layer Business Models
#
# Purpose:
# Transform cleansed Silver data into business-ready analytical
# tables for reporting, reconciliation and Power BI.
#
# Gold outputs:
# - sales_transaction_detail
# - daily_sales_summary
# - customer_sales_summary
# - product_sales_summary
# - inventory_status
# - order_payment_reconciliation

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Create Gold schema
# ---------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# ---------------------------------------------------------
# 2. Load Silver tables
# ---------------------------------------------------------

customers = spark.table("silver.customers")
products = spark.table("silver.products")
locations = spark.table("silver.business_locations")
orders = spark.table("silver.sales_orders")
order_lines = spark.table("silver.sales_order_lines")
inventory = spark.table("silver.inventory")
payments = spark.table("silver.payments")


# ---------------------------------------------------------
# 3. Sales Transaction Detail
#
# Only completed orders are included in recognised sales.
# ---------------------------------------------------------

completed_orders = orders.filter(
    F.col("order_status") == "Completed"
)

sales_transaction_detail = (
    completed_orders.alias("o")
    .join(
        order_lines.alias("ol"),
        F.col("o.order_id") == F.col("ol.order_id"),
        "inner"
    )
    .join(
        products.alias("p"),
        F.col("ol.product_id") == F.col("p.product_id"),
        "left"
    )
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
    .select(
        F.col("o.order_id"),
        F.col("ol.order_line_id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("c.customer_name"),
        F.col("c.customer_type"),
        F.col("o.location_id"),
        F.col("l.location_name"),
        F.col("l.location_type"),
        F.col("ol.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("ol.quantity"),
        F.col("ol.unit_price"),
        F.col("ol.discount_amount"),
        F.col("ol.line_total").alias("sales_amount"),
        F.col("p.unit_cost"),
        (
            F.col("ol.quantity") * F.col("p.unit_cost")
        ).alias("cost_of_sales")
    )
    .withColumn(
        "gross_profit",
        F.col("sales_amount") - F.col("cost_of_sales")
    )
    .withColumn(
        "gross_margin_pct",
        F.when(
            F.col("sales_amount") != 0,
            (
                F.col("gross_profit")
                / F.col("sales_amount")
            ) * 100
        ).otherwise(F.lit(0))
    )
)

sales_transaction_detail.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.sales_transaction_detail")


# ---------------------------------------------------------
# 4. Daily Sales Summary
# ---------------------------------------------------------

daily_sales_summary = (
    sales_transaction_detail
    .groupBy(
        "order_date",
        "location_id",
        "location_name"
    )
    .agg(
        F.countDistinct("order_id").alias("completed_orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("sales_amount").alias("total_sales"),
        F.sum("cost_of_sales").alias("total_cost_of_sales"),
        F.sum("gross_profit").alias("total_gross_profit")
    )
    .withColumn(
        "gross_margin_pct",
        F.when(
            F.col("total_sales") != 0,
            (
                F.col("total_gross_profit")
                / F.col("total_sales")
            ) * 100
        ).otherwise(F.lit(0))
    )
)

daily_sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.daily_sales_summary")


# ---------------------------------------------------------
# 5. Customer Sales Summary
# ---------------------------------------------------------

customer_sales_summary = (
    sales_transaction_detail
    .groupBy(
        "customer_id",
        "customer_name",
        "customer_type"
    )
    .agg(
        F.countDistinct("order_id").alias("completed_orders"),
        F.sum("quantity").alias("units_purchased"),
        F.sum("sales_amount").alias("total_sales"),
        F.sum("gross_profit").alias("total_gross_profit")
    )
    .withColumn(
        "gross_margin_pct",
        F.when(
            F.col("total_sales") != 0,
            (
                F.col("total_gross_profit")
                / F.col("total_sales")
            ) * 100
        ).otherwise(F.lit(0))
    )
)

customer_sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.customer_sales_summary")


# ---------------------------------------------------------
# 6. Product Sales Summary
# ---------------------------------------------------------

product_sales_summary = (
    sales_transaction_detail
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        F.countDistinct("order_id").alias("completed_orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("sales_amount").alias("total_sales"),
        F.sum("cost_of_sales").alias("total_cost_of_sales"),
        F.sum("gross_profit").alias("total_gross_profit")
    )
    .withColumn(
        "gross_margin_pct",
        F.when(
            F.col("total_sales") != 0,
            (
                F.col("total_gross_profit")
                / F.col("total_sales")
            ) * 100
        ).otherwise(F.lit(0))
    )
)

product_sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.product_sales_summary")


# ---------------------------------------------------------
# 7. Inventory Status
# ---------------------------------------------------------

inventory_status = (
    inventory.alias("i")
    .join(
        products.alias("p"),
        F.col("i.product_id") == F.col("p.product_id"),
        "left"
    )
    .join(
        locations.alias("l"),
        F.col("i.location_id") == F.col("l.location_id"),
        "left"
    )
    .select(
        F.col("i.inventory_id"),
        F.col("i.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("i.location_id"),
        F.col("l.location_name"),
        F.col("i.quantity_on_hand"),
        F.col("i.reorder_level"),
        F.col("i.last_updated")
    )
    .withColumn(
        "stock_status",
        F.when(
            F.col("quantity_on_hand") <= F.col("reorder_level"),
            F.lit("Reorder Required")
        ).otherwise(
            F.lit("Stock OK")
        )
    )
    .withColumn(
        "reorder_quantity",
        F.when(
            F.col("quantity_on_hand") <= F.col("reorder_level"),
            F.col("reorder_level") - F.col("quantity_on_hand")
        ).otherwise(F.lit(0))
    )
)

inventory_status.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.inventory_status")


# ---------------------------------------------------------
# 8. Order / Payment Reconciliation
# ---------------------------------------------------------

order_values = (
    order_lines
    .groupBy("order_id")
    .agg(
        F.sum("line_total").alias("order_value")
    )
)

successful_payments = (
    payments
    .filter(F.col("payment_status") == "Successful")
    .groupBy("order_id")
    .agg(
        F.sum("payment_amount").alias("successful_payment_amount"),
        F.max("payment_date").alias("latest_payment_date")
    )
)

order_payment_reconciliation = (
    orders.alias("o")
    .join(
        order_values.alias("ov"),
        F.col("o.order_id") == F.col("ov.order_id"),
        "left"
    )
    .join(
        successful_payments.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.order_status"),
        F.col("o.payment_method"),
        F.col("ov.order_value"),
        F.coalesce(
            F.col("p.successful_payment_amount"),
            F.lit(0)
        ).alias("successful_payment_amount"),
        F.col("p.latest_payment_date")
    )
    .withColumn(
        "outstanding_amount",
        F.col("order_value") - F.col("successful_payment_amount")
    )
    .withColumn(
        "reconciliation_status",
        F.when(
            F.col("successful_payment_amount") == 0,
            F.lit("No Successful Payment")
        )
        .when(
            F.col("outstanding_amount") > 0,
            F.lit("Outstanding")
        )
        .when(
            F.col("outstanding_amount") < 0,
            F.lit("Overpaid")
        )
        .otherwise(
            F.lit("Fully Paid")
        )
    )
)

order_payment_reconciliation.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.order_payment_reconciliation")


# ---------------------------------------------------------
# 9. Gold Layer Validation Summary
# ---------------------------------------------------------

gold_tables = [
    "sales_transaction_detail",
    "daily_sales_summary",
    "customer_sales_summary",
    "product_sales_summary",
    "inventory_status",
    "order_payment_reconciliation"
]

print("--------------------------------------------------")
print("Gold business models completed successfully.")
print("--------------------------------------------------")

for table_name in gold_tables:

    record_count = spark.table(
        f"gold.{table_name}"
    ).count()

    print(
        f"gold.{table_name}: "
        f"{record_count} records"
    )

print("--------------------------------------------------")
print(f"Gold tables created: {len(gold_tables)}")
print("--------------------------------------------------")

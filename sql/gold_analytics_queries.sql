/*
Microsoft Fabric Enterprise Lakehouse
Gold Layer Analytics Queries

Purpose:
Provide reusable SQL queries against the curated Gold layer
for business analysis, validation and Power BI reporting.
*/


-- =========================================================
-- 1. Overall Sales Performance
-- =========================================================

SELECT
    COUNT(DISTINCT order_id) AS completed_orders,
    SUM(quantity) AS units_sold,
    SUM(sales_amount) AS total_sales,
    SUM(cost_of_sales) AS total_cost_of_sales,
    SUM(gross_profit) AS total_gross_profit,
    ROUND(
        SUM(gross_profit) * 100.0 /
        NULLIF(SUM(sales_amount), 0),
        2
    ) AS gross_margin_pct
FROM gold.sales_transaction_detail;


-- =========================================================
-- 2. Daily Sales by Location
-- =========================================================

SELECT
    order_date,
    location_name,
    completed_orders,
    units_sold,
    total_sales,
    total_cost_of_sales,
    total_gross_profit,
    gross_margin_pct
FROM gold.daily_sales_summary
ORDER BY
    order_date,
    location_name;


-- =========================================================
-- 3. Product Performance
-- =========================================================

SELECT
    product_id,
    product_name,
    category,
    completed_orders,
    units_sold,
    total_sales,
    total_cost_of_sales,
    total_gross_profit,
    gross_margin_pct
FROM gold.product_sales_summary
ORDER BY total_sales DESC;


-- =========================================================
-- 4. Customer Performance
-- =========================================================

SELECT
    customer_id,
    customer_name,
    customer_type,
    completed_orders,
    units_purchased,
    total_sales,
    total_gross_profit,
    gross_margin_pct
FROM gold.customer_sales_summary
ORDER BY total_sales DESC;


-- =========================================================
-- 5. Inventory Requiring Attention
-- =========================================================

SELECT
    inventory_id,
    product_id,
    product_name,
    category,
    location_id,
    location_name,
    quantity_on_hand,
    reorder_level,
    reorder_quantity,
    last_updated,
    stock_status
FROM gold.inventory_status
WHERE stock_status = 'Reorder Required'
ORDER BY
    reorder_quantity DESC,
    product_name;


-- =========================================================
-- 6. Outstanding and Unreconciled Payments
-- =========================================================

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    payment_method,
    order_value,
    successful_payment_amount,
    outstanding_amount,
    latest_payment_date,
    reconciliation_status
FROM gold.order_payment_reconciliation
WHERE reconciliation_status <> 'Fully Paid'
ORDER BY
    outstanding_amount DESC,
    order_date;


-- =========================================================
-- 7. Gold Table Record Counts
-- =========================================================

SELECT
    'sales_transaction_detail' AS table_name,
    COUNT(*) AS record_count
FROM gold.sales_transaction_detail

UNION ALL

SELECT
    'daily_sales_summary',
    COUNT(*)
FROM gold.daily_sales_summary

UNION ALL

SELECT
    'customer_sales_summary',
    COUNT(*)
FROM gold.customer_sales_summary

UNION ALL

SELECT
    'product_sales_summary',
    COUNT(*)
FROM gold.product_sales_summary

UNION ALL

SELECT
    'inventory_status',
    COUNT(*)
FROM gold.inventory_status

UNION ALL

SELECT
    'order_payment_reconciliation',
    COUNT(*)
FROM gold.order_payment_reconciliation;

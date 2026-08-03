# Monitoring and Operational Validation

## Overview

Microsoft Fabric's Monitoring hub was used to review the execution of the Bronze, Silver and Gold notebooks:

- `nb_bronze_ingestion`
- `nb_silver_transformations`
- `nb_gold_business_models`

The notebook sessions show a final status of **Stopped** because the interactive Spark sessions were stopped or timed out after processing. The individual Spark jobs completed successfully.

![Fabric Monitoring Overview](images/monitoring-overview.png)

## Gold-Layer Spark Run

The detailed Gold-layer monitoring view confirmed:

- Individual Spark jobs succeeded
- Queue duration was 0 seconds
- Total application duration was 21 minutes 49 seconds
- Six Gold business-ready tables were created

The tables created were:

1. `gold.sales_transaction_detail`
2. `gold.daily_sales_summary`
3. `gold.customer_sales_summary`
4. `gold.product_sales_summary`
5. `gold.inventory_status`
6. `gold.order_payment_reconciliation`

![Gold Spark Run](images/gold-spark-run.png)

## Monitoring Approach

The solution includes the following operational checks:

- Review notebook activities in the Fabric Monitoring hub
- Confirm Spark jobs and stages completed successfully
- Review run and queue durations
- Confirm expected Delta tables were created
- Validate record counts between processing layers
- Review data-quality results stored in the audit schema

## Production Enhancements

A production implementation could also include:

- Scheduled pipeline execution
- Failure notifications
- Data-quality threshold alerts
- Row-count reconciliation alerts
- Incremental-load monitoring
- Capacity and Spark-session monitoring
- Power BI semantic-model refresh monitoring

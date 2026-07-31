# Source Data Design

## Business Context

The fictional organisation operates across several locations and sells products to customers.

Its operational data is stored in separate systems for customers, products, sales, inventory, suppliers and business locations. The Microsoft Fabric solution will combine these datasets into one trusted analytics platform.

---

## Source Datasets

### 1. Customers

Contains customer master information.

| Column | Data Type | Description |
|---|---|---|
| customer_id | Integer | Unique customer identifier |
| customer_name | String | Customer or company name |
| customer_type | String | Individual or Business |
| city | String | Customer city |
| province | String | Customer province |
| country | String | Customer country |
| registration_date | Date | Date the customer was registered |
| customer_status | String | Active or Inactive |

**Primary key:** `customer_id`

---

### 2. Products

Contains the organisation's product catalogue.

| Column | Data Type | Description |
|---|---|---|
| product_id | Integer | Unique product identifier |
| product_name | String | Product description |
| category | String | Product category |
| supplier_id | Integer | Supplier responsible for the product |
| unit_cost | Decimal | Cost paid to the supplier |
| selling_price | Decimal | Customer selling price |
| product_status | String | Active or Discontinued |

**Primary key:** `product_id`

---

### 3. Sales Orders

Contains sales-order header information.

| Column | Data Type | Description |
|---|---|---|
| order_id | Integer | Unique sales-order identifier |
| customer_id | Integer | Customer who placed the order |
| location_id | Integer | Business location processing the order |
| order_date | Date | Date the order was created |
| order_status | String | Pending, Completed or Cancelled |
| payment_method | String | Payment method used |

**Primary key:** `order_id`

---

### 4. Sales Order Lines

Contains the individual products included in each order.

| Column | Data Type | Description |
|---|---|---|
| order_line_id | Integer | Unique order-line identifier |
| order_id | Integer | Related sales order |
| product_id | Integer | Product sold |
| quantity | Integer | Quantity sold |
| unit_price | Decimal | Selling price per unit |
| discount_amount | Decimal | Discount applied |
| line_total | Decimal | Total value of the order line |

**Primary key:** `order_line_id`

---

### 5. Inventory

Contains product stock levels by location.

| Column | Data Type | Description |
|---|---|---|
| inventory_id | Integer | Unique inventory record |
| product_id | Integer | Related product |
| location_id | Integer | Location holding the stock |
| quantity_on_hand | Integer | Current available quantity |
| reorder_level | Integer | Minimum preferred stock level |
| last_updated | DateTime | Last inventory update |

**Primary key:** `inventory_id`

---

### 6. Suppliers

Contains supplier master information.

| Column | Data Type | Description |
|---|---|---|
| supplier_id | Integer | Unique supplier identifier |
| supplier_name | String | Supplier name |
| city | String | Supplier city |
| country | String | Supplier country |
| supplier_status | String | Active or Inactive |

**Primary key:** `supplier_id`

---

### 7. Business Locations

Contains the organisation's branches, warehouses or operational sites.

| Column | Data Type | Description |
|---|---|---|
| location_id | Integer | Unique location identifier |
| location_name | String | Branch, warehouse or site name |
| location_type | String | Branch, Warehouse or Distribution Centre |
| city | String | Location city |
| province | String | Location province |
| country | String | Location country |

**Primary key:** `location_id`

---

## Dataset Relationships

- One customer can have many sales orders.
- One sales order can contain many sales-order lines.
- One product can appear in many sales-order lines.
- One supplier can supply many products.
- One location can process many sales orders.
- One product can have inventory at several locations.

---

## Planned Data Quality Rules

The pipeline will check that:

- Primary keys are not blank.
- Duplicate records are identified.
- Quantities are not negative.
- Selling prices and unit costs are valid.
- Sales-order dates are valid.
- Customer and product references exist.
- Order-line totals agree with quantity, price and discount.
- Inventory records contain valid product and location identifiers.

---

## Next Step

Create fictional CSV source files based on this design and ingest them into the Bronze layer of the Microsoft Fabric Lakehouse.

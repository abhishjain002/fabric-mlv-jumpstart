# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
# META       "default_lakehouse_name": "mlv_lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%md
# MAGIC # Materialized Lake Views — Get Started
# MAGIC
# MAGIC **Jumpstart: Materialized Lake Views Get Started**
# MAGIC
# MAGIC Welcome to the Materialized Lake Views tutorial! In this notebook, you will:
# MAGIC
# MAGIC 1. **Create source tables** in a `bronze` schema with sample products and orders data
# MAGIC 2. **Enable Change Data Feed (CDF)** on source tables for optimal incremental refresh
# MAGIC 3. **Create materialized lake views** in `silver` and `gold` schemas using the medallion architecture
# MAGIC 4. **Verify results** by querying the gold materialized lake view
# MAGIC 5. **Learn about scheduled refresh** and automatic lineage tracking
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - A workspace with a Microsoft Fabric-enabled capacity
# MAGIC - A lakehouse with **lakehouse schemas enabled** and **Fabric Runtime 1.3**
# MAGIC
# MAGIC ### What are Materialized Lake Views?
# MAGIC Materialized lake views are precomputed, persisted query results stored as Delta tables in your lakehouse.
# MAGIC They automatically refresh when source data changes, giving you always-up-to-date transformed data
# MAGIC without manual orchestration.
# MAGIC
# MAGIC > **Run each cell below in order.** The notebook is fully self-contained.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## Step 1: Create Source Tables (Bronze Layer)
# MAGIC
# MAGIC We create a `bronze` schema and populate it with two tables:
# MAGIC - **`bronze.products`** — product catalog with ID, name, and price
# MAGIC - **`bronze.orders`** — order records with order ID, product reference, quantity, and date
# MAGIC
# MAGIC These are the raw source tables that our materialized lake views will transform.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products (
# MAGIC    product_id INT,
# MAGIC    product_name STRING,
# MAGIC    price DOUBLE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO bronze.products VALUES
# MAGIC (101, 'Laptop', 1200.50),
# MAGIC (102, 'Smartphone', 699.99),
# MAGIC (103, 'Tablet', 450.00);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders (
# MAGIC    order_id INT,
# MAGIC    product_id INT,
# MAGIC    quantity INT,
# MAGIC    order_date DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO bronze.orders VALUES
# MAGIC    (1001, 101, 2, '2025-06-01'),
# MAGIC    (1002, 103, 1, '2025-06-02'),
# MAGIC    (1003, 102, 3, '2025-06-03');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ### Verify Source Tables
# MAGIC
# MAGIC Refresh the **Lakehouse explorer** on the left panel to see the newly created `products` and `orders`
# MAGIC tables under the `bronze` schema.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Quick verification: view the source data
# MAGIC SELECT 'products' AS table_name, COUNT(*) AS row_count FROM bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM bronze.orders;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## Step 2: Enable Change Data Feed (CDF)
# MAGIC
# MAGIC Change Data Feed tracks row-level changes (inserts, updates, deletes) on Delta tables.
# MAGIC Enabling CDF on source tables allows materialized lake views to use **incremental refresh** —
# MAGIC processing only the changed data instead of recomputing everything from scratch.
# MAGIC
# MAGIC This is a **one-time setup** per source table. Learn more:
# MAGIC [Optimal refresh for materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view)

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE bronze.products SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE bronze.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## Step 3: Create Materialized Lake Views (Silver & Gold Layers)
# MAGIC
# MAGIC Now we define materialized lake views that implement the **medallion architecture**:
# MAGIC
# MAGIC | Layer | View | Purpose |
# MAGIC |-------|------|---------|
# MAGIC | **Silver** | `silver.cleaned_order_data` | Joins orders with products, calculates per-order revenue |
# MAGIC | **Gold** | `gold.product_sales_summary` | Aggregates by product — total quantity, revenue, and average order value |
# MAGIC
# MAGIC Materialized lake views are created with `CREATE MATERIALIZED LAKE VIEW` syntax.
# MAGIC Fabric persists the results as Delta tables and automatically manages refresh.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS silver.cleaned_order_data AS
# MAGIC SELECT
# MAGIC    o.order_id,
# MAGIC    o.order_date,
# MAGIC    o.product_id,
# MAGIC    p.product_name,
# MAGIC    o.quantity,
# MAGIC    p.price,
# MAGIC    o.quantity * p.price AS revenue
# MAGIC FROM bronze.orders o
# MAGIC JOIN bronze.products p
# MAGIC ON o.product_id = p.product_id;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.product_sales_summary AS
# MAGIC SELECT
# MAGIC    product_id,
# MAGIC    product_name,
# MAGIC    SUM(quantity) AS total_quantity_sold,
# MAGIC    SUM(revenue) AS total_revenue,
# MAGIC    ROUND(AVG(revenue), 2) AS average_order_value
# MAGIC FROM
# MAGIC    silver.cleaned_order_data
# MAGIC GROUP BY
# MAGIC    product_id,
# MAGIC    product_name;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## Step 4: Verify Results
# MAGIC
# MAGIC Query the gold materialized lake view to see the aggregated product sales summary.
# MAGIC You should see **3 rows** — one for each product — with total quantity sold, total revenue,
# MAGIC and average order value.
# MAGIC
# MAGIC Refresh the **Lakehouse explorer** to see `cleaned_order_data` and `product_sales_summary`
# MAGIC under the `silver` and `gold` schemas respectively.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM gold.product_sales_summary;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ### Expected Output
# MAGIC
# MAGIC | product_id | product_name | total_quantity_sold | total_revenue | average_order_value |
# MAGIC |------------|-------------|---------------------|---------------|---------------------|
# MAGIC | 101 | Laptop | 2 | 2401.00 | 2401.00 |
# MAGIC | 102 | Smartphone | 3 | 2099.97 | 2099.97 |
# MAGIC | 103 | Tablet | 1 | 450.00 | 450.00 |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## Step 5: Schedule Refresh & Monitor Lineage
# MAGIC
# MAGIC Fabric can automatically keep your materialized lake views up to date as source data changes.
# MAGIC Follow these steps to set up scheduled refresh:
# MAGIC
# MAGIC ### Set Up Scheduled Refresh
# MAGIC 1. **Close this notebook** and go back to your lakehouse
# MAGIC 2. Select **Manage materialized lake views (preview)** from the lakehouse menu
# MAGIC 3. You should see the auto-generated **lineage graph** showing:
# MAGIC    - `bronze.products` → `silver.cleaned_order_data` → `gold.product_sales_summary`
# MAGIC    - `bronze.orders` → `silver.cleaned_order_data` → `gold.product_sales_summary`
# MAGIC 4. Select **Schedules** from the top ribbon
# MAGIC 5. In the Schedules pane, select **On** for Schedule refresh
# MAGIC 6. Choose the desired frequency (by the minute, hourly, daily, weekly, or monthly)
# MAGIC 7. Specify the recurring interval and select **Apply**
# MAGIC
# MAGIC ### How Automatic Refresh Works
# MAGIC Once scheduled, Fabric automatically:
# MAGIC - **Detects changes** in source tables (`bronze.orders`, `bronze.products`)
# MAGIC - **Determines refresh order** based on the lineage graph (silver before gold)
# MAGIC - **Chooses the optimal refresh strategy**: incremental (using CDF), full, or skip
# MAGIC - **Refreshes dependent views** in the correct order — no orchestration needed!
# MAGIC
# MAGIC ### Test the Refresh
# MAGIC Try inserting new data into a source table, then wait for the next scheduled run:
# MAGIC ```sql
# MAGIC INSERT INTO bronze.orders VALUES (1004, 101, 1, '2025-06-04');
# MAGIC ```
# MAGIC After the refresh completes, `gold.product_sales_summary` will automatically reflect
# MAGIC the updated totals for product 101 (Laptop).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Congratulations! You've built a working **bronze → silver → gold** pipeline with
# MAGIC materialized lake views and automatic lineage-based refresh.
# MAGIC
# MAGIC ### Learn More
# MAGIC - [What are materialized lake views?](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view)
# MAGIC - [Spark SQL reference for materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
# MAGIC - [Refresh behavior (incremental, full, skip)](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view)
# MAGIC - [Full medallion architecture tutorial](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/tutorial)

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

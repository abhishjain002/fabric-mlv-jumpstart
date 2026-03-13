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

# MARKDOWN ********************

# # Materialized Lake Views — Get Started
# 
# **Jumpstart: Materialized Lake Views Get Started**
# 
# Welcome to the Materialized Lake Views tutorial! In this notebook, you will:
# 
# 1. **Create source tables** in a `bronze` schema with sample products and orders data
# 2. **Enable Change Data Feed (CDF)** on source tables for optimal incremental refresh
# 3. **Create materialized lake views** in `silver` and `gold` schemas using the medallion architecture
# 4. **Verify results** by querying the gold materialized lake view
# 5. **Learn about scheduled refresh** and automatic lineage tracking
# 
# ### What are Materialized Lake Views?
# Materialized lake views are precomputed, persisted query results stored as Delta tables in your lakehouse.
# They automatically refresh when source data changes, giving you always-up-to-date transformed data
# without manual orchestration.
# 
# > **Run each cell below in order.** The notebook is fully self-contained.

# MARKDOWN ********************

# ## Step 1: Create Source Tables (Bronze Layer)
# 
# We create a `bronze` schema and populate it with two tables:
# - **`bronze.products`** — product catalog with ID, name, and price
# - **`bronze.orders`** — order records with order ID, product reference, quantity, and date
# 
# These are the raw source tables that our materialized lake views will transform.

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

# MARKDOWN ********************

# ### Verify Source Tables
# 
# Refresh the **Lakehouse explorer** on the left panel to see the newly created `products` and `orders`
# tables under the `bronze` schema.

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

# MARKDOWN ********************

# ## Step 2: Enable Change Data Feed (CDF)
# 
# Change Data Feed tracks row-level changes (inserts, updates, deletes) on Delta tables.
# Enabling CDF on source tables allows materialized lake views to use **incremental refresh** —
# processing only the changed data instead of recomputing everything from scratch.
# 
# This is a **one-time setup** per source table. Learn more:
# [Optimal refresh for materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view)

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE bronze.products SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ALTER TABLE bronze.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Create Materialized Lake Views (Silver & Gold Layers)
# 
# Now we define materialized lake views that implement the **medallion architecture**:
# 
# | Layer | View | Purpose |
# |-------|------|---------|
# | **Silver** | `silver.cleaned_order_data` | Joins orders with products, calculates per-order revenue |
# | **Gold** | `gold.product_sales_summary` | Aggregates by product — total quantity, revenue, and average order value |
# 
# Materialized lake views are created with `CREATE MATERIALIZED LAKE VIEW` syntax.
# Fabric persists the results as Delta tables and automatically manages refresh.

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

# MARKDOWN ********************

# ## Step 4: Verify Results
# 
# Query the gold materialized lake view to see the aggregated product sales summary.
# You should see **3 rows** — one for each product — with total quantity sold, total revenue,
# and average order value.
# 
# Refresh the **Lakehouse explorer** to see `cleaned_order_data` and `product_sales_summary`
# under the `silver` and `gold` schemas respectively.

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM gold.product_sales_summary;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Expected Output
# 
# | product_id | product_name | total_quantity_sold | total_revenue | average_order_value |
# |------------|-------------|---------------------|---------------|---------------------|
# | 101 | Laptop | 2 | 2401.00 | 2401.00 |
# | 102 | Smartphone | 3 | 2099.97 | 2099.97 |
# | 103 | Tablet | 1 | 450.00 | 450.00 |

# MARKDOWN ********************

# ## Step 5: Schedule Refresh & Monitor Lineage
# 
# Fabric can automatically keep your materialized lake views up to date as source data changes.
# Follow these steps to set up scheduled refresh:
# 
# ### Set Up Scheduled Refresh
# 1. **Close this notebook** and go back to your lakehouse
# 2. Select **Manage materialized lake views (preview)** from the lakehouse menu
# 3. You should see the auto-generated **lineage graph** showing:
#    - `bronze.products` → `silver.cleaned_order_data` → `gold.product_sales_summary`
#    - `bronze.orders` → `silver.cleaned_order_data` → `gold.product_sales_summary`
# 4. Select **Schedules** from the top ribbon
# 5. In the Schedules pane, select **On** for Schedule refresh
# 6. Choose the desired frequency (by the minute, hourly, daily, weekly, or monthly)
# 7. Specify the recurring interval and select **Apply**
# 
# ### How Automatic Refresh Works
# Once scheduled, Fabric automatically:
# - **Detects changes** in source tables (`bronze.orders`, `bronze.products`)
# - **Determines refresh order** based on the lineage graph (silver before gold)
# - **Chooses the optimal refresh strategy**: incremental (using CDF), full, or skip
# - **Refreshes dependent views** in the correct order — no orchestration needed!
# 
# ### Test the Refresh
# Try inserting new data into a source table, then wait for the next scheduled run:
# ```sql
# INSERT INTO bronze.orders VALUES (1004, 101, 1, '2025-06-04');
# ```
# After the refresh completes, `gold.product_sales_summary` will automatically reflect
# the updated totals for product 101 (Laptop).

# MARKDOWN ********************

# ## What's Next?
# 
# Congratulations! You've built a working **bronze → silver → gold** pipeline with
# materialized lake views and automatic lineage-based refresh.
# 
# ### Learn More
# - [What are materialized lake views?](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view)
# - [Spark SQL reference for materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view)
# - [Refresh behavior (incremental, full, skip)](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view)
# - [Full medallion architecture tutorial](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/tutorial)

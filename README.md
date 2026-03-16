# Materialized Lake Views — Fabric Jumpstart

[![Fabric Jumpstart](https://img.shields.io/badge/Fabric-Jumpstart-blue)](https://github.com/microsoft/fabric-jumpstart)

A **Fabric Jumpstart** source repository containing workspace items for the **Materialized Lake Views Get Started** tutorial. This jumpstart deploys a complete bronze → silver → gold medallion pipeline using materialized lake views in a Microsoft Fabric lakehouse.

## Prerequisites

- A Microsoft Fabric workspace with an enabled capacity
- Lakehouse schemas enabled with Fabric Runtime 1.3

## What Gets Deployed

| Item | Type | Description |
|------|------|-------------|
| `mlv_lakehouse` | Lakehouse | Hosts bronze source tables and silver/gold materialized lake views |
| `mlv_get_started` | Notebook | Self-documenting tutorial — creates tables, enables CDF, defines MLVs, verifies results |

## What You'll Learn

1. **Create source tables** in a `bronze` schema with sample products and orders data
2. **Enable Change Data Feed (CDF)** on source tables for optimal incremental refresh
3. **Create materialized lake views** in `silver` and `gold` schemas using the medallion architecture
4. **Verify results** by querying the gold materialized lake view
5. **Schedule refresh** and explore automatic lineage tracking

## Architecture

```
bronze.products ──┐
                  ├──► silver.cleaned_order_data ──► gold.product_sales_summary
bronze.orders ────┘
```

| Layer | Schema | Object | Description |
|-------|--------|--------|-------------|
| Bronze | `bronze` | `products`, `orders` | Raw source tables with sample data |
| Silver | `silver` | `cleaned_order_data` (MLV) | Joins orders with products, calculates per-order revenue |
| Gold | `gold` | `product_sales_summary` (MLV) | Aggregates by product — total quantity, revenue, avg order value |

## Installation

Install via the [fabric-jumpstart](https://github.com/microsoft/fabric-jumpstart) Python library:

```python
import fabric_jumpstart as jumpstart

jumpstart.install('materialized-lake-views', workspace_id='<your-workspace-guid>')
```

## Related Documentation

- [Get started with materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/get-started-with-materialized-lake-views)
- [What are materialized lake views?](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view)
- [Refresh behavior (incremental, full, skip)](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view)
- [Full medallion architecture tutorial](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/tutorial)

## License

This project is part of the [Fabric Jumpstart](https://github.com/microsoft/fabric-jumpstart) ecosystem.

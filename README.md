
# ApexLabs – NutriWarehouse Analytics Pipeline

## 1. Project Overview

ApexLabs Nutrition has acquired **NutriWarehouse**, a wholesale supplement distributor. This repository implements a **medallion-style analytics pipeline** that:

* Ingests and processes NutriWarehouse data into a **Bronze → Silver → Gold** architecture.
* Aligns NutriWarehouse data with existing **ApexLabs** structures.
* Produces a **shared dimensional model** (star schema) that can be used for unified reporting, dashboards, and advanced analytics across both companies.

The pipeline is designed to be **modular**, **incrementally extensible**, and **analytics-ready** for future use cases like sales forecasting, inventory optimization, and customer segmentation.

---

## 2. Business Context

### ApexLabs (Parent)

* Direct-to-consumer supplement brand.
* Focus: **online sales**, subscription models, customer behaviour, and marketing attribution.
* Primary analytics needs:

  * Revenue and margin tracking by product/channel.
  * Customer lifetime value and churn risk.
  * Campaign performance.

### NutriWarehouse (Acquired Brand)

* B2B **wholesale distributor** of similar supplement products.
* Focus: **bulk orders**, retail partners, and warehouse operations.
* Primary analytics needs:

  * Regional sales performance.
  * Inventory and stock movement.
  * Retail partner performance.

### Integration Goal

Bring NutriWarehouse onto ApexLabs’ analytics platform so that:

* Both brands share **consistent dimensions** (products, dates, channels, regions).
* Facts from each company can be **compared and combined** (e.g. unified sales views, consolidated inventory picture).
* New analytics logic is implemented once and reused across both brands.

---

## 3. High-Level Architecture

The pipeline follows the **Medallion Architecture**:

```text
        Source Systems
   ┌─────────────────────┐
   │ NutriWarehouse ERP  │
   │ ApexLabs D2C / CRM  │
   └─────────┬───────────┘
             │
             ▼
        BRONZE LAYER
   (Raw Ingested Tables)
             │
             ▼
        SILVER LAYER
   (Cleaned & Conformed)
             │
             ▼
         GOLD LAYER
 (Dimensional Model / Marts)
             │
             ▼
      Dashboards & ML
```

Key properties:

* **Bronze**: minimal transformation; schema evolves with source, used for traceability.
* **Silver**: standardized types & formats, resolved joins, basic data quality.
* **Gold**: denormalized **star schemas** and business views ready for BI tools.

---

## 4. Tech Stack

* **Execution Environment:** Databricks / Spark notebooks
* **Compute:** Apache Spark (PySpark + `%sql` notebooks)
* **Storage & Format:** Delta Lake tables (e.g. `FORMAT "delta"`)
* **Catalog / Governance:** Unity Catalog-style structure (e.g. `fmcg.catalog`, `bronze`/`silver`/`gold` schemas)
* **Language:**

  * PySpark (`pyspark.sql.functions as F`) for transformations
  * SQL (`%sql`) for DDL and some transformations
* **Visualization / Reporting:** Dashboards (folder: `dasboard/`) feeding from **Gold** layer

---

## 5. Repository Structure

> Folder names are based on the current repo layout. You can tweak descriptions if you adjust your structure later.

```text
apexlabs-analytics-pipeline/
├─ data/
├─ dasboard/
├─ dimensional_data_processing/
├─ fact_data_processing/
├─ scripts/
├─ setup/
└─ README.md   (this file)
```

### `data/`

* Sample or seed datasets for ApexLabs and NutriWarehouse.
* Typically includes CSVs or parquet files representing:

  * Orders / sales
  * Products
  * Customers / accounts
  * Inventory / stock movements
* Used as input to populate the **Bronze** layer.

### `setup/`

* Environment and catalog initialization notebooks.
* Includes:

  * **`setup_catalog.ipynb`**

    * Creates a dedicated catalog (e.g. `fmcg`).
    * Sets up schemas for medallion layers:

      ```sql
      %sql
      CREATE CATALOG IF NOT EXISTS fmcg;
      USE CATALOG fmcg;

      CREATE SCHEMA IF NOT EXISTS bronze;
      CREATE SCHEMA IF NOT EXISTS silver;
      CREATE SCHEMA IF NOT EXISTS gold;
      ```
    * Ensures both **ApexLabs** and **NutriWarehouse** pipelines share the same catalog + schemas.

### `dimensional_data_processing/`

* Notebooks or scripts that build **dimension tables** in the **Gold** layer.

* Example:

  * **`dim_date_table_creation.ipynb`**

    * Generates a **monthly-grain date dimension** table `fmcg.gold.dim_date` using Spark:

      * `date_key` (e.g. `202401` for January 2024).
      * `month_start_date`.
      * `year`, `month_name`, `month_short_name`.
      * `quarter`, `year_quarter`.
    * Example logic:

      ```python
      from pyspark.sql import functions as F

      start_date = "2024-01-01"
      end_date   = "2025-12-01"

      df = (
          spark.sql(f"""
              SELECT explode(
                  sequence(
                      to_date('{start_date}'),
                      to_date('{end_date}'),
                      interval 1 month
                  )
              ) AS month_start_date
          """)
      )

      df = (
          df
          .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
          .withColumn("year", F.year("month_start_date"))
          .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
          .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
          .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
          .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
      )

      df.write.mode("overwrite").format("delta").saveAsTable("fmcg.gold.dim_date")
      ```

* This folder is where you will also create dimensions like:

  * `dim_product`
  * `dim_customer` / `dim_account`
  * `dim_channel`
  * `dim_region` / `dim_store`

### `fact_data_processing/`

* Transformations that produce **fact tables** for the Gold layer (star schema).
* Typical fact tables for this project may include:

  * `fact_sales` (both ApexLabs and NutriWarehouse sales, with a `source_system` flag).
  * `fact_inventory_snapshot` (on-hand quantities and stock valuation).
  * `fact_shipments` or `fact_orders` for logistics analysis.
* These notebooks:

  * Join cleaned Silver data with the relevant dimensions.
  * Generate **surrogate keys** (via joins to dimension tables like `dim_date`, `dim_product`, etc.).
  * Aggregate to required grains (e.g. **daily per product per channel**, or **monthly per product per region**).

### `scripts/`

* Helper scripts and shared logic.
* Example:

  * **`utilities.ipynb`**

    * Centralizes common configuration values:

      ```python
      bronze_schema = "bronze"
      silver_schema = "silver"
      gold_schema   = "gold"
      ```
    * Keeps schema usage consistent across notebooks.
* Could also include:

  * Reusable transformation functions (e.g. column standardization, deduplication).
  * Small helpers for reading/writing tables, or generating surrogate keys.

### `dasboard/`

* Assets for dashboards consuming the Gold layer.
* May include:

  * SQL queries or notebook snippets used by BI tools.
  * Example views for:

    * Monthly revenue by product and brand.
    * Inventory turnover.
    * Top customers or retailer performance.

---

## 6. Medallion Data Flow

### 6.1 Bronze Layer – Raw Ingest

**Goal:** Land source data with minimal transformation.

* **Location:** `fmcg.bronze.*`

* **Content:**

  * Direct ingests from:

    * NutriWarehouse operational database / exports.
    * ApexLabs D2C / CRM sources.
  * Stored as Delta or other file formats, preserving original structure.

* **Typical steps:**

  * Load CSVs or raw exports from `data/` into Bronze tables with consistent naming, e.g.:

    * `bronze.nutriwarehouse_orders_raw`
    * `bronze.nutriwarehouse_products_raw`
    * `bronze.apexlabs_orders_raw`
    * `bronze.apexlabs_customers_raw`
  * Add ingestion metadata columns:

    * `ingestion_ts`
    * `source_file`
    * `batch_id`

### 6.2 Silver Layer – Cleaned & Conformed

**Goal:** Standardize, validate, and integrate.

* **Location:** `fmcg.silver.*`

* **Characteristics:**

  * Cleaned column names, standardized types, normalized date formats.
  * Removal of duplicates and obviously invalid records.
  * Basic referential checks (e.g. only keep orders with valid product IDs).

* **Examples:**

  * `silver.orders`

    * Unified order table from both ApexLabs and NutriWarehouse with a `source_system` column.
    * Numeric and date types are enforced.
  * `silver.products`

    * Combined product master, mapping NutriWarehouse product codes to ApexLabs internal product IDs where overlaps exist.
  * `silver.customers` / `silver.accounts`

    * Cleaned entity master data.

* **Responsibilities:**

  * **Schema alignment** between ApexLabs and NutriWarehouse.
  * **Business-friendly field naming**.
  * Apply light business rules (e.g. deriving `order_status`, `net_sales`, etc.).

### 6.3 Gold Layer – Dimensional Model

**Goal:** Provide analytics-ready tables for BI and advanced analytics.

* **Location:** `fmcg.gold.*`
* **Pattern:** Star schemas with clear **facts** and **dimensions**.

#### Key Dimensions (Examples)

* `dim_date`

  * Implemented in `dim_date_table_creation.ipynb`.
  * One row per **month start**, with keys and labels used for time-based reporting.
* `dim_product`

  * Consolidated product master (SKUs, brands, categories, flavours, size).
* `dim_customer` / `dim_account`

  * Individual customers (ApexLabs) and corporate accounts (NutriWarehouse).
* `dim_channel`

  * Sales channel (online store, wholesale, marketplace, etc.).
* `dim_region` / `dim_store`

  * Geographic or store-level attributes (country, state, city, store).

#### Key Fact Tables (Examples)

* `fact_sales`

  * Grain: one row per **order line** (or aggregated daily/monthly).
  * Links: `date_key`, `product_key`, `customer_key`, `channel_key`, `region_key`.
  * Measures: `gross_sales`, `discount_amount`, `net_sales`, `quantity`, `cost`, `margin`.
  * Attribute: `source_system` (e.g. `apexlabs` vs `nutriwarehouse`).

* `fact_inventory_snapshot`

  * Grain: **daily / monthly snapshot** per product & warehouse.
  * Measures: `stock_on_hand`, `stock_on_order`, `stock_value`.

* `fact_shipments` (optional)

  * Grain: one row per shipment.
  * Measures: `shipping_cost`, `shipping_time`, `delivered_flag`.

---

## 7. NutriWarehouse Integration Strategy

The integration is handled primarily at **Silver** and **Gold** layers:

1. **Bronze:**

   * Land NutriWarehouse exports separately from existing ApexLabs data.
   * Keep raw structures so that upstream changes are traceable.

2. **Silver:**

   * Map NutriWarehouse concepts (e.g. `warehouse_id`, `customer_type`, `order_status`) into a **common canonical model** shared with ApexLabs.
   * Standardize enums and categorical fields (e.g. channel codes, payment methods).
   * Introduce a `source_system` column enabling:

     * Combined analysis (ApexLabs + NutriWarehouse).
     * Source-specific filtering when needed.

3. **Gold:**

   * Build **shared dimensions** used by both brands (e.g., `dim_date`, `dim_product`, `dim_region`).
   * Load facts from both sources into unified `fact_*` tables with `source_system`.
   * Ensure metrics are computed consistently (same margin formula, same tax rules, etc.).

This design allows:

* **Global reporting:** e.g., “Total sales by product category across both brands”.
* **Brand-specific analysis:** filter by `source_system`.
* Future acquisitions to plug into the same model with minimal refactoring.



## 8. Running the Pipeline

> Below is a generic execution flow; adapt to your actual orchestration (e.g. Databricks Jobs, Airflow, etc.).

### 8.1 Prerequisites

* Databricks workspace or local Spark environment.
* Access to raw data (CSV/Parquet) in the `data/` folder or external storage.
* Cluster with:

  * Delta Lake support.
  * Unity Catalog (if using `CREATE CATALOG`).

### 8.2 Execution Order (Suggested)

1. **Run setup / catalog notebook**

   * `setup/setup_catalog.ipynb`
   * Creates catalog `fmcg` and `bronze`, `silver`, `gold` schemas.

2. **Load Bronze Layer**

   * Notebooks/scripts in `scripts/` (or additional notebooks you create) that:

     * Read from `data/` or external sources.
     * Write to `fmcg.bronze.*` tables.

3. **Build Silver Layer**

   * Notebooks in `scripts/` or `dimensional_data_processing/` that:

     * Clean & standardize Bronze tables.
     * Write to `fmcg.silver.*`.

4. **Build Dimensions**

   * Run:

     * `dimensional_data_processing/dim_date_table_creation.ipynb`
     * Other dimension notebooks (`dim_product`, `dim_customer`, etc.).
   * These write into `fmcg.gold.dim_*`.

5. **Build Facts**

   * Run notebooks in `fact_data_processing/` to:

     * Join Silver tables with dimensions.
     * Aggregate and write into `fmcg.gold.fact_*` tables.

6. **Connect Dashboards**

   * Use the Gold tables as a source for dashboards defined in `dasboard/`.
   * Configure your BI tool (e.g., Power BI, Tableau, Databricks SQL) to connect to `fmcg.gold.*`.



## 9. Data Quality & Governance

To maintain trust in the analytics produced:

* **Bronze:**

  * Capture ingestion metadata (`ingestion_ts`, `source_file`).
  * Preserve raw files for replay and audits.

* **Silver:**

  * Validate critical fields (e.g., non-null IDs, valid dates).
  * Handle duplicates (e.g., use window functions to keep latest record per business key).
  * Enforce consistent types (e.g., cast prices to `DECIMAL`, dates to `DATE`).

* **Gold:**

  * Ensure referential integrity between facts and dimensions.
  * Implement **surrogate keys** for dimensions (e.g., `product_key`, `customer_key`) and use them in fact tables.
  * Use naming conventions and documentation for each table.

Optionally, you can:

* Add **expectations** (e.g. with tools like Great Expectations or Databricks expectations) at Silver/Gold boundaries.
* Schedule regular **data quality checks** and notifications.



## 10. Extensibility & Next Steps

Some natural next steps for this project:

1. **Additional Dimensions**

   * `dim_promotion` for discount campaigns.
   * `dim_supplier` for procurement analysis.

2. **Advanced Analytics**

   * Forecast demand and stock requirements based on `fact_sales` and `fact_inventory`.
   * Customer lifetime value (CLV) modelling for ApexLabs customers.

3. **Operational KPIs**

   * Lead times and SLA tracking from `fact_shipments`.
   * Fill rate and stock-out analysis from `fact_inventory_snapshot`.

4. **Orchestration**

   * Wrap notebook execution in a pipeline:

     * Databricks Workflows / Jobs.
     * Airflow / Prefect pointing to these scripts/notebooks.

5. **Testing**

   * Add unit tests for transformation logic (e.g. PySpark unit tests).
   * Add schema / contract tests between Bronze → Silver → Gold.



## 11. Contributing

1. Create a feature branch:
   `git checkout -b feature/my-change`
2. Make changes (e.g. new dimension, new fact, or new data source).
3. Test transformations locally / in a dev workspace.
4. Submit a pull request with:

   * Summary of changes.
   * Impacted tables / layers.
   * Any new assumptions or business rules.
=

## 12. Summary

This repository implements a **medallion architecture** for integrating NutriWarehouse into the existing ApexLabs analytics platform. By carefully separating **Bronze (raw)**, **Silver (clean/conformed)**, and **Gold (dimensional)** layers, it:

* Keeps ingestion flexible and auditable.
* Standardizes and aligns data from multiple brands.
* Exposes a robust, shared **star schema** that powers dashboards and advanced analytics.

You can adapt and grow this pipeline as NutriWarehouse scales, more brands are acquired, or new analytical use cases emerge.

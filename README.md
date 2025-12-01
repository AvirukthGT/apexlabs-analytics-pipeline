
# ApexLabs – NutriWarehouse Analytics Pipeline
## **1. Project Overview**

ApexLabs Nutrition has recently acquired **NutriWarehouse**, a wholesale supplement distributor operating on a different data stack and business model. This project builds a **unified analytics pipeline** that ingests, cleans, standardizes, and transforms NutriWarehouse data into the existing ApexLabs analytics environment.

The entire solution is built using a **Medallion Architecture (Bronze → Silver → Gold)** to ensure modularity, scalability, and data quality. The pipeline provides:

* **Raw data ingestion** from NutriWarehouse (orders, products, customers, inventory exports).
* **Standardization and conformance** with ApexLabs’ data model.
* **Shared dimension tables** (products, dates, customers/accounts, regions).
* **Unified fact tables** (sales, inventory snapshots, shipments).
* **Analytics-ready Gold views** for dashboards and reporting.

By merging NutriWarehouse’s B2B wholesale operations data with ApexLabs’ D2C brand data, the project enables consistent cross-brand reporting, centralized data governance, and improved business decision-making.

The final outcome is a production-ready analytics foundation that allows ApexLabs to analyze:

* Sales and revenue performance across both brands
* Inventory and warehouse operations
* Customer/account behavior
* Product performance and promotional effectiveness

<img width="5042" height="2852" alt="image" src="https://github.com/user-attachments/assets/a958a3fe-7564-47b7-b440-7e36bdb7dbc6" />

---

# **2. Business Context**

ApexLabs Nutrition operates a direct-to-consumer (D2C) supplement brand, focusing on online sales, customer experience, and subscription-driven revenue. NutriWarehouse, the newly acquired company, operates as a wholesale distributor supplying retail partners and gyms with bulk supplement products.

Because both companies sell similar product lines but operate on different business models (D2C vs. B2B wholesale), their data structures, metrics, and operational workflows are not directly compatible.

The goal of this project is to integrate NutriWarehouse into ApexLabs’ analytics ecosystem by:

* Standardizing the two companies’ data models
* Creating shared dimensions (products, dates, regions, channels)
* Producing unified fact tables for cross-brand reporting

This enables consolidated insights across the ApexLabs group and supports future acquisitions with a repeatable, scalable analytics foundation.

#**3. Repository Structure**

The project follows a clean medallion-oriented layout, with separate areas for setup, data ingestion, dimensional modeling, fact processing, and dashboards.

```text
apexlabs-analytics-pipeline/
│
├── data/
│   ├── ApexLabs/
│   │   ├── Full Load/
│   │   │   ├── customers/
│   │   │   ├── gross price/
│   │   │   ├── orders/
│   │   │   └── products/
│   │   └── Incremental Load/
│   │       └── orders/
│   │           └── <daily order CSVs for December>
│   │
│   ├── NutriHouse/
│   │   ├── Full Load/
│   │   │   ├── customers/
│   │   │   ├── gross price/
│   │   │   ├── orders/
│   │   │   └── products/
│   │   └── Incremental Load/
│   │       └── orders/
│   │           └── <daily order CSVs for December>
│
├── dimensional_data_processing/
│   ├── customer_data_processing.ipynb
│   ├── pricing_data_processing.ipynb
│   └── products_data_processing.ipynb
│
├── fact_data_processing/
│   ├── full_load_fact.ipynb
│   └── incremental_load_fact.ipynb
│
├── scripts/
│   ├── denormalise_table_query_fmcg.sql
│   └── incremental_data_parent_company_query.sql
│
├── setup/
│   ├── catalog_setup.ipynb
│   ├── dim_date_table_creation.ipynb
│   └── utilities.ipynb
│
├── dashboard/
│   └── fmcg_dashboard.pdf
│
└── README.md
```

### **`data/`**

Raw data used for Bronze ingestion.

* **ApexLabs** and **NutriHouse** separated.
* Each has:

  * **Full Load** → customers, products, orders, gross price
  * **Incremental Load** → daily order extracts for December
* Enables simulation of real-world batch + incremental streaming ingestion.


### **`dimensional_data_processing/`**

Scripts that build **Gold-layer dimensions**:

* `customer_data_processing.ipynb`
  Clean + conform customer/account data.
* `pricing_data_processing.ipynb`
  Standardizes gross price structures and pricing attributes.
* `products_data_processing.ipynb`
  Conforms product catalogs from both companies.

These output Gold dimensional tables such as `dim_customer`, `dim_product`, etc.


### **`fact_data_processing/`**

Logic for building **fact tables**:

* `full_load_fact.ipynb`
  Builds fact tables using the complete dataset.
* `incremental_load_fact.ipynb`
  Builds fact tables using daily incremental loads for December.

Outputs include fact tables like:

* `fact_gross_price_apexlabs`
* `fact_gross_price_nutrihouse`
* Unified fact tables (if unioned in Gold)


### **`scripts/`**

Reusable SQL transformation logic:

* `denormalise_table_query_fmcg.sql`
  Used for flattening / denormalizing Silver data.
* `incremental_data_parent_company_query.sql`
  Logic to load incremental orders for ApexLabs.


### **`setup/`**

Environment initialization and core dimensions:

* `catalog_setup.ipynb`
  Creates catalog + medallion schemas (`bronze`, `silver`, `gold`).
* `dim_date_table_creation.ipynb`
  Generates the Gold `dim_date` table.
* `utilities.ipynb`
  Defines schema constants used across notebooks.


### **`dashboard/`**

Visualization outputs and reporting:

* `fmcg_dashboard.pdf`
  A PDF dashboard built from Gold-layer fact + dimension tables.

---

# **4. Setup**

This section describes all the initialization steps required before running the Bronze, Silver, and Gold data pipelines. The setup executes three main tasks:

1. **Create the catalog and medallion schemas**
2. **Initialize helper variables used across notebooks/scripts**
3. **Build the foundational `dim_date` table in the Gold layer**

## **4.1 Catalog & Schema Initialization**

File: `catalog_setup.py` 

This script prepares the Databricks environment by creating a dedicated catalog (`fmcg`) and the three medallion schemas (`bronze`, `silver`, `gold`).

```sql
-- Create catalog if it does not exist
CREATE CATALOG IF NOT EXISTS fmcg;
USE CATALOG fmcg;

-- Create medallion schemas
CREATE SCHEMA IF NOT EXISTS fmcg.gold;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
```

### **What this achieves**

* Ensures all tables for ApexLabs + NutriWarehouse live under **one governed catalog**.
* Ensures consistent table placement across:

  * `fmcg.bronze.*` (raw ingested data)
  * `fmcg.silver.*` (clean/conformed data)
  * `fmcg.gold.*` (dimensional model / fact tables)

This step **must be run before any other pipeline script**.

## **4.2 Utility Schema Variables**

File: `utilities.py` 

This small utility script exposes standard schema names to ensure consistent usage across notebooks and pipelines:

```python
bronze_schema="bronze"
silver_schema="silver"
gold_schema="gold"
```

These variables are imported by downstream notebooks to avoid hard-coding schema names and to keep schema references centralized.

### **Purpose**

* Prevents accidental schema mismatches.
* Keeps medallion layer references clean and consistent across the repo.
* Makes future changes (e.g., renaming schemas) much easier.

## **4.3 Gold Layer Setup — Date Dimension Creation**

File: `dim_date_table_creation.py` 

This script generates the **Month-Level Date Dimension** used by both ApexLabs and NutriWarehouse fact tables in the Gold layer.

### **Steps performed**

#### **1. Define date boundaries**

```python
start_date = "2024-01-01"
end_date   = "2025-12-01"
```

#### **2. Generate one row per month between start and end dates**

Uses Spark SQL `sequence()` + `explode()` to generate month start dates:

```python
df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 month
        )
    ) AS month_start_date
""")
```

#### **3. Add analytics-friendly columns**

* `date_key` (YYYYMM integer)
* `year`
* `month_name` (e.g., “January”)
* `month_short_name` (e.g., “Jan”)
* `quarter` (Q1, Q2, etc.)
* `year_quarter` (e.g., 2024-Q1)

```python
df = (
    df.withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
      .withColumn("year", F.year("month_start_date"))
      .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
      .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
      .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
      .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
)
```

#### **4. Save as a Gold table**

```python
df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("fmcg.gold.dim_date")
```

### **Purpose of `dim_date`**

* Acts as a **shared dimension** for both brands
* Enables time-based analysis for Gross Price facts
* Ensures consistent reporting across dashboards
* Provides surrogate month-level `date_key`, which simplifies fact table joins



## **4.4 Summary of Setup Flow**

| Step | Script                       | Purpose                                               |
| ---- | ---------------------------- | ----------------------------------------------------- |
| 1    | `catalog_setup.py`           | Creates `fmcg` catalog and Bronze/Silver/Gold schemas |
| 2    | `utilities.py`               | Standardizes schema variable references               |
| 3    | `dim_date_table_creation.py` | Builds foundational Gold dimension table `dim_date`   |









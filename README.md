#  Sales Management Data Warehouse (MySQL Data Engineering Project)

## Project Overview
This project implements a complete end-to-end Data Engineering pipeline using MySQL to simulate a real-world sales management data warehouse environment.

The goal is to build a scalable, structured system for data ingestion, transformation, storage, and analytics across three architectural layers — Raw, Staging, and Data Warehouse.

---

## Objectives
- Design a data warehouse using the Star Schema model.
- Implement ETL pipelines (Extract → Transform → Load) using MySQL stored procedures.
- Automate data quality checks, transformation logic, and logging.
- Generate analytical insights from integrated fact and dimension tables.
- Showcase practical Data Engineering workflow from ingestion to analytics.

---

## Data Architecture

The data architecture for this project follows three Architectural Layers **Raw**, **Staging**, and **Modelling** layers
![Data Architecture](docs/data_architecture.png)

1. **Raw Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Staging Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Modelling Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
### Layer 1: Raw Layer (Source Ingestion)**
- Stores the unprocessed CSV files exactly as received from source systems.
- Each table mirrors the structure of the source dataset.
- Batch-loaded using `LOAD DATA LOCAL INFILE`.
- **Loading Type:** Full Load (Truncate & Insert).

**Schema Name:** [raw_sales_management](scripts/raw/)

**Tables:**

`brands`, `categories`, `customers`, `order_items`, `orders`, `products`, `staffs`, `stocks`, `stores`

**Key Features:**
- 1:1 mirror of source files  
- Includes load logging via `load_log` table  
- Load type: Batch Truncate + Insert

**Logging Table:**

| Column      | Description                |
| ----------- | -------------------------- |
| table_name_ | Target table name          |
| load_status | SUCCESS / FAILED           |
| message     | Error or success message   |
| batch_tag   | Timestamp batch identifier |

---

### **Layer 2: Staging Layer (Transformation & Standardization)**
- Cleans and standardizes raw data.
- Applies transformations like:
  - Null value removal
  - Data standardization
  - Derived column creation
  - Data enrichment
- **Loading Type:** Full Load (Truncate & Insert) using stored procedure

**Schema Name:** [stg_sales_management](scripts/staging/)

## ⚙️ ETL Pipeline (Stored Procedure)

A single stored procedure automates ETL flow:

```sql
CALL stg_load_stp();
````

**Key Transformations:**
| Transformation | Description |
|----------------|-------------|
| Data cleaning | Removing duplicates, fixing nulls, trimming spaces |
| Derived column | Added `sales` column where applicable |
| Standardization | Ensured consistent naming for cities, states, and stores |
| Data validation | Used QA queries to verify record counts and duplicates |

**Data Quality Checks: [Staging Quality Check](tests/stg_quality_check.sql)**
- Null or duplicate primary keys (for composite keys, checks both columns together).
- Compare raw table records counts with staging table records counts
- Allows up to 5% variance (marked as WARNING if exceeded)
---

### **Layer 3: Data Warehouse Layer (Integration & Analytics)**
- Contains **integrated, historical, analytical** data in Star Schema format.
- Combines facts and dimensions for performance reporting and analytics.

**Schema Name:** [dw_sales_management](scripts/modelling/)

**Data modell:**
![Data Model](docs/data_model.png)


**Star Schema Tables:**
#### 🧭 Dimensions:
```

dim_customers
dim_products
dim_stores
dim_staffs
dim_dates

```

#### 📊 Facts:
```

fact_sales
fact_inventory

````

**Derived Columns:**
| Table | Derived Field | Formula |
|--------|----------------|----------|
| fact_sales     | `total_amount`    |
| fact_inventory | `last_updated`    |

---

## ⚙️ ETL Pipeline (Stored Procedure)

A single stored procedure automates ETL flow:

```sql
CALL dw_load_stp();
````

### **Process Flow:**

1. Truncate all warehouse tables.
2. Insert transformed data from staging.
3. Create derived columns.
4. Log load success/failure in `raw_sales_management.load_log`.

---

## Data Quality checks: [Modelling Quality Check](tests/dw_quality_check.sql)
- Uniqueness of surrogate keys in dimension tables.
- Referential integrity between fact and dimension tables.
- Validation of relationships in the data model for analytical purposes.

---

## Analytics & Views

| View Name                    | Description                               |
| ---------------------------- | ----------------------------------------- |
| `vw_sales_summary`           | Total amount by store, staff, and month   |
| `vw_inventory_summary`       | Current inventory level per store/product |
| `vw_sales_by_store `         |  Total sales by store                     |

**View Query:** [Analytics](scripts/analytics/)

---

## Tools Used

| Category                 | Tools / Concepts                              |
| ------------------------ | --------------------------------------------- |
| Data Architecture        | drawio                                        |
| Database                 | MySQL                                         |
| Modeling                 | Star Schema, Dimensional Modeling             |
| ETL                      | Stored Procedures, SQL Joins, Derived Columns |
| Logging                  | MySQL Diagnostic & Error Handling             |
| Data Quality             | Validation Queries, Row Counts, Null Checks   |

---

## Folder Structure

```
/sales_data_warehouse_project
│
├── /datasets/data_source    # Original CSV files
|
├── /docs                    # System diagram
|   ├── data_architecture.png
|   ├── data_flow.png
|   ├── data_model.pdf
|      
├── /scripts/raw
│   ├── raw_ddl.sql            # Raw layer DDL
|   ├── load_raw.sql           # Raw layer load
├── /scripts/staging
│   ├── ddl_staging.sql         # Staging layer DDL
|   ├── load_stg.sql            # Staging layer load
├── /scripts/modelling
│   ├── ddl_dw.sql              # DW layer DDL
|   ├── load_dw.sql             # DW layer load
|
├── /scripts/analytics
|   ├── data_analytics.sql      # Raw layer load
│
├── /tests
│   ├── dw_quality_check.sql    # DW quality check
│   ├── stg_quality_check.sql   # Staging quality check
│
└── README.md
```

---

## How to Run

1. Import all source CSV files into `raw_sales_management` schema.
2. Execute the **Raw → Staging ETL** procedure.
  
   ```sql
   CALL stg_load_stp();
   ```
4. Run the **DW ETL procedure**:

   ```sql
   CALL dw_load_stp();
   ```
5. Verify `load_log` for successful loads.
6. Execute analytical views for insights.

---


## Key Learning Outcomes

* Practical hands-on ETL pipeline building in MySQL.
* End-to-end understanding of Data Warehouse architecture.
* Mastery of data cleaning, transformation, and analytics SQL.
* Real-world project experience in data engineering workflow.


Would you like me to also include a **GitHub-friendly architecture diagram description (ASCII format)** or a **Power BI visualization dashboard section** at the end of the README for your portfolio version?
```

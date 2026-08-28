
# DuckLake - Starlake Starflow TPC-H Data Pipeline Example

This project demonstrates an end-to-end data pipeline for the [TPC-H](https://www.tpc.org/tpch/) benchmark using [Starlake](https://github.com/starlake-ai/starlake). It showcases data ingestion, transformation, and analytics on industry-standard datasets, with a focus on reproducibility and modularity.

## Project Overview

DuckLake ingests and processes TPC-H benchmark data, a standard for decision support and analytics. The project is organized to follow best practices for data engineering with Starflow:

- **Data Ingestion**: Raw TPC-H data (CSV) is loaded from the `datasets/` directory.
- **Data Processing**: Starflow YAML metadata in `metadata/` defines schemas, types, and data quality rules.
- **Data Analytics**: SQL transformations in `metadata/transform/` generate business insights and KPIs.

## Project Structure

```
├── datasets/              # Data directories for all pipeline stages
│   ├── archive/           # Archived source data (e.g., TPC-H CSVs)
│   ├── data_files/        # Partitioned and processed data files
│   ├── incoming/          # Dropzone for new data
│   ├── stage/             # Intermediate processing
│   └── unresolved/        # Files with issues
│
├── metadata/              # Starflow configuration and pipeline logic
│   ├── application.sl.yml # Main Starflow application config
│   ├── dags/              # Orchestration workflows (Airflow, Dagster, Snowflake)
│   ├── expectations/      # Data quality templates
│   ├── load/tpch/         # Table schemas for TPC-H (YAML)
│   ├── transform/kpi/     # SQL transformations for analytics
│   └── types/             # Data type definitions
│
├── sample-data/           # Example data for testing (e.g., starbake/)
└── README.md
```

## TPC-H Tables

The following TPC-H tables are defined in `metadata/load/tpch/` and ingested from CSV files in `datasets/archive/tpch/`:

- **CUSTOMER**: Customer information (C_CUSTKEY, C_NAME, C_ADDRESS, ...)
- **ORDERS**: Orders placed (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, ...)
- **LINEITEM**: Line items for each order (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, ...)
- **PART**: Product catalog (P_PARTKEY, P_NAME, P_MFGR, ...)
- **PARTSUPP**: Supplier-part relationships (PS_PARTKEY, PS_SUPPKEY, ...)
- **SUPPLIER**: Supplier information (S_SUPPKEY, S_NAME, ...)
- **NATION**: Nation reference data (N_NATIONKEY, N_NAME, ...)
- **REGION**: Region reference data (R_REGIONKEY, R_NAME, ...)

Each table's schema, types, and sample values are defined in the corresponding YAML file under `metadata/load/tpch/`.

## Transformations & Analytics

SQL transformations in `metadata/transform/kpi/` implement TPC-H business queries and KPIs, such as:

- Pricing summary
- Top suppliers
- Revenue and profit analysis
- Shipping and order priority queries

These queries can be run as part of the pipeline or independently for analytics.

## Orchestration

Workflows for Airflow, Dagster, and Snowflake are provided in `metadata/dags/` for flexible orchestration and scheduling.

## How to Run

1. Place TPC-H CSV data files in `datasets/archive/tpch/` (see [TPC-H data generator](https://www.tpc.org/tpch/)).
2. Configure your environment in `metadata/application.sl.yml` and `metadata/types/` as needed.
3. Use Starflow CLI or your orchestrator to launch the pipeline (see workflow YAMLs in `metadata/dags/`).
4. Processed data will appear in `datasets/data_files/tpch/`.

## Development Guide

- All configuration files use the `.sl.yml` extension.
- Table schemas and types are in `metadata/load/tpch/` and `metadata/types/`.
- Transformations are SQL files in `metadata/transform/kpi/`.
- Data quality rules are in `metadata/expectations/`.
- Sample data for quick tests is in `sample-data/`.

## References

- [Starlake Documentation](https://github.com/starlake-ai/starlake)
- [TPC-H Benchmark](https://www.tpc.org/tpch/)

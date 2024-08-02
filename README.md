# F1-Racing-Analysis-Using-Data-Bricks
## Overview
This project focuses on analyzing Formula 1 data using Azure Databricks, Delta Lake, and Azure Data Factory. It involves building an ETL pipeline, transforming data for BI reporting, and creating visualizations to uncover insights into driver and team performance.

## Project Requirements
Data Ingestion: Ingest all F1 data files into Delta Lake with schema enforcement, audit columns, and incremental loading. Data is stored in Parquet format.

Data Transformation: Combine key datasets (e.g., races and results) to create a unified dataset for reporting. Transformation logic supports incremental loads.

BI Reporting: Generate reports on driver and constructor standings for the current race year and historically from 1950 onwards.

Analytical Insights: Rank and visualize dominant drivers and teams, providing insights into their performance.

## Technologies Used
Azure Databricks: For building and managing the ETL pipeline.

Delta Lake: To handle data storage with schema enforcement, audit logs, and time-based querying.

Azure Data Factory (ADF): For orchestrating and scheduling the ETL pipelines.

PySpark: To perform data transformations and analysis.

SQL: For querying and aggregating data.

Python: For custom scripts and data processing.

## Implementation
### ETL Pipeline:

Set up Azure Databricks to ingest F1 data files into Delta Lake.

Applied schema enforcement and audit columns.

Implemented incremental data loading logic to append new race data without overwriting existing data.

### Data Transformation:

Used PySpark and SQL to join key tables (e.g., races and results) and create comprehensive datasets.

Enabled BI reporting capabilities for driver and constructor standings.

### BI Reporting:

Developed visualizations and dashboards to track driver and team performance.

Produced reports for the current race year and historical data from 1950.

### Non-Functional Requirements:

Scheduled pipelines to run every Sunday at 10 PM using Azure Data Factory.

Implemented monitoring, alerting, and the ability to rerun failed pipelines.

Ensured GDPR compliance with Delta Lake’s data history and deletion features.


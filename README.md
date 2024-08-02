# F1-Racing-Analysis-Using-Data-Bricks
## Overview
This project focuses on analyzing Formula 1 data using Azure Databricks, Delta Lake, and Azure Data Factory. It involves building an ETL pipeline, transforming data for BI reporting, and creating visualizations to uncover insights into driver and team performance.
## Project Requirements
Data Ingestion: Ingest all F1 data files into Delta Lake with schema enforcement, audit columns, and incremental loading. Data is stored in Parquet format.
Data Transformation: Combine key datasets (e.g., races and results) to create a unified dataset for reporting. Transformation logic supports incremental loads.
BI Reporting: Generate reports on driver and constructor standings for the current race year and historically from 1950 onwards.
Analytical Insights: Rank and visualize dominant drivers and teams, providing insights into their performance.

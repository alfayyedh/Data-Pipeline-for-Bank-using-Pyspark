# Building an ETL Pipeline with PySpark

## Introduction

Every project tells a story — this is mine.
In this project, I built an end-to-end ETL (Extract, Transform, Load) pipeline using PySpark.
The mission: turn scattered, messy banking data into structured, usable insights — just like in a real-world data engineering setup.

## The Problem

When I first looked at the data, I noticed three big challenges:

- The data lived in multiple places — a database and a massive CSV file.
- It was too large for simple scripts — it needed distributed processing.
- It was raw and messy, not ready for any meaningful analysis.

I needed a pipeline that could handle all of this automatically, reliably, and efficiently.

## The Data

There were two main data sources:
- A database containing tables about customer demographics and a direct marketing campaign (education_status, marital_status, and marketing_campaign_deposit).
- A CSV file with over 800,000 customer records — transaction histories, balances, and more.

## My Approach

Before touching any code, I planned the solution:

- Use PySpark to scale up the processing.
- Set up Docker containers to simulate the databases (source, warehouse, and a logging database).
- Create a modular ETL pipeline to keep everything clean and organized.

## Building the Pipeline
1. Environment Setup
   - Installed and configured PySpark inside Jupyter Notebook.
   - Spun up three PostgreSQL databases via Docker Compose — one for source data, one for the warehouse, and one for ETL logs.

2. Extraction
   - Built PySpark functions to pull data from both the database and the CSV file.
   - Focused on making the extraction flexible and reusable.

3. Source-to-Target Mapping
   - Defined transformation rules with clarity (based on earlier "business discussions" — simulated here).

4. Transformation
   - Applied mappings to clean and reshape the data.\
   - Used PySpark DataFrame operations to handle complex transformations.

5. Loading
   - Inserted the cleaned data into the data warehouse.
   - Ensured idempotent loads (no duplicates when re-running).

6. Logging and Monitoring
   - Every step logs into a dedicated logging database, making the ETL process traceable.
  
## Project Structure
```
/helper_functions
/extract
/profile
/transform
/load
main.py
```
Each module plays its part in the pipeline, and main.py ties it all together.

## Running The Pipeline
Running it is simple:
1. Set up the environment.
2. Launch Docker containers for the databases.
3. Run the pipeline:
```
python main.py
```
5. Watch the logs and monitor the results!







  

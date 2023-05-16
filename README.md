

<h1 align="center">airflow_etl_pipeline_project</h1>

<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png" alt="Airflow Logo" width="400" height="150">
</p>

<p align="center">
  A Data Pipeline for Extracting, Transforming, and Loading Data into a Snowflake Data Warehouse using Airflow
</p>


## Overview

This repository provides an Airflow DAG (Directed Acyclic Graph) that orchestrates the process of extracting data from a PostgreSQL database, transforming it, and loading it into a Snowflake data warehouse. The DAG performs the following steps:

1. Extract and Load Data from PostgreSQL:
   - The `task_emp_sal` operator extracts data from the `finance.emp_sal` table in the PostgreSQL database and loads it into an AWS S3 bucket as a CSV file.
   - The `task_emp_det` operator extracts data from the `hr.emp_details` table in the PostgreSQL database and loads it into the same S3 bucket as a CSV file.

2. Data Transformation and Joining:
   - The `data` task reads the employee data from the CSV files in the S3 bucket.
   - It performs any necessary transformations on the data and joins the `emp_sal` and `emp_details` tables based on the `emp_id` column, creating a combined dataset.

3. Joining with Employee Dimension and Update:
   - The combined dataset from the previous step is joined with the `emp_dim` table in the Snowflake data warehouse, which represents a slowly changing dimension type 2.
   - The task identifies if the salary of an employee has changed by comparing the salary values between the combined dataset and the `emp_dim` table.
   - If a salary change is detected, the task updates the `effective_end_date` and `active` columns for the existing row in the `emp_dim` table, and inserts a new row for the same employee with the updated salary.


## Prerequisites

- Airflow 
- Python 
- PostgreSQL 
- Snowflake 
- Pandas 
- AWS S3

## Installation

1- Put the `emp_dim_insert_update.py` and `queries.py` in this path `/opt/airflow/includes`  

2- Put the `etl_dag.py`  in this path `/opt/airflow/dags` 

3. Access the Airflow web interface.

4. Trigger the `join_INSERT` DAG manually or set up a schedule based on your requirements.

5. Monitor the progress and status of the DAG execution through the Airflow UI.



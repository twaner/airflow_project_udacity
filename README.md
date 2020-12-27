README2.md
# Udacity Airflow Project

## Purpose
A music streaming startup, Sparkify, has grown its user base and is looking to improve upon their data analysis process. their data currently gets stored in S3 in JSON files. They require a more sophisticated approach to use their data. 

To do this, they're looking to leverage Airflow to automate an ETL process to move the data into Redshift tables from files stored on s3 buckets.

## Files
1. dags/airflow_dag.py - This file contains the dag, dag config and operators.
2. plugins/helpers/sql_queries.py - A class that contains all necessary SQL queries.
3. plugins/operators - This directory contains the custtom Airflow Operators.

## Settings
1. verbose_logging - Turns on info level logging.
2. Create - Runs the Create SQL
3. Delete - Runs the Delete SQL
4. Append - Runs the Insert SQL
5. Region - Region for s3 buckets, if different from Redshift's.
6. Schema - The Redshift Schema name.
7. redshift_conn_id - The key for the Airflow config setting.
8. aws_credentials_id - The key for the Airflow config setting.

##Dag info
The dag is configured to run hourly. On failure it will retry 5 times, on a five minute delay. There will be no email notice on retries.

## Steps to Run
1. Start Airflow.
2. In the Airflow UI, setup the credentials for s3 and Redshift.
3. Once that is complete, turn on the Dag and start a Dag run or wait for the next scheduled run.
4. Inspect the Dag run and view logs to verify results. Turn on verbose logging for additional information.

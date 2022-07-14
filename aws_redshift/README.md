# Amazon Redshift
This is the repository for the SQL scripts to be executed on the Redshift instance before running the one time data import dag defined in Airflow.

The scripts should be executed in the following order:
1. 001_create_schema.sql: create the schema for different tables
2. 002_create_staging_tables.sql: create staging tables for storing transformed raw data
3. 003_create_dimension_tables.sql: create dimension tables that store data for some attributes of fact data
4. 004_create_fact_tables.sql: create fact tables for fact data
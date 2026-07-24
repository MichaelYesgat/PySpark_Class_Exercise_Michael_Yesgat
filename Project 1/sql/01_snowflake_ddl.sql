-- Project 1 - Snowflake database, schemas, S3 stage, and curated tables
-- Run this file first.

CREATE DATABASE IF NOT EXISTS RETAIL_PIPELINE_DB;
USE DATABASE RETAIL_PIPELINE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS DQ;


-- PARSE_HEADER allows COPY INTO to match the Spark CSV header names to the
-- Snowflake table columns by name.
CREATE OR REPLACE FILE FORMAT RAW.CSV_WITH_HEADER_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'N/A', 'NA', '', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;


-- PySpark writes the Snowflake-ready CSV folders beneath this S3 location:
-- s3://yesgat-020529563157-us-east-2-an/snowflake_stage/
--
-- RETAIL_S3_INTEGRATION must be an existing Snowflake storage integration
-- with permission to read this S3 path. Do not place AWS access keys in SQL.
-- for security i removed the link for my s3 bucket
CREATE OR REPLACE STAGE RAW.RETAIL_CURATED_S3_STAGE
    URL = 's3://###################-us-east-2-an/snowflake_stage/'
    STORAGE_INTEGRATION = RETAIL_S3_INTEGRATION
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_WITH_HEADER_FORMAT');


-- Matches customers_df_clean from project_1_completed.py.
CREATE OR REPLACE TABLE CURATED.DIM_CUSTOMERS (
    customer_id NUMBER(38,0) NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    signup_date DATE,
    country STRING,
    state STRING,
    postal_code STRING,
    is_active BOOLEAN,
    loyalty_points NUMBER(38,0),
    CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id)
);


-- Matches products_df_clean from project_1_completed.py.
CREATE OR REPLACE TABLE CURATED.DIM_PRODUCTS (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    brand STRING,
    price NUMBER(12,2),
    cost NUMBER(12,2),
    stock_quantity NUMBER(38,0),
    weight_kg NUMBER(8,3),
    created_date DATE,
    is_active BOOLEAN,
    CONSTRAINT pk_dim_products PRIMARY KEY (product_id)
);


-- Matches orders_df_clean from project_1_completed.py.
CREATE OR REPLACE TABLE CURATED.FACT_ORDERS (
    order_id NUMBER(38,0) NOT NULL,
    customer_id NUMBER(38,0) NOT NULL,
    product_id STRING NOT NULL,
    order_date DATE,
    ship_date DATE,
    quantity NUMBER(38,0),
    unit_price NUMBER(12,2),
    discount_pct NUMBER(5,2),
    source_total_amount NUMBER(12,2),
    calculated_total_amount NUMBER(14,2),
    amount_difference NUMBER(14,2),
    payment_method STRING,
    order_status STRING,
    CONSTRAINT pk_fact_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_id)
        REFERENCES CURATED.DIM_CUSTOMERS(customer_id),
    CONSTRAINT fk_fact_product
        FOREIGN KEY (product_id)
        REFERENCES CURATED.DIM_PRODUCTS(product_id)
);


-- Matches data_quality_summary_df from project_1_completed.py.
CREATE OR REPLACE TABLE DQ.DQ_SUMMARY (
    dataset STRING,
    source_record_count NUMBER(38,0),
    target_record_count NUMBER(38,0),
    removed_record_count NUMBER(38,0)
);

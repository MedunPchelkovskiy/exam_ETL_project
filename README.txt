ETL project

The goal of the project is to create complete ETL process.
The aim is to get files from cloud provider, in this project we use AWS S3 buckets, do some cleaning, transformations and enrichment.
Then, according to business requirements, we perform certain analytical tasks. The results of which, loaded on cloud,
in this case we using Snowflake, provide insights and enable us to make decisions.


Configuration

    Create project folder. After that open terminal and navigate to this folder.
    Go to https://www.astronomer.io/docs/astro/cli/install-cli/?tab=linux#install-the-astro-cli , choose your host operating system and
     follow the steps to install the Astro CLI.
    Go to your preferred IDE(pycharm, VS Code, etc.) and open project folder with existing astro structure.

    Prerequisites

    Python 3.12
    Docker desktop 4.43.2 (This is my version used during project development)

    Create a virtual environment (recommended):

        python3 -m venv venv
        source venv/bin/activate  # On Windows use `venv\Scripts\activate

    Install the dependencies:

        All needed packages and dependencies are provided in requirements.txt file.
        You can install it with following command:

        pip install -r requirements.txt


    Run the project

    astro dev start

        Access the application: Open a web browser and go to http://127.0.0.1:8080.

Database Setup

    Configuration needed for store data in Snowflake, after each task, is provided in config.yaml file.
    In Airflow you must go to connections, setup for this case AWS and Snowflake.
    Below you will find the necessary script required for Snowflake.


    CREATE WAREHOUSE retail_etl_project_wh
    WITH
        WAREHOUSE_SIZE = "XSMALL"
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE;



CREATE DATABASE retail_etl_project_db;

CREATE ROLE etl_dev;


CREATE SCHEMA RETAIL_ETL_PROJECT_DB.CLEANED_LAYER;
CREATE SCHEMA RETAIL_ETL_PROJECT_DB.BUSINESS_LAYER;
CREATE SCHEMA RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER;

GRANT USAGE ON DATABASE RETAIL_ETL_PROJECT_DB  TO ROLE etl_dev;
GRANT USAGE ON WAREHOUSE RETAIL_ETL_PROJECT TO ROLE etl_dev;
GRANT USAGE ON SCHEMA RETAIL_ETL_PROJECT_DB.CLEANED_LAYER      TO ROLE etl_dev;
GRANT USAGE ON SCHEMA RETAIL_ETL_PROJECT_DB.BUSINESS_LAYER     TO ROLE etl_dev;
GRANT USAGE ON SCHEMA RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER TO ROLE etl_dev;

GRANT INSERT, SELECT
    ON ALL TABLES IN SCHEMA RETAIL_ETL_PROJECT_DB.CLEANED_LAYER TO ROLE etl_dev;
GRANT INSERT, SELECT
    ON ALL TABLES IN SCHEMA RETAIL_ETL_PROJECT_DB.BUSINESS_LAYER TO ROLE etl_dev;
GRANT INSERT, SELECT
    ON ALL TABLES IN SCHEMA RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER TO ROLE etl_dev;


CREATE TABLE RETAIL_ETL_PROJECT_DB.CLEANED_LAYER.SALES_DATA (
    sales_id     INTEGER,
    product_id   INTEGER,
    region       VARCHAR,
    quantity     INTEGER,
    price        DOUBLE,
    time_stamp   DATETIME,
    discount     DOUBLE,
    order_status VARCHAR,
    total_sales  DOUBLE
);


CREATE TABLE RETAIL_ETL_PROJECT_DB.CLEANED_LAYER.PRODUCTS_DATA (
    product_id  INTEGER,
    category    VARCHAR,
    brand       VARCHAR,
    rating      DOUBLE,
    in_stock    BOOLEAN,
    launch_date DATETIME
);


CREATE TABLE RETAIL_ETL_PROJECT_DB.CLEANED_LAYER.MERGED_DATA (
    sales_id     INTEGER,
    product_id   INTEGER,
    region       VARCHAR,
    quantity     INTEGER,
    price        DOUBLE,
    time_stamp   DATETIME,
    discount     DOUBLE,
    order_status VARCHAR,
    total_sales  DOUBLE,
    category     VARCHAR,
    brand        VARCHAR,
    rating       DOUBLE,
    in_stock     BOOLEAN,
    launch_date  DATETIME
);


CREATE TABLE RETAIL_ETL_PROJECT_DB.BUSINESS_LAYER.ENRICHED_DATA (
    sales_id     INTEGER,
    product_id   INTEGER,
    region       VARCHAR,
    quantity     INTEGER,
    price        DOUBLE,
    time_stamp   DATETIME,
    discount     DOUBLE,
    order_status VARCHAR,
    total_sales  DOUBLE,
    category     VARCHAR,
    brand        VARCHAR,
    rating       DOUBLE,
    in_stock     BOOLEAN,
    launch_date  DATETIME,
    month        VARCHAR,
    weekday      VARCHAR,
    hour         INTEGER,
    sales_bucket VARCHAR
);



CREATE OR REPLACE TABLE RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER.SALES_TRENDS(
    quarter VARCHAR,
    category VARCHAR,
    total_sales DOUBLE
)  ;


CREATE OR REPLACE TABLE RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER.SALES_RANKING(
    region VARCHAR,
    total_sales DOUBLE,
    revenue_share DOUBLE,
    cumulative_revenue_share DOUBLE
) ;


CREATE OR REPLACE TABLE RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER.sales_seasonality(
    category VARCHAR,
    month VARCHAR,
    monthly_total_sales DOUBLE,
    monthly_total_quantity INTEGER
) ;



CREATE OR REPLACE TABLE RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER.sales_status(
    status VARCHAR,
    pending INTEGER,
    shipped INTEGER,
    returned INTEGER
) ;


CREATE OR REPLACE TABLE RETAIL_ETL_PROJECT_DB.PRESENTATION_LAYER.average_sales_and_units(
    sales_bucket VARCHAR,
    average_sales INTEGER,
    average_quantity DOUBLE
) ;


GRANT ROLE etl_dev TO USER <YOUR SNOWFLAKE USERNAME>;

USE ROLE etl_dev;




SELECT * FROM sales_data;

SELECT * FROM enriched_data;

SELECT * FROM SALES_RANKING;

SELECT * FROM SALES_TRENDS;

SELECT * FROM SALES_SEASONALITY;

SELECT * FROM SALES_STATUS;

SELECT * FROM average_sales_and_units;







import pandas as pd
import yaml
from airflow.decorators import dag, task, task_group
from airflow.sdk.bases.operator import AirflowException

from include.extract_s3_data import try_to_extract
from include.load import data_loading_in_snowflake
from include.transform import sales_data_transformation, products_data_transformation, \
    merging_sales_data_with_products_data, merged_data_enriched, quarterly_sales_by_category, \
    sales_revenue_by_region, sales_seasonality, weekly_order_counts_by_status, average_sales_and_units_by_sales_bucket

with open("include/config.yaml") as config_file:
    config = yaml.safe_load(config_file)


@dag()
def etl_pipeline():
    @task_group(group_id="extract_group")
    def extract_group():
        """Extracting files from AWS bucket."""

        @task()
        def extract_json_files(bucket, folder, aws_conn_id, file_ext="json"):
            return try_to_extract(bucket=bucket, folder=folder, aws_conn_id=aws_conn_id, file_ext=file_ext)

        @task()
        def extract_csv_files(bucket, folder, aws_conn_id, file_ext="csv"):
            return try_to_extract(bucket=bucket, folder=folder, aws_conn_id=aws_conn_id, file_ext=file_ext)

        @task()
        def get_product_data_file(extracted_files: dict):
            for key, df in extracted_files.items():
                if "product" in key:
                    return df.to_json(orient="split")
            raise AirflowException("Not found product data file")

        @task()
        def get_sales_data_file(extracted_files: dict):
            for key, df in extracted_files.items():
                if "sales" in key:
                    return df.to_json(orient="split")
            raise AirflowException("Not found sales data file")

        extracted_json_files = extract_json_files(bucket=config["s3"]["bucket"], folder=config["s3"]["folder"],
                                                  aws_conn_id=config["aws_conn_id"])
        extracted_csv_files = extract_csv_files(bucket=config["s3"]["bucket"], folder=config["s3"]["folder"],
                                                aws_conn_id=config["aws_conn_id"])
        product_file_in_json = get_product_data_file(extracted_json_files)
        sales_file_in_json = get_sales_data_file(extracted_csv_files)

        return {"products_json": product_file_in_json, "sales_json": sales_file_in_json}

    @task_group(group_id="transform_group")
    def transform_group(sales_json: str, products_json: str, ):
        """Cleaning, transformation and enrichment data."""

        @task
        def transform_sales_data(sales_dt_json: str):
            sales_df = pd.read_json(sales_dt_json, orient="split")
            cleaned_sales_df = sales_data_transformation(sales_df)
            return cleaned_sales_df.to_json(orient="split", date_format="iso")

        @task
        def transform_product_data(products_dt_json: str):
            products_df = pd.read_json(products_dt_json, orient="split")
            cleaned_products_df = products_data_transformation(products_df)
            return cleaned_products_df.to_json(orient="split", date_format="iso")

        @task
        def data_merging(sales_json: str, products_json: str):
            sales_df = pd.read_json(sales_json, orient="split")
            products_df = pd.read_json(products_json, orient="split")
            merged_df = merging_sales_data_with_products_data(sales_df, products_df)
            return merged_df.to_json(orient="split", date_format="iso")

        @task
        def data_enrich(merged_json: str):
            merged_df = pd.read_json(merged_json, orient="split")
            enriched_df = merged_data_enriched(merged_df)
            return enriched_df.to_json(orient="split", date_format="iso")

        cleaned_sales = transform_sales_data(sales_json)
        cleaned_products = transform_product_data(products_json)
        merged_data = data_merging(cleaned_sales, cleaned_products)
        enriched_data = data_enrich(merged_data)

        return enriched_data

    @task_group(group_id="analytical_group")
    def analytical_group(enriched_data: str):
        """Get cleaned and enriched data and perform analytical task to get some insights needed for business decision."""

        @task
        def get_quarterly_sales_trend(json_file: str):
            df = pd.read_json(json_file, orient="split")
            trend_df = quarterly_sales_by_category(df)
            return trend_df.to_json(orient="split", date_format="iso")

        @task
        def get_sales_ranking_and_performance(json_file: str):
            df = pd.read_json(json_file, orient="split")
            ranking_df = sales_revenue_by_region(df)
            return ranking_df.to_json(orient="split", date_format="iso")

        @task
        def get_sales_seasonality_by_category(json_file: str):
            df = pd.read_json(json_file, orient="split")
            seasonality_df = sales_seasonality(df)
            return seasonality_df.to_json(orient="split", date_format="iso")

        @task
        def get_weekly_orders_counts_by_status(json_file: str):
            df = pd.read_json(json_file, orient="split")
            weekly_counts_df = weekly_order_counts_by_status(df)
            return weekly_counts_df.to_json(orient="split", date_format="iso")

        @task
        def get_average_sales_and_units_by_sales_bucket(json_file: str):
            df = pd.read_json(json_file, orient="split")
            average_values_df = average_sales_and_units_by_sales_bucket(df)
            return average_values_df.to_json(orient="split", date_format="iso")

        quarterly_sales_trend = get_quarterly_sales_trend(enriched_data)
        sales_revenue_regional = get_sales_ranking_and_performance(enriched_data)
        sales_seasonality_per_category = get_sales_seasonality_by_category(enriched_data)
        weekly_orders_counting_by_status = get_weekly_orders_counts_by_status(enriched_data)
        average_sales_and_units_sales_bucket = get_average_sales_and_units_by_sales_bucket(enriched_data)

        return {"sales_trends": quarterly_sales_trend,
                "sales_ranking": sales_revenue_regional,
                "sales_seasonality": sales_seasonality_per_category,
                "sales_status": weekly_orders_counting_by_status,
                "average_sales_and_units_sales_bucket": average_sales_and_units_sales_bucket}

    @task_group(group_id="loading_group")
    def loading_group(final_json: str, sales_trends: str, sales_ranking: str, sales_seasonality: str,
                      sales_status: str, average_sales_and_units_sales_bucket: str):
        """Loading data in Snowflake after analytical tasks"""

        @task
        def snowflake_loading(final_json, database: str, schema: str, table_name: str, snowflake_conn_id: str):
            final_df = pd.read_json(final_json, orient="split")
            data_loading_in_snowflake(final_df, database, schema, table_name)

        connection_id = config["snowflake"]["conn_id"]
        dbase = config["snowflake"]["database"]
        targets = config["snowflake"]["targets"]

        snowflake_loading(final_json=final_json, database=dbase, schema=targets["sales"]["schema"],
                          table_name=targets["sales"]["table"], snowflake_conn_id=connection_id)
        snowflake_loading(final_json=final_json, database=dbase, schema=targets["products"]["schema"],
                          table_name=targets["products"]["table"], snowflake_conn_id=connection_id)
        snowflake_loading(final_json=final_json, database=dbase, schema=targets["merged"]["schema"],
                          table_name=targets["merged"]["table"], snowflake_conn_id=connection_id)
        snowflake_loading(final_json=final_json, database=dbase, schema=targets["enriched"]["schema"],
                          table_name=targets["enriched"]["table"], snowflake_conn_id=connection_id)

        snowflake_loading(final_json=sales_trends, database=dbase, schema=targets["trends"]["schema"],
                          table_name=targets["trends"]["table"], snowflake_conn_id=connection_id)

        snowflake_loading(final_json=sales_ranking, database=dbase, schema=targets["ranking"]["schema"],
                          table_name=targets["ranking"]["table"], snowflake_conn_id=connection_id)

        snowflake_loading(final_json=sales_seasonality, database=dbase, schema=targets["seasonality"]["schema"],
                          table_name=targets["seasonality"]["table"], snowflake_conn_id=connection_id)

        snowflake_loading(final_json=sales_status, database=dbase, schema=targets["status"]["schema"],
                          table_name=targets["status"]["table"], snowflake_conn_id=connection_id)

        snowflake_loading(final_json=average_sales_and_units_sales_bucket, database=dbase,
                          schema=targets["average"]["schema"],
                          table_name=targets["average"]["table"], snowflake_conn_id=connection_id)

    extracted = extract_group()
    enriched_json = transform_group(extracted["sales_json"], extracted["products_json"])
    analyzed = analytical_group(enriched_json)
    loading_group(final_json=enriched_json,
                  sales_trends=analyzed["sales_trends"],
                  sales_ranking=analyzed["sales_ranking"],
                  sales_seasonality=analyzed["sales_seasonality"],
                  sales_status=analyzed["sales_status"],
                  average_sales_and_units_sales_bucket=analyzed["average_sales_and_units_sales_bucket"]
                  )


etl_pipeline()

import logging

import pandas as pd

from include.validation.average_sales_and_units_by_sales_bucket_validation import \
    validate_average_sales_and_units_by_sales_bucket
from include.validation.enriched_data_validation_schema import validate_enriched_data_outgoing_schema
from include.validation.products_validation_schema import products_entry_schema, validate_product_outgoing_schema
from include.validation.quarterly_sales_validation_schema import validate_quarterly_sales_outgoing_schema
from include.validation.sales_revenue_by_region_outgoing_schema import \
    validate_sales_revenue_by_region_outgoing_schema
from include.validation.sales_validation_schema import validate_sales_entry_schema, validate_sales_outgoing_schema
from include.validation.validate_sales_seasonality_outgoing_schema import validate_sales_seasonality_outgoing_schema
from include.validation.weekly_order_counts_by_status_validation_schema import validate_weekly_order_counts_by_status

logger = logging.getLogger(__name__)


def sales_data_transformation(sales_df: pd.DataFrame):
    """ It is good practice to standardize all columns.
        In this case, we follow the exam requirements."""

    """Sales data cleaning and transformation"""

    logger.info(f"Initiating transformation of sales data")
    sales_df = validate_sales_entry_schema(sales_df)
    sales_df.columns = sales_df.columns.str.replace(" ", "_")
    sales_df["Region"] = sales_df["Region"].str.lower().str.strip()
    sales_df.dropna(subset=['Region', 'Time_stamp', 'proDuct_Id'], inplace=True)
    sales_df.drop_duplicates(inplace=True)
    sales_df = sales_df[(sales_df["Price"] > 0) & (sales_df["qty"] > 0)]
    sales_df["Time_stamp"] = pd.to_datetime(sales_df["Time_stamp"], errors="coerce")
    sales_df["total_sales"] = (sales_df["Price"] * (1 - sales_df["discount"] / 100)) * sales_df["qty"]
    sales_df.rename(columns={'proDuct_Id': 'product_id'}, inplace=True)   # Rename column name is not in project requrements,
    logger.info(f"Done transformation of sales data")                     # but it throw key error when merging,
    return validate_sales_outgoing_schema(sales_df)                       # table joints need equality in columns names


def products_data_transformation(products_df: pd.DataFrame):
    """Products data cleaning and transformation"""
    logger.info(f"Initiating transformation of products data")
    products_df = products_entry_schema(products_df)
    # None of the required four columns from the product_data file need a snake_case transformation.
    products_df['brand'] = products_df['brand'].str.upper()
    products_df["category"] = products_df["category"].str.lower()
    products_df.dropna(subset=['product_id', 'rating'],inplace=True)
    products_df.drop_duplicates(inplace=True)
    logger.info(f"Done transformation of products data")
    return validate_product_outgoing_schema(products_df)


def merging_sales_data_with_products_data(sales_df: pd.DataFrame, products_df: pd.DataFrame):
    """Merging sales data and products data files after cleaning and transformation"""
    logger.info(f"Start merging sales_df with products_df")
    merged_df = sales_df.merge(products_df, on="product_id", how="inner")
    return merged_df


def merged_data_enriched(merged_df: pd.DataFrame):
    """Enrichment after merging, to perform upcoming analytical tasks"""
    logger.info(f"Merged data enrich process")
    merged_df["month"] = pd.to_datetime(merged_df["Time_stamp"]).dt.month_name()
    merged_df["weekday"] = pd.to_datetime(merged_df["Time_stamp"]).dt.day_name()
    merged_df["hour"] = pd.to_datetime(merged_df["Time_stamp"]).dt.hour.astype("int64")
    merged_df["sales_bucket"] = pd.cut(
        merged_df["total_sales"],
        bins=[0, 100, 500, float("inf")],  # make sense to explore total_sales value and bins to be based on this?
        labels=["Low", "Mid", "High"],
                                           # TODO: get max value of total_sales with merged_df["total_sales"].max,
                                           # TODO: and create bins dynamically with np.linspace
    )

    return validate_enriched_data_outgoing_schema(merged_df)


def quarterly_sales_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """Identifying quarterly sales trend by category"""
    logger.info(f"Identifying quarterly sales trend by category")
    df['Time_stamp'] = pd.to_datetime(df['Time_stamp'])
    df['quarter'] = df['Time_stamp'].dt.to_period('Q').astype(str)
    quarterly_sales = df.groupby(['quarter', 'category'])['total_sales'].sum().reset_index()

    return validate_quarterly_sales_outgoing_schema(quarterly_sales)


def sales_revenue_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """ Calculate product sales revenue by region"""
    logger.info(f"Product sales revenue by region")
    region_sales = df.groupby('Region')['total_sales'].sum().reset_index()
    total_sales = region_sales['total_sales'].sum()
    region_sales["revenue_share"] = region_sales["total_sales"] / total_sales * 100
    region_sales['cumulative_revenue_share'] = region_sales['revenue_share'].cumsum()

    return validate_sales_revenue_by_region_outgoing_schema(region_sales)


def sales_seasonality(df: pd.DataFrame):
    """Finding fluctuation on sales over different months"""
    logger.info(f"Get Product sales seasonality by month and category")
    seasonality_df = df.groupby(['month', 'category']).agg(
        monthly_total_sales=('total_sales', 'sum'),
        monthly_total_quantity=('qty', 'sum')
    ).reset_index()

    return validate_sales_seasonality_outgoing_schema(seasonality_df)


def weekly_order_counts_by_status(df: pd.DataFrame):
    """Tracking order status on a weekly basis."""
    logger.info(f"Calculate weekly orders by their status")
    df["Time_stamp"] = pd.to_datetime(df["Time_stamp"])
    df["week"] = df["Time_stamp"].dt.isocalendar().week.astype("int64")
    order_counts = df.groupby(["week", "order_status"]).size().reset_index(name="order_counts")
    pivoted_df = order_counts.pivot_table(index="week", columns="order_status", values="order_counts",
                                          fill_value=0).reset_index()
    return validate_weekly_order_counts_by_status(pivoted_df)


def average_sales_and_units_by_sales_bucket(df: pd.DataFrame):
    """ Resume average sales and units by sales bucket. """
    logger.info(f"Resume average sales and units by sales bucket")
    average_df = df.groupby("sales_bucket").agg(
        average_sales=('total_sales', 'mean'),
        average_quantity=('qty', 'mean')
    ).reset_index()

    return validate_average_sales_and_units_by_sales_bucket(average_df)

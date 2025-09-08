import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)


sales_entry_schema = pa.DataFrameSchema({
    "sales id": pa.Column(int),
    "proDuct Id": pa.Column(int),
    "Region": pa.Column(str),
    "qty": pa.Column(int),
    "Price": pa.Column(float),
    "Time stamp": pa.Column(str),
    "discount": pa.Column(float),
    "order_status": pa.Column(str)
})


sales_outgoing_schema = pa.DataFrameSchema({
    "sales_id": Column(int),
    "product_id": Column(int),
    "Region": Column(str),
    "qty": Column(int, Check.greater_than(0)),
    "Price": Column(float, Check.greater_than(0)),
    "Time_stamp": Column(pa.DateTime),
    "discount": Column(float),
    "order_status": Column(str),
    "total_sales": Column(float)
})


def validate_sales_entry_schema(sales_df:pd.DataFrame):
    try:
        return sales_entry_schema.validate(sales_df)
    except SchemaError as e:
        logger.error(f"Entry schema validation failed: {e.failure_cases}")
        return sales_df


def validate_sales_outgoing_schema(sales_df:pd.DataFrame):
    return sales_outgoing_schema.validate(sales_df)

import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

products_entry_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock": Column(bool),
    # "launch_date": Column(pa.DateTime, nullable=True),
    "launch_date": Column(str, nullable=True),
})


product_outgoing_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str, Check(lambda c: c.str.islower())),
    "brand": Column(str, Check(lambda b: b.str.isupper())),
    "rating": Column(float),
    "in_stock": Column(bool),
    "launch_date": Column(str, nullable=True),   # If case of using it for analysis make column to pd.datetime!
})


def validate_products_entry_schema(products_df:pd.DataFrame):
    try:
        return products_entry_schema.validate(products_df)
    except SchemaError as e:
        logger.error(f"Entry schema validation failed: {e.failure_cases}")
        return products_df


def validate_product_outgoing_schema(products_df:pd.DataFrame):
    return product_outgoing_schema.validate(products_df)
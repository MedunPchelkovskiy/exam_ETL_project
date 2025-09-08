import logging

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

logger = logging.getLogger(__name__)

merged_data_outgoing_schema = pa.DataFrameSchema({
    "sales_id": Column(int),
    "product_id": Column(int),
    "Region": Column(str),
    "qty": Column(int),
    "Price": Column(float),
    # "time_stamp": Column(pa.DateTime),
    "Time_stamp": Column(str),
    "discount": Column(float),
    "order_status": Column(str),
    "total_sales": Column(float),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock": Column(bool),
    "launch_date": Column(str),
    # "launch_date": Column(pa.DateTime),
    "month": Column(str),
    "weekday": Column(str),
    "hour": Column(int),
    "sales_bucket": Column(str),
})


def validate_enriched_data_outgoing_schema(merged_df: pd.DataFrame):
    return merged_data_outgoing_schema.validate(merged_df)

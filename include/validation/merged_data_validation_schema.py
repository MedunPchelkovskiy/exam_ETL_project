import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

merged_data_outgoing_schema = pa.DataFrameSchema({
    "sales_id": Column(int),
    "proDuct_Id": Column(int),
    "region": Column(str),
    "qty": Column(int),
    "Price": Column(float),
    "Time_stamp": Column(pa.DateTime),
    "discount": Column(float),
    "order_status": Column(str),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock": Column(bool),
    "launch_date": Column(pa.DateTime),
})


def validate_merged_data_outgoing_schema(merged_df:pd.DataFrame):
    return merged_data_outgoing_schema.validate(merged_df)

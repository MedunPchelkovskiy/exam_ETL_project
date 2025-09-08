import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

weekly_order_counts_by_status_schema = pa.DataFrameSchema({
    "week": Column(int),
    "Pending": Column(float),
    "Shipped": Column(float),
    "Returned": Column(float)
})


def validate_weekly_order_counts_by_status(df: pd.DataFrame):
    return weekly_order_counts_by_status_schema.validate(df)
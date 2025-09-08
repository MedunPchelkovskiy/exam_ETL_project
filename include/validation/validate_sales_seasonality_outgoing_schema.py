import logging
import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column, Check
from pandera.errors import SchemaError

logger = logging.getLogger(__name__)

sales_seasonality_outgoing_schema = pa.DataFrameSchema({
    "category": Column(str),
    "month": Column(str),
    "monthly_total_sales": Column(float),
    "monthly_total_quantity": Column(int)
})


def validate_sales_seasonality_outgoing_schema(df: pd.DataFrame):
    return sales_seasonality_outgoing_schema.validate(df)
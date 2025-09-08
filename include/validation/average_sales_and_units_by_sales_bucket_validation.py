import logging

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

logger = logging.getLogger(__name__)

average_sales_and_units_by_sales_bucket_schema = pa.DataFrameSchema({
    "sales_bucket": Column(str),
    "average_sales": Column(float),
    "average_quantity": Column(float)
})


def validate_average_sales_and_units_by_sales_bucket(df: pd.DataFrame):
    return average_sales_and_units_by_sales_bucket_schema.validate(df)

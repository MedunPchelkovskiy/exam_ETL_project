import logging

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

logger = logging.getLogger(__name__)

sales_revenue_by_region_outgoing_schema = pa.DataFrameSchema({
    "Region": Column(str),
    "total_sales": Column(float),
    "revenue_share": Column(float),
    "cumulative_revenue_share": Column(float)
})


def validate_sales_revenue_by_region_outgoing_schema(df: pd.DataFrame) -> pd.DataFrame:
    return sales_revenue_by_region_outgoing_schema.validate(df)

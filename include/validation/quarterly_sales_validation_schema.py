import logging

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column

logger = logging.getLogger(__name__)

quarterly_sales_outgoing_schema = pa.DataFrameSchema({
    "quarter": Column(str),
    "category": Column(str),
    "total_sales": Column(float)
})


def validate_quarterly_sales_outgoing_schema(df: pd.DataFrame) -> pd.DataFrame:
    return quarterly_sales_outgoing_schema.validate(df)

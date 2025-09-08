import pandas as pd

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def data_loading_in_snowflake(df: pd.DataFrame, database: str, schema: str, table: str) -> None:
    """After completing the analytical tasks, we load the obtained data into Snowflake."""

    if len(df.index) == 0:
        raise ValueError("This Data Frame is empty")

    snowflake_hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    engine = snowflake_hook.get_sqlalchemy_engine()

    # with engine.begin() as conn:
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        index=False,
        if_exists="replace",
        method="multi"
    )

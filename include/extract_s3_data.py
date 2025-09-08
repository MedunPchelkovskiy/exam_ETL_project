import io
import logging

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.operator import AirflowException

logger = logging.getLogger(__name__)


def try_to_extract(bucket, folder, aws_conn_id, file_ext):
    """
    A function that extracts and reads files from Amazon S3 buckets.
    It allows extension to other file formats.
    It returns a dictionary with the file name as the key and
     the data frame as the value for subsequent processing.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        raise ValueError("Files not found!")

    dfs = {}

    for key in keys:
        """ If number of file formats expand, if/else statement can be replaced with dictionary(enums)"""

        if not key.lower().endswith(f"{file_ext}"):
            continue

        s3_file = s3_hook.get_key(key=key, bucket_name=bucket)
        file_content = s3_file.get()["Body"].read().decode("utf-8")

        try:
            if file_ext == "csv":
                df = pd.read_csv(io.StringIO(file_content))
            elif file_ext == "json":
                df = pd.read_json(io.StringIO(file_content))
            else:
                logger.error(f"{file_ext} file extension is not supported")
                raise AirflowException(f" The {file_ext} file extension is not supported")
        except Exception:
            logger.exception(f"Can't load file {key} with file extension {file_ext}")
            raise AirflowException(f"Can't load file {key} with file extension {file_ext}")

        dfs[key] = df
        logger.info(f"Successfully loaded {len(dfs)} file/s with {file_ext} from {folder} folder in {bucket} bucket")
        return dfs

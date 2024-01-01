"""Iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    IcebergSink,
)


class TargetIceberg(Target):
    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property("add_record_metadata", th.BooleanType, default=False),
        th.Property("aws_region", th.StringType, required=True),
        th.Property("aws_access_key_id", th.StringType),
        th.Property("aws_secret_access_key", th.StringType),
        th.Property("aws_session_token", th.StringType),
        th.Property("aws_profile", th.StringType),
        th.Property("s3_uri", th.StringType, required=True),
        th.Property("s3_bucket", th.StringType, required=True),  # e.g. my-bucket
        th.Property("s3_key_prefix", th.StringType),  # e.g. my-prefix
        th.Property("nessie_uri", th.StringType, required=True),
        th.Property("spark_master_uri", th.StringType, required=True),
        th.Property("spark_master_api_uri", th.StringType, required=True),
        th.Property("table_name", th.StringType),
        th.Property("primary_key", th.StringType),
        th.Property("partition_by", th.ArrayType(th.StringType)),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()

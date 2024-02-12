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
        th.Property(
            "use_spark",
            th.BooleanType,
            default=False,
            description="Use Spark to write to Iceberg instead of Pyiceberg, which is the default method",
        ),
        th.Property(
            "spark",
            th.ObjectType(
                th.Property("aws_region", th.StringType, required=True),
                th.Property("aws_access_key_id", th.StringType, required=True),
                th.Property("aws_secret_access_key", th.StringType, required=True),
                th.Property("s3_uri", th.StringType, required=True),
                th.Property("s3_bucket", th.StringType, required=True),
                th.Property("rest_catalog_uri", th.StringType, required=True),
                th.Property("app_name", th.StringType, required=True),
                th.Property("num_executors", th.StringType, default="2"),
                th.Property("executor_memory", th.StringType, default="2g"),
                th.Property("primary_key", th.StringType, required=True),
                th.Property("partition_by", th.ArrayType, default=None),
            ),
        ),
        th.Property(
            "pyiceberg",
            th.ObjectType(
                th.Property(
                    "iceberg_catalog_name",
                    th.StringType,
                    required=True,
                    description="Name of the Iceberg catalog",
                ),
                th.Property(
                    "iceberg_catalog_namespace_name",
                    th.StringType,
                    required=True,
                    description="Name of the Iceberg catalog namespace (only used `use_spark` is set to False)",
                ),
            ),
        ),
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()

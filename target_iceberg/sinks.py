"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from typing import cast, Any
from singer_sdk.sinks import BatchSink
import pyarrow as pa  # type: ignore
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
from pyarrow import fs

from .iceberg import singer_to_pyarrow_schema, pyarrow_to_pyiceberg_schema


class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.stream_name = stream_name
        self.schema = schema

    def validate_record(self, record):
        for key, value in record.items():
            if key in self.schema['properties']:
                expected_type = self.schema['properties'][key]['type']
                if isinstance(expected_type, list):
                    expected_type = [t for t in expected_type if t != 'null'][0]  # Get the non-null type
                if value is not None:
                    try:
                        if expected_type == 'integer':
                            record[key] = int(value)
                        elif expected_type == 'number':
                            record[key] = float(value)
                        elif expected_type == 'string':
                            record[key] = str(value)
                        elif expected_type == 'boolean':
                            record[key] = bool(value)
                        # Add more type conversions as needed
                    except ValueError:
                        self.logger.warning(f"Could not convert field '{key}' to {expected_type}. Value: {value}")
            else:
                self.logger.warning(f"Field '{key}' not found in schema")

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.info(f"Processing batch of {len(context['records'])} records for stream {self.stream_name}")

        # Validate and convert record types
        for record in context["records"]:
            self.validate_record(record)

        # Load the Iceberg catalog
        region = fs.resolve_s3_region(self.config.get("s3_bucket"))
        self.logger.info(f"AWS Region: {region}")

        catalog_name = self.config.get("iceberg_catalog_name")
        self.logger.info(f"Catalog name: {catalog_name}")

        s3_endpoint = self.config.get("s3_endpoint")
        self.logger.info(f"S3 endpoint: {s3_endpoint}")

        iceberg_rest_uri = self.config.get("iceberg_rest_uri")
        self.logger.info(f"Iceberg REST URI: {iceberg_rest_uri}")

        catalog = load_catalog(
            catalog_name,
            **{
                "uri": iceberg_rest_uri,
                "s3.endpoint": s3_endpoint,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": region,
                "s3.access-key-id": self.config.get("aws_access_key_id"),
                "s3.secret-access-key": self.config.get("aws_secret_access_key"),
            },
        )

        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name: str = cast(str, self.config.get("iceberg_catalog_namespace_name"))
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            # NoSuchNamespaceError is also raised for some reason (probably a bug - but needs to be handled anyway)
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create pyarrow df
        singer_schema = self.schema
        pa_schema = singer_to_pyarrow_schema(self, singer_schema)
        df = pa.Table.from_pylist(context["records"], schema=pa_schema)

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

            # TODO: Handle schema evolution - compare existing table schema with singer schema (converted to pyiceberg schema)
        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)
            table = catalog.create_table(table_id, schema=pyiceberg_schema)
            self.logger.info(f"Table '{table_id}' created")

        # Add data to the table
        try:
            table.append(df)
            self.logger.info(f"Successfully appended {len(context['records'])} records to table '{table_id}'")
        except Exception as e:
            self.logger.error(f"Failed to append data to table '{table_id}': {str(e)}")
            raise

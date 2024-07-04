"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import pandas as pd
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

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
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
        conrecords = context["records"]
        self.logger.info(f"********* context[records]: {conrecords} *********")

        # Convert records to a Pandas DataFrame
        df_pandas = pd.DataFrame(context["records"])

        self.logger.info(f"********* df_pandas.head(1): {df_pandas.head(1)} *********")

        # Create a PyArrow Table from the DataFrame, inferring the schema
        df_pyarrow = pa.Table.from_pandas(df_pandas, preserve_index=False)

        def add_field_ids(schema, start_id=1):
            field_ids = list(range(start_id, start_id + len(schema)))
            fields_with_ids = []
            
            for field, field_id in zip(schema, field_ids):
                if pa.types.is_struct(field.type):
                    # Recursively add field_ids to the nested struct
                    nested_schema_with_ids, next_id = add_field_ids(field.type)
                    field_with_id = pa.field(
                        field.name,
                        pa.struct(nested_schema_with_ids),
                        field.nullable,
                        metadata={"field_id": str(field_id)}
                    )
                else:
                    field_with_id = pa.field(
                        field.name,
                        field.type,
                        field.nullable,
                        metadata={"field_id": str(field_id)}
                    )
                fields_with_ids.append(field_with_id)
            
            return fields_with_ids

        # Add field IDs to the PyArrow schema
        # field_ids = list(range(1, len(df_pyarrow.schema) + 1))
        # fields_with_ids = [
        #     pa.field(field.name, field.type, field.nullable, metadata={"field_id": str(field_id)})
        #     for field, field_id in zip(df_pyarrow.schema, field_ids)
        # ]
        # schema_with_ids = pa.schema(fields_with_ids)

        schema_with_ids = pa.schema(add_field_ids(df_pyarrow.schema))

        df_pyarrow = df_pyarrow.cast(schema_with_ids)

        # # Create pyarrow df
        # singer_schema = self.schema
        # pa_schema = singer_to_pyarrow_schema(self, singer_schema)
        # df = pa.Table.from_pylist(context["records"], schema=pa_schema)

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        # try:
        #     table = catalog.load_table(table_id)
        #     self.logger.info(f"Table '{table_id}' loaded")

        #     # TODO: Handle schema evolution - compare existing table schema with singer schema (converted to pyiceberg schema)
        # except NoSuchTableError as e:
        #     # Table doesn't exist, so create it
        #     pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)
        #     table = catalog.create_table(table_id, schema=pyiceberg_schema)
        #     self.logger.info(f"Table '{table_id}' created")

        # # Add data to the table
        # table.append(df)

        try:
            table = catalog.load_table(table_id)
        except NoSuchTableError:
            # Create table with schema inferred from PyArrow Table
            self.logger.info(f"********* PyArrow Schema: '{df_pyarrow.schema}' *********")
            pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, df_pyarrow.schema)
            table = catalog.create_table(table_id, schema=pyiceberg_schema)

        # Append data to the table
        table.append(df_pyarrow)

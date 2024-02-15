"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from typing import Dict, List, Optional
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
from requests import HTTPError
from pyarrow import fs

from .iceberg import singer_to_pyiceberg_schema


class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
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

        # Create pyarrow df
        fields_to_drop = ["_sdc_deleted_at", "_sdc_table_version"]
        df = pa.Table.from_pylist(context["records"])
        df_narrow = df.drop_columns(fields_to_drop)

        # Load the Iceberg catalog
        # IMPORTANT: Make sure pyiceberg catalog env variables are set in the host machine - i.e. PYICEBERG_CATALOG__DEFAULT__URI, etc
        #   - For more details, see: https://py.iceberg.apache.org/configuration/)
        region = fs.resolve_s3_region(self.config.get("s3_bucket"))
        catalog_name = self.config.get("iceberg_catalog_name")
        catalog = load_catalog(
            catalog_name,
            **{
                "uri": self.config.get("iceberg_rest_uri"),
                "s3.endpoint": os.environ.get(
                    "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__ENDPOINT"
                ),
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": region,
                "s3.access-key-id": os.environ.get(
                    "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__ACCESS_KEY_ID"
                ),
                "s3.secret-access-key": os.environ.get(
                    "PYICEBERG_CATALOG__ICEBERGCATALOG__S3__SECRET_ACCESS_KEY"
                ),
            },
        )

        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name = self.config.get("iceberg_catalog_namespace_name")
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError): 
            # NoSuchNamespaceError is also raised for some reason (probably a bug - but needs to be handled anyway)
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"
        singer_schema = self.schema
        singer_schema_narrow = singer_schema
        singer_schema_narrow["properties"] = {x: singer_schema["properties"][x] for x in singer_schema["properties"] if x not in fields_to_drop}

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

            # TODO: Handle schema evolution - compare existing table schema with singer schema (converted to pyiceberg schema)
        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            table_schema = singer_to_pyiceberg_schema(self, singer_schema)
            table = catalog.create_table(table_id, schema=table_schema)
            self.logger.info(f"Table '{table_id}' created")

        # Add data to the table
        table.append(df_narrow)

"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Dict, List, Optional
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from .iceberg import build_table_schema
from .utils import singer_records_to_list


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
        self.logger.info(f"self.config={self.config}")

        # Create pyarrow df
        pylist = singer_records_to_list(context["records"])
        df = pa.Table.from_pylist(pylist)

        # Load the Iceberg catalog (see ~/.pyiceberg.yaml)
        catalog_name = "default"
        catalog = load_catalog(catalog_name)

        # Define a schema
        # json_schema: from singer tap - i.e. {"id": {"type": "integer"}, "updated_at": {"type": "string", "format": "date-time"}, ...}
        json_schema = self.schema["properties"]
        table_schema = build_table_schema(json_schema)

        # Create a table
        table_name = self.stream_name
        table = catalog.create_table(f"{catalog_name}.{table_name}", schema=table_schema)

        # Add data to the table
        table.append(df)

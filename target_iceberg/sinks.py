"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Dict, List, Optional
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from requests import HTTPError

from .pyiceberg import singer_schema_to_pyiceberg_schema
from .spark.utils import run_spark_submit, upload_packaged_files_to_s3


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

    def process_batch_via_spark(self, context: dict) -> None:
        SPARK_APP_PATH = "./spark/spark.py"
        
        spark_config = self.config.get("spark")

        # Upload packaged files to shared file store
        project_files_tar_s3_path, project_venv_tar_s3_path = (
            upload_packaged_files_to_s3(config=spark_config, dist_dir="../dist")
        )

        run_spark_submit(
            config=spark_config,
            opt={
                "project_files_tar_s3_path": project_files_tar_s3_path,
                "project_venv_tar_s3_path": project_venv_tar_s3_path,
            },
            spark_app_path=SPARK_APP_PATH,
        )

    def process_batch_via_pyiceberg(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        pyiceberg_config = self.config.get("pyiceberg")

        # Create pyarrow df
        df = pa.Table.from_pylist(context["records"])

        # Load the Iceberg catalog
        # IMPORTANT: Make sure pyiceberg catalog env variables are set in the host machine - i.e. PYICEBERG_CATALOG__DEFAULT__URI, etc
        #   - For more details, see: https://py.iceberg.apache.org/configuration/)
        catalog_name = pyiceberg_config.get("iceberg_catalog_name")
        catalog = load_catalog(catalog_name)

        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name = pyiceberg_config.get("iceberg_catalog_namespace_name")
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except NamespaceAlreadyExistsError:
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

            # TODO: Handle schema evolution - compare existing table schema with singer schema (converted to pyiceberg schema)
        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            table_schema = singer_schema_to_pyiceberg_schema(self, self.schema)
            table = catalog.create_table(table_id, schema=table_schema)
            self.logger.info(f"Table '{table_id}' created")

        # Add data to the table
        table.append(df)

    def process_batch(self, context: dict) -> None:
        use_spark = self.config.get("use_spark")

        if use_spark:
            self.process_batch_via_spark(self, context)
        else:
            self.process_batch_via_pyiceberg(self, context)

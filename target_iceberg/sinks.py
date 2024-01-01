"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Dict, List, Optional
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
from pyspark.sql import SparkSession
from target_iceberg.spark import get_spark_conf, submit_spark_job


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

    # def start_batch(self, context: dict) -> None:
    #     """Start a batch.

    #     Developers may optionally add additional markers to the `context` dict,
    #     which is unique to this batch.

    #     Args:
    #         context: Stream partition or context dictionary.
    #     """
    #     # Sample:
    #     # ------
    #     # batch_key = context["batch_id"]
    #     # context["file_path"] = f"{batch_key}.csv"

    # def process_record(self, record: dict, context: dict) -> None:
    #     """Process the record.

    #     Developers may optionally read or write additional markers within the
    #     passed `context` dict from the current batch.

    #     Args:
    #         record: Individual record in the stream.
    #         context: Stream partition or context dictionary.
    #     """
    #     # Sample:
    #     # ------
    #     # with open(context["file_path"], "a") as csvfile:
    #     #     csvfile.write(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.

        Required script for build:
            `venv-pack -p ./.venv`
        """
        # Start Spark Session
        spark_conf = get_spark_conf(self.config)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        print("Spark Running")

        # Create a Spark dataframe
        headers = self.schema["properties"].keys()
        df = spark.createDataframe(context["records"], headers)

        # Create a temp view of the dataframe so it can be selected via SQL
        df.createOrReplaceTempView("records_temp_view")

        # Create an Iceberg table
        partition_clause = (
            ""
            if not self.partition_by
            else f"PARTITIONED BY ({', '.join(self.partition_by)})"
        )

        spark.sql(
            f"CREATE TABLE IF NOT EXISTS nessie.{self.table_name} USING iceberg {partition_clause}"
        ).show()

        # Write the dataframe to the Iceberg table
        primary_key = self.key_properties[0]
        spark.sql(
            f"""MERGE INTO nessie.{self.table_name} t USING (SELECT * FROM records_temp_view) u ON t.{primary_key} = u.{primary_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *"""
        )
        
        # Submit the spark job
        submit_spark_job(self.config)

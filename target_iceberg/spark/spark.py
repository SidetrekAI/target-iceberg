import json
import argparse
from pyspark.sql import SparkSession

from .utils import get_spark_conf


def spark_main():
    # Grab the config from spark-submit command
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-config", help="Config for the Spark app")
    args = parser.parse_args()
    config = {}
    if args.app_config:
        config = json.loads(args.app_config)

    # Start Spark Session
    spark_conf = get_spark_conf(config=config)
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    print("Spark Session created successfully")

    print("# SparkSession Created", "#" * 80)

    # Create a Spark dataframe
    headers = list(self.schema["properties"].keys())
    df = spark.createDataFrame(context["records"], headers)
    print("Spark df created")

    # Create a temp view of the dataframe so it can be selected via SQL
    df.createOrReplaceTempView("records_temp_view")
    print("Spark temp view created")

    # Get the schema of the DataFrame created from the temporary view
    temp_view_schema = df.schema

    # Construct the schema definition part of the SQL statement based on the temporary view's schema
    schema_definition = ", ".join(
        [
            f"{field.name} {field.dataType.simpleString()}"
            for field in temp_view_schema.fields
        ]
    )

    # Include partitioning if required
    partition_clause = (
        ""
        if not spark_conf.get("partition_by")
        else f"PARTITIONED BY ({', '.join(spark_conf['partition_by'])})"
    )

    # Specify the Iceberg table name
    iceberg_table_name = spark_conf.get(
        "table_name"
    )  # Ensure this is set to the target Iceberg table name

    # Drop the Iceberg table if it already exists
    spark.sql(f"DROP TABLE IF EXISTS nessie.{iceberg_table_name}")

    # Construct the CREATE TABLE statement for the Iceberg table based on the temp view's schema
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS nessie.{iceberg_table_name} (
            {schema_definition}
        ) USING iceberg
        {partition_clause}
    """
    spark.sql(create_table_sql).show()

    print("# Iceberg Table Created", "#" * 80)

    columns = [col for col in df.columns if col != spark_conf.get("primary_key")]
    # Construct SET part of the SQL for UPDATE
    update_set_sql = ", ".join([f"t.{col} = u.{col}" for col in columns])

    # Construct columns and values SQL for INSERT
    insert_columns_sql = ", ".join(columns)
    insert_values_sql = ", ".join([f"u.{col}" for col in columns])

    # Construct the MERGE INTO SQL statement
    merge_sql = f"""
        MERGE INTO nessie.{spark_conf.get('table_name')} AS t
        USING (SELECT * FROM records_temp_view) AS u
        ON t.{spark_conf.get('primary_key')} = u.{spark_conf.get('primary_key')}
        WHEN MATCHED THEN UPDATE SET {update_set_sql}
        WHEN NOT MATCHED THEN INSERT ({insert_columns_sql}) VALUES ({insert_values_sql})
    """

    # Execute the MERGE INTO operation
    spark.sql(merge_sql).show()


spark_main()

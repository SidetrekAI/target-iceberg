from typing import Dict
import os
import json
import glob
import subprocess
from typing import Dict
import logging
import pyspark
import boto3
from botocore.exceptions import ClientError


SPARK_TEMP_S3_PREFIX = "spark/temp"
ICEBERG_VERSION = "1.4.3"
AWS_VERSION = "2.20.131"


def get_spark_conf(config: Dict) -> pyspark.SparkConf:
    """Get Spark configuration"""

    spark_packages = (
        "spark.jars.packages",
        f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        f"software.amazon.awssdk:bundle:{AWS_VERSION}",
        f"software.amazon.awssdk:url-connection-client:{AWS_VERSION}",
    )

    sql_extensions = (
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    catalog_config = [
        ("spark.sql.catalog.icebergcatalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.icebergcatalog.type", "rest"),
        ("spark.sql.catalog.icebergcatalog.uri", config.set("rest_catalog_uri")),
    ]

    spark_conf = pyspark.SparkConf().setAll(
        [spark_packages, sql_extensions, *catalog_config]
    )

    return spark_conf


def upload_file(
    file_name,
    bucket,
    object_name=None,
    aws: Dict = {},
):
    """Upload a file to an S3 bucket"""

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        "s3",
        endpoint_url=aws.get("s3_uri"),
        aws_access_key_id=aws.get("aws_access_key_id"),
        aws_secret_access_key=aws.get("aws_secret_access_key"),
        aws_session_token=aws.get("aws_session_token"),
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def upload_packaged_files_to_s3(config: Dict, dist_dir: str):
    """
    First upload packaged project files and venv to s3 so they can be accessed by both Spark master and workers
    """
    project_files_tar_path = os.path.abspath(
        [fname for fname in glob.glob(f"{dist_dir}/spark_submit_test-*.tar.gz")][0]
    )
    project_venv_tar_path = os.path.abspath(f"{dist_dir}/project_venv.tar.gz")

    for fname in [project_files_tar_path, project_venv_tar_path]:
        upload_file(
            fname,
            bucket=config.get("s3_bucket"),
            object_name=f"{SPARK_TEMP_S3_PREFIX}/{os.path.basename(fname)}",
            aws={
                "s3_uri": config.get("s3_uri"),
                "aws_access_key_id": config.get("aws_access_key_id"),
                "aws_secret_access_key": config.get("aws_secret_access_key"),
            },
        )

    project_files_tar_fname = os.path.basename(project_files_tar_path)
    project_files_tar_s3_path = f"s3a://{config.get('s3_bucket')}/{SPARK_TEMP_S3_PREFIX}/{project_files_tar_fname}"
    project_venv_tar_fname = os.path.basename(project_venv_tar_path)
    project_venv_tar_s3_path = f"s3a://{config.get('s3_bucket')}/{SPARK_TEMP_S3_PREFIX}/{project_venv_tar_fname}"

    return project_files_tar_s3_path, project_venv_tar_s3_path


def run_spark_submit(config: Dict, spark_app_path):
    app_name = config.get("app_name")
    num_executors = config.get("num_executors")
    executor_memory = config.get("executor_memory")

    # Cluster mode is not supported for Python applications and this is why we need client mode
    command = [
        "/opt/spark/bin/spark-submit",
        "--name",
        app_name,
        "--deploy-mode",
        "client",
        "--num-executors",
        num_executors,
        "--executor-memory",
        executor_memory,
        "--py-files",
        "./constants.py",
        spark_app_path,
        "--app-config",
        json.dumps(config),
    ]

    subprocess.run(command, shell=False)

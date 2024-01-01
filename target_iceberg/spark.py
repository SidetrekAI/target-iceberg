import pyspark
import requests


def get_spark_conf(config):
    # Spark packages
    spark_pacakges = (
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13-1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,software.amazon.awssdk:bundle:2.21.41,software.amazon.awssdk:url-connection-client:2.21.41",
    )

    # SQL Extensions
    sql_extentions = (
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )

    # Configuring Catalog
    nessie_warehouse = (
        f"s3a://{config.s3_bucket}"
        if not config.s3_key_prefix
        else f"s3a://{config.s3_bucket}/{config.s3_key_prefix}"
    )
    catalog_config = [
        ("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.nessie.uri", config.nessie_uri),
        ("spark.sql.catalog.nessie.ref", "main"),  # default branch,
        ("spark.sql.catalog.nessie.authentication.type", "NONE"),
        (
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        ),
        ("spark.sql.catalog.nessie.warehouse", nessie_warehouse),
        ("spark.sql.catalog.nessie.s3.endpoint", config.s3_uri),
        ("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
    ]

    # S3/Minio credentials
    s3_config = [
        ("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id),
        ("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key),
    ]

    return (
        pyspark.SparkConf()
        .setAppName("test-iceberg")
        .setMaster(config.spark_master_uri)
        .setAll([spark_pacakges, sql_extentions, *catalog_config, *s3_config])
    )


def submit_spark_job(config):
    """
    For this to work properly, run `poetry build` before pushing to the repo.
    This will package up external dependencies as .whl and .tar.gz files in the dist/ directory, which can be used in spark-submit via --archives flag.
    """
    requests.post(
        url=config.spark_master_api_uri + "/v1/submissions/create",
        headers={"Content-Type": "application/json;charset=UTF-8"},
        data={
            "clientSparkVersion": "3.5.0",
            "action": "CreateSubmissionRequest",
            "mainClass": "org.apache.spark.deploy.SparkSubmit",
            "appResource": "./target_iceberg/target.py",
            "sparkProperties": {
                "spark.app.name": "target-iceberg",
                "spark.driver.supervise": "false",
                "spark.master": config.spark_master_uri,
                "spark.submit.deployMode": "cluster",
                "spark.driver.cores": "2",
                "spark.driver.memory": "2g",
                "spark.executor.cores": "2",
                "spark.executor.memory": "2g",
                "spark.eventLog.enabled": "false",
                "spark.submit.files": "./target_iceberg/spark-defaults.conf",
                "spark.submit.pyFiles": "./target_iceberg/*.py",
                "spark.archives": "./dist/target_iceberg-*.tar.gz",
            },
            "environmentVariables": {"SPARK_ENV_LOADED": "1"},
        },
    )

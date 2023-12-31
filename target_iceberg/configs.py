import pyspark
from pyspark.sql import SparkSession
import os

spark_conf = (
    pyspark.SparkConf()
        .setAppName('test-iceberg')
  		# packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.13-1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,software.amazon.awssdk:bundle:2.21.41,software.amazon.awssdk:url-connection-client:2.21.41')
  		# SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
  		# Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', os.environ['NESSIE_URI'])
        .set('spark.sql.catalog.nessie.ref', 'main') # default branch
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://minio:9001')
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
  		# MINIO CREDENTIALS
        .set('spark.hadoop.fs.s3a.access.key', os.environ['MINIO_ACCESS_KEY'])
        .set('spark.hadoop.fs.s3a.secret.key', os.environ['MINIO_SECRET_KEY'])
)
"""SparkSession factory.

On Databricks the runtime already provides a session (with Delta); we just reuse it.
Locally we build one with the open-source delta-spark package.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "real-estate-lakehouse") -> SparkSession:
    if is_databricks():
        # Serverless / Free Edition: do not set unsupported configs; Delta is built in.
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip  # local-only dependency

    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # local only; never reached on Databricks
    return spark

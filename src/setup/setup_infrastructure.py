import argparse

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--landing_volume", required=True)

args = parser.parse_args()

catalog = args.catalog
schema = args.schema
landing_volume = args.landing_volume

spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError("No active Spark session found.")

if not catalog or not schema or not landing_volume:
    raise ValueError(
        "catalog, schema, and landing_volume parameters are required."
    )

print(
    f"Initializing infrastructure for "
    f"{catalog}.{schema} using volume {landing_volume}"
)


# ---------------------------------------------------------
# Catalog
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE CATALOG IF NOT EXISTS `{catalog}`
    """
)


# ---------------------------------------------------------
# Schema
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`
    """
)


# ---------------------------------------------------------
# Landing volume
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    `{catalog}`.`{schema}`.`{landing_volume}`
    """
)


# ---------------------------------------------------------
# Bronze
# Raw GPS pings
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{catalog}`.`{schema}`.`bronze_pings`
    (
        truck_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        event_ts STRING,
        ingestion_ts TIMESTAMP,
        source_file STRING
    )
    USING DELTA
    """
)


# ---------------------------------------------------------
# Silver
# Validated and deduplicated GPS pings
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{catalog}`.`{schema}`.`silver_pings`
    (
        truck_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        event_ts TIMESTAMP,
        ingestion_ts TIMESTAMP,
        source_file STRING
    )
    USING DELTA
    """
)


# ---------------------------------------------------------
# Static truck reference table
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{catalog}`.`{schema}`.`truck_details`
    (
        truck_id STRING,
        make STRING,
        model STRING,
        capacity_lbs INT,
        home_depot STRING,
        region STRING,
        driver STRING
    )
    USING DELTA
    """
)


# ---------------------------------------------------------
# Gold
# Current enriched truck position
# ---------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
    `{catalog}`.`{schema}`.`gold_truck_positions`
    (
        truck_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        event_ts TIMESTAMP,
        make STRING,
        model STRING,
        capacity_lbs INT,
        home_depot STRING,
        region STRING,
        driver STRING
    )
    USING DELTA
    """
)

print(f"Infrastructure initialized successfully for {catalog}.{schema}")
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col


# ---------------------------------------------------------
# Parameters
# ---------------------------------------------------------

parser = argparse.ArgumentParser()

parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--landing_volume", required=True)

args = parser.parse_args()

catalog = args.catalog
schema = args.schema
landing_volume = args.landing_volume


# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError("No active Spark session found.")


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

landing_path = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/pings"
)

schema_location = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/"
    f"_schemas/bronze_pings"
)

checkpoint_location = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/"
    f"_checkpoints/bronze_pings"
)

target_table = (
    f"{catalog}.{schema}.bronze_pings"
)


print(f"Landing path: {landing_path}")
print(f"Schema location: {schema_location}")
print(f"Checkpoint location: {checkpoint_location}")
print(f"Target table: {target_table}")


# ---------------------------------------------------------
# Auto Loader
# ---------------------------------------------------------

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(landing_path)
)


# ---------------------------------------------------------
# Add operational metadata
# ---------------------------------------------------------

bronze_with_metadata = (
    bronze_stream
    .withColumn(
        "ingestion_ts",
        current_timestamp()
    )
    .withColumn(
        "source_file",
        col("_metadata.file_path")
    )
    .select(
        "truck_id",
        "latitude",
        "longitude",
        "event_ts",
        "ingestion_ts",
        "source_file"
    )
)


# ---------------------------------------------------------
# Write to Bronze Delta table
# ---------------------------------------------------------

query = (
    bronze_with_metadata.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        checkpoint_location
    )
    .trigger(availableNow=True)
    .toTable(target_table)
)


query.awaitTermination()

print(
    f"Bronze ingestion completed successfully: "
    f"{target_table}"
)
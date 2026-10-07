import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


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
# Tables / checkpoint
# ---------------------------------------------------------

bronze_table = (
    f"{catalog}.{schema}.bronze_pings"
)

silver_table = (
    f"{catalog}.{schema}.silver_pings"
)

checkpoint_location = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/"
    f"_checkpoints/silver_pings"
)


print(f"Bronze table: {bronze_table}")
print(f"Silver table: {silver_table}")
print(f"Checkpoint: {checkpoint_location}")


# ---------------------------------------------------------
# Read Bronze as a stream
# ---------------------------------------------------------

bronze_stream = (
    spark.readStream
    .table(bronze_table)
)


# ---------------------------------------------------------
# Clean and conform
# ---------------------------------------------------------

clean_stream = (
    bronze_stream

    # Conform timestamp type
    .withColumn(
        "event_ts",
        to_timestamp(col("event_ts"))
    )

    # Required fields
    .filter(
        col("truck_id").isNotNull()
        & col("event_ts").isNotNull()
    )

    # Valid latitude
    .filter(
        col("latitude").isNotNull()
        & col("latitude").between(-90.0, 90.0)
    )

    # Valid longitude
    .filter(
        col("longitude").isNotNull()
        & col("longitude").between(-180.0, 180.0)
    )
)


# ---------------------------------------------------------
# foreachBatch MERGE
# ---------------------------------------------------------

def merge_to_silver(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no rows to process.")
        return

    # Remove duplicate logical pings inside this micro-batch.
    deduplicated_df = (
        batch_df
        .dropDuplicates(
            ["truck_id", "event_ts"]
        )
    )

    source_count = batch_df.count()
    deduplicated_count = deduplicated_df.count()

    print(
        f"Batch {batch_id}: "
        f"valid rows={source_count}, "
        f"after deduplication={deduplicated_count}"
    )

    silver_delta = DeltaTable.forName(
        spark,
        silver_table
    )

    (
        silver_delta.alias("target")
        .merge(
            deduplicated_df.alias("source"),
            """
            target.truck_id = source.truck_id
            AND target.event_ts = source.event_ts
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"Batch {batch_id}: MERGE completed."
    )


# ---------------------------------------------------------
# Start Silver stream
# ---------------------------------------------------------

query = (
    clean_stream.writeStream
    .foreachBatch(merge_to_silver)
    .option(
        "checkpointLocation",
        checkpoint_location
    )
    .trigger(availableNow=True)
    .start()
)


query.awaitTermination()

print(
    f"Silver processing completed successfully: "
    f"{silver_table}"
)
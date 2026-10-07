import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window


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

silver_table = (
    f"{catalog}.{schema}.silver_pings"
)

truck_details_table = (
    f"{catalog}.{schema}.truck_details"
)

gold_table = (
    f"{catalog}.{schema}.gold_truck_positions"
)

checkpoint_location = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/"
    f"_checkpoints/gold_truck_positions"
)


print(f"Silver table: {silver_table}")
print(f"Truck details table: {truck_details_table}")
print(f"Gold table: {gold_table}")
print(f"Checkpoint: {checkpoint_location}")


# ---------------------------------------------------------
# Static reference data
# ---------------------------------------------------------

truck_details = (
    spark.read
    .table(truck_details_table)
)


# ---------------------------------------------------------
# Streaming Silver source
# ---------------------------------------------------------

silver_stream = (
    spark.readStream
    .table(silver_table)
)


# ---------------------------------------------------------
# Stream-static join
# ---------------------------------------------------------

enriched_stream = (
    silver_stream.alias("p")
    .join(
        truck_details.alias("t"),
        col("p.truck_id") == col("t.truck_id"),
        "left"
    )
    .select(
        col("p.truck_id").alias("truck_id"),
        col("p.latitude").alias("latitude"),
        col("p.longitude").alias("longitude"),
        col("p.event_ts").alias("event_ts"),
        col("t.make").alias("make"),
        col("t.model").alias("model"),
        col("t.capacity_lbs").alias("capacity_lbs"),
        col("t.home_depot").alias("home_depot"),
        col("t.region").alias("region"),
        col("t.driver").alias("driver")
    )
)


# ---------------------------------------------------------
# foreachBatch current-position MERGE
# ---------------------------------------------------------

def merge_current_positions(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no rows to process.")
        return

    # Find latest ping for each truck within this micro-batch.
    latest_window = (
        Window
        .partitionBy("truck_id")
        .orderBy(col("event_ts").desc())
    )

    latest_batch = (
        batch_df
        .withColumn("_row_number", row_number().over(latest_window))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
        .withColumn("position_updated_at", current_timestamp())
    )

    print(
        f"Batch {batch_id}: "
        f"processing {latest_batch.count()} truck positions"
    )

    gold_delta = DeltaTable.forName(
        spark,
        gold_table
    )

    (
        gold_delta.alias("target")
        .merge(
            latest_batch.alias("source"),
            "target.truck_id = source.truck_id"
        )

        # Update only if incoming position is newer.
        .whenMatchedUpdate(
            condition="""
                source.event_ts > target.event_ts
            """,
            set={
                "latitude": "source.latitude",
                "longitude": "source.longitude",
                "event_ts": "source.event_ts",
                "make": "source.make",
                "model": "source.model",
                "capacity_lbs": "source.capacity_lbs",
                "home_depot": "source.home_depot",
                "region": "source.region",
                "driver": "source.driver",
                "position_updated_at": "source.position_updated_at"
            }
        )

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        f"Batch {batch_id}: Gold MERGE completed."
    )


# ---------------------------------------------------------
# Start Gold stream
# ---------------------------------------------------------

query = (
    enriched_stream.writeStream
    .foreachBatch(merge_current_positions)
    .option(
        "checkpointLocation",
        checkpoint_location
    )
    .trigger(availableNow=True)
    .start()
)


query.awaitTermination()

print(
    f"Gold processing completed successfully: "
    f"{gold_table}"
)
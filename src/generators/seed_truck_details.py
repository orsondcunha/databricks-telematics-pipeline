import argparse
import random

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)

args = parser.parse_args()

catalog = args.catalog
schema = args.schema

spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError("No active Spark session found.")


# ---------------------------------------------------------
# Deterministic seed
# ---------------------------------------------------------

random.seed(42)


# ---------------------------------------------------------
# Reference data
# ---------------------------------------------------------

makes = [
    ("Freightliner", "Cascadia"),
    ("Volvo", "VNL"),
    ("Kenworth", "T680"),
    ("Peterbilt", "579"),
]

depots = [
    ("Chicago", "Midwest"),
    ("Dallas", "South"),
    ("Denver", "West"),
    ("Atlanta", "Southeast"),
]

drivers = [
    "A. Rivera",
    "B. Chen",
    "C. Okafor",
    "D. Patel",
    "E. Nguyen",
    "F. Santos",
    "G. Kim",
    "H. Brooks",
    "I. Novak",
    "J. Alvarez",
]


# ---------------------------------------------------------
# Generate 20 truck records
# ---------------------------------------------------------

rows = []

for i in range(1, 21):
    make, model = random.choice(makes)
    depot, region = random.choice(depots)

    rows.append(
        (
            f"TRK-{i:03d}",
            make,
            model,
            random.choice([20000, 26000, 34000, 40000]),
            depot,
            region,
            random.choice(drivers),
        )
    )


df = spark.createDataFrame(
    rows,
    [
        "truck_id",
        "make",
        "model",
        "capacity_lbs",
        "home_depot",
        "region",
        "driver",
    ],
)


# ---------------------------------------------------------
# Idempotent reference-data load
# ---------------------------------------------------------

df.createOrReplaceTempView("truck_details_source")

spark.sql(
    f"""
    MERGE INTO `{catalog}`.`{schema}`.`truck_details` AS target
    USING truck_details_source AS source

    ON target.truck_id = source.truck_id

    WHEN MATCHED THEN UPDATE SET
        target.make = source.make,
        target.model = source.model,
        target.capacity_lbs = source.capacity_lbs,
        target.home_depot = source.home_depot,
        target.region = source.region,
        target.driver = source.driver

    WHEN NOT MATCHED THEN INSERT (
        truck_id,
        make,
        model,
        capacity_lbs,
        home_depot,
        region,
        driver
    )
    VALUES (
        source.truck_id,
        source.make,
        source.model,
        source.capacity_lbs,
        source.home_depot,
        source.region,
        source.driver
    )
    """
)

print(
    f"Successfully seeded 20 trucks into "
    f"{catalog}.{schema}.truck_details"
)
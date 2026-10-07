import argparse
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


gold_table = f"{catalog}.{schema}.gold_truck_positions"

print(f"Running migration 001 against {gold_table}")


# ---------------------------------------------------------
# Check current schema
# ---------------------------------------------------------

existing_columns = {
    field.name
    for field in spark.table(gold_table).schema.fields
}


# ---------------------------------------------------------
# Apply migration only if necessary
# ---------------------------------------------------------

if "position_updated_at" not in existing_columns:

    spark.sql(
        f"""
        ALTER TABLE `{catalog}`.`{schema}`.`gold_truck_positions`
        ADD COLUMNS (
            position_updated_at TIMESTAMP
        )
        """
    )

    print(
        f"Migration applied successfully: "
        f"added position_updated_at to {gold_table}"
    )

else:

    print(
        f"Migration already applied: "
        f"position_updated_at exists in {gold_table}"
    )
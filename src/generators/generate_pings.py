import argparse
import json
import random
import time

from datetime import datetime, timezone


parser = argparse.ArgumentParser()

parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--landing_volume", required=True)
parser.add_argument("--num_batches", type=int, default=10)
parser.add_argument("--sleep_seconds", type=int, default=1)

args = parser.parse_args()

catalog = args.catalog
schema = args.schema
landing_volume = args.landing_volume
num_batches = args.num_batches
sleep_seconds = args.sleep_seconds


# ---------------------------------------------------------
# Landing path
# ---------------------------------------------------------

landing_path = (
    f"/Volumes/{catalog}/{schema}/{landing_volume}/pings"
)

print(f"Generating GPS pings in: {landing_path}")


# ---------------------------------------------------------
# Truck population
# ---------------------------------------------------------

trucks = [
    f"TRK-{i:03d}"
    for i in range(1, 21)
]

LAT0 = 41.85
LON0 = -87.65


def make_ping(truck_id):
    return {
        "truck_id": truck_id,
        "latitude": round(
            LAT0 + random.uniform(-0.2, 0.2),
            6,
        ),
        "longitude": round(
            LON0 + random.uniform(-0.2, 0.2),
            6,
        ),
        "event_ts": datetime.now(
            timezone.utc
        ).isoformat(),
    }


# ---------------------------------------------------------
# Create landing directory
# ---------------------------------------------------------

dbutils.fs.mkdirs(landing_path)


# ---------------------------------------------------------
# Generate micro-batch files
# ---------------------------------------------------------

for batch in range(num_batches):

    rows = [
        make_ping(random.choice(trucks))
        for _ in range(random.randint(5, 15))
    ]

    # Intentional duplicate.
    if rows:
        rows.append(dict(rows[0]))

    # Intentional bad coordinate.
    bad_row = make_ping(random.choice(trucks))
    bad_row["latitude"] = None

    rows.append(bad_row)

    file_name = (
        f"{landing_path}/"
        f"pings_{int(time.time() * 1000)}_{batch}.json"
    )

    file_contents = "\n".join(
        json.dumps(row) for row in rows
    ) + "\n"

    dbutils.fs.put(
        file_name,
        file_contents,
        overwrite=True
    )

    print(
        f"Batch {batch + 1}/{num_batches}: "
        f"wrote {file_name} ({len(rows)} rows)"
    )

    time.sleep(sleep_seconds)


print("Finished generating GPS ping files.")

# Design Notes

## Approach

I chose to build the pipeline using Spark Structured Streaming, Auto Loader, and Delta Lake instead of using Lakeflow.

I wanted the main streaming behavior to be visible in the code, especially checkpointing, deduplication, `foreachBatch`, Delta MERGE, and the stream-static join.

The pipeline follows a simple Bronze → Silver → Gold design.

Bronze keeps the GPS data close to what came from the source. I intentionally don't remove duplicates or bad coordinates here because I want the original records available if I need to troubleshoot or replay the data.

Silver handles the actual cleanup. It converts the event timestamp, validates the GPS coordinates, and removes duplicate pings.

Gold joins the GPS events with `truck_details` and keeps the latest known position for each truck.

## Deduplication

The generated GPS data doesn't contain a unique event ID, so I use:

```text
truck_id + event_ts
```

as the key for identifying duplicate pings.

Duplicates are removed inside each micro-batch, and Silver is written using Delta MERGE. The MERGE also protects against inserting the same event again across different batches or retries.

If this were a production telemetry source, I would prefer to receive a unique `event_id` from the source.

## Gold / Late Events

Gold is meant to represent the current position of each truck rather than the full GPS history.

For each batch I select the latest event for a truck and MERGE it into the Gold table.

I only update an existing row when the incoming `event_ts` is newer than the current one. This means a late-arriving older GPS event can still exist in Silver without incorrectly moving the truck backwards in Gold.

## Static Data

`truck_details` is small, so I use it as the static side of a stream-static join.

The table is loaded when the Gold job starts. Since I'm using `availableNow`, the reference data is loaded again on the next pipeline run.

One tradeoff is that changing only `truck_details` will not immediately rewrite an existing Gold record. For this take-home that is acceptable. In a production implementation I would add a separate refresh/backfill process if reference changes needed to appear immediately.

## Checkpoints and Trigger

Each streaming stage has its own checkpoint.

The checkpoint paths include the environment schema, so dev, test, and prod do not share streaming state.

I used `availableNow` instead of leaving the streams running continuously. It processes all available data and then stops, which is a better fit for this take-home and Databricks Free Edition.

Auto Loader's schema location is kept separate from its streaming checkpoint.

## Environments

I use one Asset Bundle with dev, test, and prod targets.

```text
dev  -> telematics.dev
test -> telematics.test
prod -> telematics.prod
```

All three targets use the same pipeline code. The environment-specific values come from the bundle configuration.

This also keeps each environment's data, landing files, schema tracking, and checkpoints separate.

## Schema Changes

I handle schema changes as version-controlled migrations instead of manually changing tables.

As an example, migration `001` adds `position_updated_at` to the Gold table. The script checks whether the column already exists before adding it, which makes the migration safe to rerun.

I promoted the migration through:

```text
dev -> test -> prod
```

using bundle deploy and run commands.

The change is additive, so it is also backward-compatible. If there were an issue during promotion, I would stop the promotion and make a corrective migration rather than manually changing production.

## Scaling

For 100,000 trucks I would keep the same logical design but tune the compute, trigger interval, file sizes, and Delta table layout based on actual throughput.

I would also use a source-generated event ID for deduplication and add production monitoring for streaming lag, failed jobs, rejected records, throughput, and data-quality metrics.
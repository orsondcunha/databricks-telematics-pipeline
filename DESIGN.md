

The solution uses a single Databricks workspace and a single Asset Bundle
with dev, test, and prod targets.

All environments use the same source code and bundle resources. Environment
differences are supplied through bundle variables.

| Target | Catalog | Schema | Bundle Mode |
|--------|---------|--------|-------------|
| dev | telematics | dev | development |
| test | telematics | test | development |
| prod | telematics | prod | production |

Each target has an isolated Unity Catalog schema, landing volume path,
Auto Loader schema location, and Structured Streaming checkpoint location.

This prevents streaming state or data from one environment from affecting
another environment while allowing the same implementation to be promoted
through dev, test, and prod.
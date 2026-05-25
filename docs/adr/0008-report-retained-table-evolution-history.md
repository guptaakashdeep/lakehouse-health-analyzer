# Report Retained Table Evolution History

The dashboard should include table evolution history for schema and property changes, but it should be described as retained history rather than a complete audit log. The analysis core may compare retained Iceberg metadata files, schemas, properties, snapshot logs, and metadata logs where available, and should attach calculation warnings when metadata history was pruned or older files cannot be inspected.

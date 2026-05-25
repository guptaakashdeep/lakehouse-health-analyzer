# Use Table Format Adapters

The first serious version will be Iceberg-focused, but the analysis core will expose a narrow table-format adapter boundary. Iceberg should receive deep, correct support first; future Delta or Hudi adapters can produce the same table health report contract where metrics are meaningful and report unsupported values as unknown with calculation warnings.

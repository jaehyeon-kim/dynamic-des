# Data Lake Ingestion & Storage

When running long-term historical simulations or predicting future system states, streaming data to live message brokers like Kafka is often overkill. Instead, you'll want to ingest the vast amounts of generated data directly into a Data Lake (like AWS S3 or Databricks) for downstream analytics.

Dynamic DES provides robust, enterprise-grade storage connectors (`ParquetStorageEgress` and `JsonlStorageEgress`) designed specifically to handle high-velocity I/O, parallel processing compatibility, and schema stability.

## PyArrow VFS Integration

Dynamic DES natively wraps the `pyarrow.fs` Virtual File System. This allows you to hot-swap your target destination from your local hard drive to cloud object storage without changing your core application logic.

You simply inject your configured PyArrow filesystem into the connector.

### AWS S3 Example
```python
from pyarrow import fs
from dynamic_des import ParquetStorageEgress

# Configure standard AWS credentials
s3 = fs.S3FileSystem(
    access_key="<YOUR_AWS_ACCESS_KEY>",
    secret_key="<YOUR_AWS_SECRET_KEY>",
    region="us-east-1"
)

# Pass the filesystem to the egress connector
egress = ParquetStorageEgress(
    default_path="my-data-lake-bucket/simulation_runs/events.parquet",
    filesystem=s3
)
```

### Google Cloud Storage (GCS) Example
```python
from pyarrow import fs
from dynamic_des import JsonlStorageEgress

gcs = fs.GcsFileSystem(anonymous=False)

egress = JsonlStorageEgress(
    default_path="my-gcp-bucket/simulation_runs/events.jsonl",
    filesystem=gcs
)
```

## Automatic UUID Chunking

Massive parallel processing engines like AWS Athena, Databricks, or Presto work best when reading from a partitioned directory of multiple files, rather than one massive monolithic file.

Furthermore, if the simulation was writing to a single file, network interruptions or crashes could corrupt the entire dataset. To solve this, Dynamic DES implements an **Automatic Chunking Strategy**.

Every time the internal buffer flushes (based on your `batch_size`), the egress connector generates a uniquely named chunk by injecting a short UUID into the filename.

```text
# What you specify in code:
default_path = "data/events.parquet"

# What gets written to S3 over time:
data/events_a1b2c3d4.parquet
data/events_f5e6d7c8.parquet
data/events_9b8a7c6d.parquet
```

This guarantees **lock-free** parallel writing, ensuring no file overwrite collisions occur.

## Strict Schema Drift Prevention (Parquet)

Parquet is a highly optimized, columnar binary format. Unlike JSON, its schema is incredibly strict. If your first batch of data dictates that a column named `utilization` is a `FLOAT64`, but a later batch attempts to write the integer `100` into that column, standard file writers will crash or create incompatible files that break your Data Lake.

The `ParquetStorageEgress` connector protects you from this via **Schema Caching and Inference**:

1. **Inference:** On the very first flush to a specific path, the connector examines the batch and infers the canonical PyArrow Schema. It caches this schema in memory.
2. **Enforcement:** For every subsequent batch destined for that path, the connector compares the incoming data against the cached schema.
3. **Casting:** If it detects a mismatch (e.g., an `int` attempting to enter a `float` column), it automatically up-casts the data to match the canonical schema before writing it to storage.

This allows your simulation logic to remain dynamic and Pythonic, while guaranteeing your Data Lake datasets remain perfectly structured.

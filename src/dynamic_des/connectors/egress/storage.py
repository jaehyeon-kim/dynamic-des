import asyncio
import logging
import posixpath
import queue
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import orjson

from dynamic_des.connectors.egress.base import BaseEgress, group_rows

logger = logging.getLogger(__name__)


def _generate_chunk_filename(base_path: str) -> str:
    """
    Injects a short UUID into a filepath to create unique batch chunks.

    This ensures that continuous batch writes do not overwrite each other
    and allows for lock-free parallel writing to object storage systems.

    Args:
        base_path (str): The target file path (e.g., 'data/events.parquet').

    Returns:
        str: The chunked file path (e.g., 'data/events_a1b2c3d4.parquet').
    """
    path_obj = Path(base_path)
    chunk_id = uuid.uuid4().hex[:8]
    return f"{path_obj.parent / path_obj.stem}_{chunk_id}{path_obj.suffix}"


# The `type` values a filesystem mapping can name, and the PyArrow class each builds.
_FILESYSTEM_TYPES = {"local": "LocalFileSystem", "s3": "S3FileSystem"}


def _check_filesystem(filesystem: Any) -> None:
    """
    Rejects a filesystem mapping with an unknown `type` before the run starts.

    Args:
        filesystem (Any): The `filesystem` argument of a storage egress.

    Raises:
        ValueError: If a mapping names a type other than `local` or `s3`.
    """
    if isinstance(filesystem, Mapping):
        kind = str(filesystem.get("type", "local")).lower()
        if kind not in _FILESYSTEM_TYPES:
            raise ValueError(
                f"Unknown filesystem type '{kind}'; the supported types are "
                f"{', '.join(_FILESYSTEM_TYPES)}"
            )


def _build_filesystem(filesystem: Any, fs: Any) -> Any:
    """
    Returns the PyArrow FileSystem a storage egress writes through.

    Args:
        filesystem (Any): None for the local disk, a PyArrow FileSystem, which is
            returned as it is, or a mapping. A mapping's `type` (`local`, the
            default, or `s3`) picks the class, and its other keys are passed to that
            class's constructor, so an S3 mapping takes `S3FileSystem` arguments
            such as `endpoint_override`, `access_key`, `secret_key`, `region` and
            `scheme`. An `endpoint_override` that starts with `http://` or
            `https://` also sets `scheme`, unless `scheme` is given.
        fs (Any): Injected reference to the `pyarrow.fs` module.

    Returns:
        Any: The FileSystem instance.
    """
    if filesystem is None:
        return fs.LocalFileSystem()
    if not isinstance(filesystem, Mapping):
        return filesystem
    _check_filesystem(filesystem)
    options = dict(filesystem)
    kind = str(options.pop("type", "local")).lower()
    endpoint = options.get("endpoint_override")
    if kind == "s3" and isinstance(endpoint, str) and "://" in endpoint:
        scheme, _, address = endpoint.partition("://")
        options["endpoint_override"] = address
        options.setdefault("scheme", scheme)
    return getattr(fs, _FILESYSTEM_TYPES[kind])(**options)


def _ensure_folder(filesystem: Any, file_path: str, created: set) -> None:
    """
    Creates the folder that holds `file_path`, once per folder.

    A local disk needs the folder before a file can be written into it. On S3 this
    creates the prefix. If the folder cannot be created the write still goes ahead,
    because object storage accepts a key under a prefix that was never created, and
    a local write then fails with the underlying error.

    Args:
        filesystem (Any): The PyArrow FileSystem written through.
        file_path (str): The file about to be written.
        created (set): The folders already handled by this writer.
    """
    folder = posixpath.dirname(file_path)
    if not folder or folder in created:
        return
    created.add(folder)
    try:
        filesystem.create_dir(folder, recursive=True)
    except OSError as error:
        logger.warning(f"Could not create the folder '{folder}': {error}")


class JsonlStorageEgress(BaseEgress):
    """
    High-throughput JSONL batch writer for local file systems or object storage.

    This connector utilizes PyArrow's Virtual File System (VFS) to seamlessly
    write data to local disks or S3-compatible storage such as AWS S3 or SeaweedFS.
    It implements an enterprise-grade chunking strategy, generating a uniquely
    named file for every batch to prevent file locking and ensure crash resilience.

    By default, events are appended to the `default_path` as flat rows: each
    event's `value` mapping is unpacked into top-level fields, and telemetry is left
    out. If a `path_router` callable is provided, destination logic is delegated to
    that function, allowing for advanced multiplexing (e.g., splitting logs vs.
    errors) or dynamically dropping specific records by returning `None`. Records
    are then written as the router leaves them.

    Attributes:
        default_path (Optional[str]): The fallback destination path if no router is provided.
        filesystem (Optional[Any]): A PyArrow `FileSystem` instance, or the mapping it
            is built from when the run starts. Defaults to local disk.
        path_router (Optional[Callable]): Optional logic to dynamically route or drop payloads.

    Examples:
        Routing errors to a specific file and dropping meaningless metrics:

        ```python
        def custom_log_router(data: dict) -> str | None:
            # Drop lag metrics
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None

            # Route errors to a separate JSONL file
            if data.get("status") == "error":
                return "data/error_logs.jsonl"

            return "data/standard_logs.jsonl"

        egress = JsonlStorageEgress(path_router=custom_log_router)
        ```
    """

    def __init__(
        self,
        default_path: Optional[str] = None,
        filesystem: Optional[Any] = None,
        path_router: Optional[Callable[[dict], Optional[str]]] = None,
    ):
        """
        Initializes the JsonlStorageEgress with routing and VFS settings.

        Args:
            default_path: The target file path prefix (e.g., "data/logs.jsonl"),
                used when no router is given. Only events are written there, with
                their `value` mapping unpacked into top-level fields.
            filesystem: An instantiated PyArrow FileSystem (e.g., `fs.S3FileSystem()`),
                or a mapping such as `{"type": "s3", "endpoint_override": ...}` that
                is built into one when the run starts. If None, defaults to
                `fs.LocalFileSystem()`. The destination folder is created on the
                first write to it.
            path_router: A function taking a dict payload and returning a string path,
                or None to drop the record.
        """
        _check_filesystem(filesystem)
        self.default_path = default_path
        self.filesystem = filesystem
        self.path_router = path_router
        self._folders: set = set()

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and writes JSONL chunks.

        This loop continuously polls the internal `egress_queue` for data batches.
        When a batch is received, it offloads the synchronous PyArrow file I/O operations
        to a background thread to prevent blocking the asyncio event loop.

        Args:
            egress_queue: A thread-safe queue containing batches of dictionaries
                generated by the environment's EgressMixIn.

        Raises:
            ImportError: If the 'pyarrow' package is not installed.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`,
            ensuring background file writers have completed before shutting down.
        """
        try:
            from pyarrow import fs
        except ImportError:
            raise ImportError("pyarrow is required. pip install dynamic-des[parquet]")

        self.filesystem = _build_filesystem(self.filesystem, fs)
        batches_processed = 0

        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                    self.active_tasks = getattr(self, "active_tasks", 0) + 1
                    try:
                        await asyncio.to_thread(self._write_batch, batch)
                    finally:
                        self.active_tasks -= 1
                    # Log a progress heartbeat every 10 batches
                    batches_processed += 1
                    if batches_processed % 10 == 0:
                        q_size = egress_queue.qsize()
                        logger.info(
                            f"Parquet Writer: Processed {batches_processed} batches. "
                            f"(~{q_size} batches waiting in queue)"
                        )
                except queue.Empty:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            # This triggers during env.teardown()
            logger.info(
                f"JsonlStorageEgress shut down requested. "
                f"Successfully wrote {batches_processed} total chunks to storage."
            )

    def _write_batch(self, batch: list):
        """
        Groups a batch of records by destination path and writes them to chunked files.

        Args:
            batch (list): A list of dictionaries or Pydantic models to be written.
        """
        if self.filesystem is None:
            logger.error("Filesystem not initialized. Batch dropped.")
            return

        grouped_batches = group_rows(batch, self.path_router, self.default_path)

        for target_path, records in grouped_batches.items():
            chunk_path = _generate_chunk_filename(target_path)
            _ensure_folder(self.filesystem, chunk_path, self._folders)
            with self.filesystem.open_output_stream(chunk_path) as stream:
                for payload in records:
                    stream.write(
                        orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE)
                    )


class ParquetStorageEgress(BaseEgress):
    """
    High-performance columnar Parquet writer for Data Lake ingestion patterns.

    This connector buffers simulation records and converts them into heavily
    compressed Parquet tables using `pyarrow`. It natively supports writing to
    local disks or S3-compatible object stores (AWS S3, SeaweedFS) via PyArrow's VFS.

    To support massive parallel processing (e.g., Athena, Databricks), it
    implements file rotation (chunking), creating a uniquely named Parquet
    part-file for every flush of the buffer. Schemas are inferred from the
    first batch of data and strictly enforced on subsequent batches to prevent
    schema drift across chunks.

    Without a `path_router`, events are written to `default_path` as flat rows:
    each event's `value` mapping becomes columns, and telemetry is left out. With a
    router, records are written as the router leaves them.

    Attributes:
        default_path (Optional[str]): The fallback destination path.
        filesystem (Optional[Any]): A PyArrow `FileSystem` instance, or the mapping it
            is built from when the run starts. Defaults to local.
        path_router (Optional[Callable]): Optional logic to dynamically route or drop payloads.
        schemas (Dict[str, Any]): Internal registry of inferred PyArrow schemas per file path.

    Examples:
        Splitting simulation events and telemetry into distinct Parquet datasets directly to S3:

        ```python
        from pyarrow import fs

        def datalake_router(data: dict) -> str | None:
            if data.get("stream_type") == "telemetry":
                return "simulation_data/telemetry.parquet"
            return "simulation_data/events.parquet"

        # Connect directly to SeaweedFS or AWS S3
        s3 = fs.S3FileSystem(endpoint_override="localhost:8333", scheme="http")

        egress = ParquetStorageEgress(
            filesystem=s3,
            path_router=datalake_router
        )
        ```
    """

    def __init__(
        self,
        default_path: Optional[str] = None,
        filesystem: Optional[Any] = None,
        path_router: Optional[Callable[[dict], Optional[str]]] = None,
    ):
        """
        Initializes the ParquetStorageEgress with routing and VFS settings.

        Args:
            default_path: The target file path prefix (e.g., "data/events.parquet"),
                used when no router is given. Only events are written there, with
                their `value` mapping unpacked into columns.
            filesystem: An instantiated PyArrow FileSystem (e.g., `fs.S3FileSystem()`),
                or a mapping such as `{"type": "s3", "endpoint_override": ...}` that
                is built into one when the run starts. If None, defaults to
                `fs.LocalFileSystem()`. The destination folder is created on the
                first write to it.
            path_router: A function taking a dict payload and returning a string path,
                or None to drop the record.
        """
        _check_filesystem(filesystem)
        self.default_path = default_path
        self.filesystem = filesystem
        self.path_router = path_router
        self._folders: set = set()
        self.schemas: Dict[str, Any] = {}

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and writes Parquet chunks.

        This loop continuously polls the internal `egress_queue` for data batches.
        When a batch is received, it offloads the synchronous PyArrow table conversion
        and file I/O operations to a background thread to prevent blocking the asyncio loop.

        Args:
            egress_queue: A thread-safe queue containing batches of dictionaries
                generated by the environment's EgressMixIn.

        Raises:
            ImportError: If the 'pyarrow' package is not installed.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`,
            ensuring the final Parquet table has been fully flushed to disk.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            from pyarrow import fs
        except ImportError:
            raise ImportError("pyarrow is required. pip install dynamic-des[parquet]")

        self.filesystem = _build_filesystem(self.filesystem, fs)
        batches_processed = 0

        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                    self.active_tasks = getattr(self, "active_tasks", 0) + 1
                    try:
                        await asyncio.to_thread(self._write_batch, batch, pa, pq)
                    finally:
                        self.active_tasks -= 1
                    # Log a progress heartbeat every 10 batches
                    batches_processed += 1
                    if batches_processed % 10 == 0:
                        q_size = egress_queue.qsize()
                        logger.info(
                            f"Parquet Writer: Processed {batches_processed} batches. "
                            f"(~{q_size} batches waiting in queue)"
                        )
                except queue.Empty:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            # This triggers during env.teardown()
            logger.info(
                f"ParquetStorageEgress shut down requested. "
                f"Successfully wrote {batches_processed} total chunks to storage."
            )

    def _write_batch(self, batch: list, pa: Any, pq: Any):
        """
        Groups a batch of records, enforces strict schema typing, and writes Parquet chunks.

        This method dynamically infers the PyArrow schema from the first chunk of data
        destined for a specific path. It caches this schema and safely casts all
        subsequent chunks to match, preventing pipeline-breaking schema drift
        (e.g., if a float unexpectedly arrives as an integer in a later batch).

        Args:
            batch (list): A list of dictionaries or Pydantic models to be written.
            pa (Any): Injected reference to the `pyarrow` module.
            pq (Any): Injected reference to the `pyarrow.parquet` module.
        """
        grouped_batches = group_rows(batch, self.path_router, self.default_path)

        for target_path, records in grouped_batches.items():
            if not self.filesystem:
                continue

            table = pa.Table.from_pylist(records)

            # Infer schema on first batch and cache it
            if target_path not in self.schemas:
                self.schemas[target_path] = table.schema

            # Cast current batch to match the canonical schema
            expected_schema = self.schemas[target_path]
            if table.schema != expected_schema:
                table = table.cast(expected_schema)

            chunk_path = _generate_chunk_filename(target_path)
            _ensure_folder(self.filesystem, chunk_path, self._folders)

            pq.write_table(table, chunk_path, filesystem=self.filesystem)

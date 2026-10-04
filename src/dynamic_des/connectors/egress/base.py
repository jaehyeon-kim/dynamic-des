import queue
from typing import Any, Callable, Dict, List, Optional


def extract_dict(data: Any) -> dict:
    """
    Helper to seamlessly extract dicts from Pydantic V1/V2 objects.

    Args:
        data (Any): A raw dictionary or a Pydantic model.

    Returns:
        dict: The extracted dictionary representation of the data.
    """
    if isinstance(data, dict):
        return data
    if hasattr(data, "model_dump"):
        return data.model_dump(mode="json")
    if hasattr(data, "dict"):
        return data.dict()
    return data


def flatten_record(record: dict) -> dict:
    """
    Returns a copy of `record` with an event's `value` mapping merged into it.

    Storage sinks use this to give each payload field its own column. It returns a
    copy because every egress provider receives the same dictionary objects, so
    changing the record in place would also change what another sink writes. A
    payload field with the same name as a record field replaces it.

    Args:
        record (dict): A published record.

    Returns:
        dict: The flat row. A record whose `value` is not a mapping is copied as it is.
    """
    value = record.get("value")
    if not isinstance(value, dict):
        return dict(record)
    row = {key: item for key, item in record.items() if key != "value"}
    row.update(value)
    return row


def default_row(data: Any) -> Optional[dict]:
    """
    Returns the row a storage sink writes for a record when no router is given.

    Telemetry records are left out, and an event's `value` mapping is unpacked into
    columns. This is what the routers in the examples did by hand, so a sink
    configured with only a path or a table name writes one flat row per event.

    Args:
        data (Any): A published record, as a dictionary or a Pydantic model.

    Returns:
        Optional[dict]: The flat row, or None for a telemetry record.
    """
    record = extract_dict(data)
    if record.get("stream_type") == "telemetry":
        return None
    return flatten_record(record)


def group_rows(
    batch: list,
    router: Optional[Callable[[Any], Optional[str]]],
    default_target: Optional[str],
) -> Dict[str, List[dict]]:
    """
    Groups a batch into the rows each destination receives.

    With a router, the router names the destination of each record, or returns None
    to drop it, and the record is written as the router left it. Without one, every
    row goes to `default_target` in the shape `default_row` gives it.

    Args:
        batch (list): The published records, as dictionaries or Pydantic models.
        router (Optional[Callable]): The caller's routing function, if any.
        default_target (Optional[str]): The destination used when there is no router.

    Returns:
        Dict[str, List[dict]]: The rows per destination, in the order they arrived.
    """
    grouped: Dict[str, List[dict]] = {}
    for data in batch:
        if router is not None:
            target = router(data)
            if not target:
                continue
            row = extract_dict(data)
        else:
            if not default_target:
                continue
            target = default_target
            flat = default_row(data)
            if flat is None:
                continue
            row = flat
        grouped.setdefault(target, []).append(row)
    return grouped


class BaseEgress:
    """
    Base class for all egress providers in the simulation.

    Egress providers act as asynchronous bridges that consume processed
    simulation data (telemetry and events) from a thread-safe internal queue
    and transmit it to external destinations such as Kafka, databases,
    or the console.
    """

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Listens to the internal queue and pushes data to an external sink.

        This method should contain an asynchronous loop that polls the
        provided queue and handles the networking/I/O logic specific
        to the destination system.

        Args:
            egress_queue: A thread-safe queue containing batches of
                dictionaries to be exported.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        raise NotImplementedError("Subclasses must implement the run method.")

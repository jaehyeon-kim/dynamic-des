"""Python for hybrid.yaml: a payload, a process with arguments and a predicate."""


def grade_part(task_id: int, context):
    """Grades each part from the shared random generator, so a seed repeats it."""
    grade = "A" if context.sampler.rng.random() < 0.9 else "B"
    return {"status": "finished", "part_id": task_id, "grade": grade}


def maintenance(context, resource: str, every: float, duration: float):
    """Waits `every` seconds, then takes the machine out of service for `duration`."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        context.env.publish_event(f"{resource}-maintenance", {"status": "down"})
        yield context.env.timeout(duration)
        registry.update(path, normal)
        context.env.publish_event(f"{resource}-maintenance", {"status": "up"})


def events_only(record: dict) -> bool:
    """Egress predicate: keeps events and drops telemetry."""
    return record["stream_type"] == "event"

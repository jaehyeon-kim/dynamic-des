from dynamic_des.connectors.admin.kafka import KafkaAdminConnector


def test_admin_process_telemetry():
    """Verify the admin connector properly routes telemetry messages to _vitals."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    data = {
        "path_id": "Line_A.lathe.utilization",
        "value": 99.9,
        "timestamp": "2023-01-01T00:00:00",
    }
    admin._process_message(data)
    assert admin.get_vitals()["Line_A.lathe.utilization"] == 99.9


def test_admin_process_events_and_pruning():
    """Verify the admin connector routes events and properly prunes old tasks."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092", max_tasks=2)

    # Send 3 tasks for the same service
    for i in range(3):
        data = {
            "key": f"task-{i}",
            "value": {"path_id": "Line_A.service.milling", "status": "started"},
            "timestamp": f"2023-01-01T00:00:0{i}",
        }
        admin._process_message(data)

    state = admin.get_state()
    service_tasks = state["Line_A"]["milling"]

    # Since max_tasks is 2, task-0 should have been pruned to prevent memory leaks
    assert len(service_tasks) == 2
    assert "task-0" not in service_tasks
    assert "task-1" in service_tasks
    assert "task-2" in service_tasks

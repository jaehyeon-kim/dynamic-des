import asyncio
from datetime import datetime

from nicegui import app, ui

from dynamic_des import KafkaAdminConnector

# 1. Initialize the Admin Connector
# We use max_tasks=200 for a rolling window of cycle time calculations.
# auto_offset_reset='earliest' is used in collect_data to build state immediately.
admin = KafkaAdminConnector(bootstrap_servers="localhost:9092", max_tasks=200)

# Local UI State for binding
ui_state = {
    "capacity": "0",
    "in_use": "0",
    "queue": "0",
    "util": "0%",
    "avg": "0.00s",
    "min": "0.00s",
    "max": "0.00s",
    "last_sync": "Waiting for data...",
}


def calculate_cycle_metrics():
    """Aggregates finished tasks to calculate cycle time KPIs."""
    state = admin.get_state()
    durations = []

    # Hierarchy: sim_id -> service -> task_id -> {status: timestamp}
    for sim_id in state.values():
        for service in sim_id.values():
            for task in service.values():
                if "queued" in task and "finished" in task:
                    q = datetime.fromisoformat(task["queued"])
                    f = datetime.fromisoformat(task["finished"])
                    durations.append((f - q).total_seconds())

    if durations:
        ui_state["avg"] = f"{sum(durations) / len(durations):.2f}s"
        ui_state["min"] = f"{min(durations):.2f}s"
        ui_state["max"] = f"{max(durations):.2f}s"

    return len(durations)


def update_vitals_display():
    v = admin.get_vitals()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] UI Refresh | Vitals Keys: {list(v.keys())}"
    )

    # Match the exact strings from your Kafka payload!
    ui_state["capacity"] = str(v.get("Line_A.lathe.capacity", 0))
    ui_state["in_use"] = str(v.get("Line_A.lathe.in_use", 0))
    ui_state["queue"] = str(v.get("Line_A.lathe.queue_length", 0))

    util = v.get("Line_A.lathe.utilization", 0)
    ui_state["util"] = f"{util:.1f}%"
    ui_state["last_sync"] = datetime.now().strftime("%H:%M:%S")


@ui.page("/")
async def index():
    # Page Styling
    ui.query("body").style("background-color: #f3f4f6")

    with ui.header().classes("bg-teal-800 items-center justify-between"):
        ui.label("Dynamic DES | Digital Twin Monitor").classes(
            "text-white text-xl font-bold"
        )
        ui.label().bind_text_from(
            ui_state, "last_sync", backward=lambda v: f"Sync: {v}"
        ).classes("text-sm")

    # --- SECTION 1: CONTROL & TELEMETRY ---
    with ui.row().classes("w-full gap-4 p-4"):
        # Control Card
        with ui.card().classes("w-72 p-6 shadow-lg"):
            ui.label("SIMULATION CONTROL").classes(
                "text-xs font-bold text-gray-400 mb-2"
            )
            ui.label("Lathe Capacity").classes("text-sm font-semibold")
            num_input = ui.number(value=1, format="%d").classes("w-full text-xl")

            async def apply_change():
                val = int(num_input.value)
                print(
                    f"DASHBOARD: Sending update Line_A.resources.lathe.current_cap -> {val}"
                )
                await admin.send_config(
                    "sim-config", "Line_A.resources.lathe.current_cap", val
                )
                ui.notify(f"Requested Capacity: {val}", type="positive")

            ui.button("APPLY CHANGE", on_click=apply_change).classes(
                "w-full mt-4 bg-teal-600"
            )

        # Vitals Row
        def vital_card(label, key):
            with ui.card().classes("w-40 p-4 shadow-md"):
                ui.label(label).classes("text-xs text-gray-500 font-bold uppercase")
                ui.label().classes("text-3xl font-mono text-teal-900").bind_text_from(
                    ui_state, key
                )

        vital_card("Max Capacity", "capacity")
        vital_card("In Use", "in_use")
        vital_card("Queue", "queue")
        vital_card("Utilization", "util")

    # --- SECTION 2: PERFORMANCE KPIs ---
    ui.label("CYCLE TIME PERFORMANCE").classes(
        "text-sm font-bold text-gray-400 px-6 mt-4"
    )
    with ui.row().classes("w-full gap-6 p-4 px-6"):

        def kpi_card(label, key, bg_color):
            with ui.card().classes(f"w-64 p-6 shadow-md {bg_color}"):
                ui.label(label).classes("text-xs font-bold uppercase text-gray-600")
                ui.label().classes("text-5xl font-black").bind_text_from(ui_state, key)

        kpi_card("Average", "avg", "bg-white border-l-8 border-teal-500")
        kpi_card("Minimum", "min", "bg-white")
        kpi_card("Maximum", "max", "bg-white")

    # Dashboard Refresh Loop (Every 2 seconds)
    def refresh():
        update_vitals_display()
        calculate_cycle_metrics()

    ui.timer(2.0, refresh)


# 4. Background Task Launcher
async def start_kafka_monitor():
    print("DEBUG: Dashboard attempting to connect to Kafka...")
    try:
        # Use 'earliest' to ensure we capture current state on startup
        await admin.collect_data(
            ["sim-telemetry", "sim-events"], auto_offset_reset="earliest"
        )
    except Exception as e:
        print(f"DEBUG ERROR: Kafka Collection Failed: {e}")


app.on_startup(lambda: asyncio.create_task(start_kafka_monitor()))

# show=False prevents browser snap errors on Linux
ui.run(title="Digital Twin Monitor", port=8080, show=False)

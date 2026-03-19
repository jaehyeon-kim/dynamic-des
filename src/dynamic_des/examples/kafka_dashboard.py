import asyncio
from collections import deque
from datetime import datetime

from nicegui import app, ui

from dynamic_des import KafkaAdminConnector

# Initialize Admin Connector Globally (Safe to share across clients)
admin = KafkaAdminConnector(bootstrap_servers="localhost:9092", max_tasks=200)

app.on_startup(
    lambda: asyncio.create_task(
        admin.collect_data(["sim-telemetry", "sim-events"], "earliest")
    )
)


def run():
    # UI Layout
    @ui.page("/")
    async def index():
        # --- ISOLATED SESSION STATE ---
        # By moving state inside the page, every browser tab gets its own clean copy
        # preventing chart duplication and frozen UI updates.
        ui_state = {
            "selected_resource": None,
            "capacity": 0,
            "max_capacity": 10,
            "in_use": 0,
            "queue": 0,
            "util": 0.0,
            "avg": "0.00s",
            "min": "0.00s",
            "max": "0.00s",
            "throughput": "0.0 /min",
            "last_sync": "Waiting for simulation...",
        }
        chart_history = {"times": [], "queue": [], "avg": []}
        seen_tasks = deque(maxlen=500)

        # Main Layout
        ui.query("body").style("background-color: #f1f5f9")

        # Header
        with ui.header().classes("bg-slate-900 justify-center"):
            with ui.row().classes("w-full max-w-7xl items-center justify-between px-4"):
                ui.label("Dynamic DES | Real-time Capacity Simulator").classes(
                    "text-white text-xl font-bold"
                )
                ui.label().bind_text_from(
                    ui_state, "last_sync", backward=lambda v: f"Sync: {v}"
                ).classes("text-sm text-gray-400")

        # Main Grid - Centered 1280px
        with ui.column().classes("w-full max-w-7xl mx-auto p-6 gap-6"):
            # --- TOP ROW: INPUTS & VITALS ---
            with ui.row().classes("w-full justify-between items-stretch"):
                # Control Card
                with ui.card().classes(
                    "w-58 h-40 p-4 shadow-sm border-t-4 border-blue-500 items-center justify-center text-center"
                ):
                    ui.label("CAPACITY CONTROL").classes(
                        "text-xs font-bold text-gray-400"
                    )
                    cap_input = ui.number(
                        value=1, format="%d", min=1, max=ui_state["max_capacity"]
                    ).classes("w-full text-center")

                    async def apply():
                        if ui_state["selected_resource"]:
                            path = f"{ui_state['selected_resource'].split('.')[0]}.resources.{ui_state['selected_resource'].split('.')[1]}.current_cap"
                            await admin.send_config(
                                "sim-config", path, int(cap_input.value)
                            )
                            ui.notify(f"Updated: {cap_input.value}")

                    ui.button("APPLY", on_click=apply).props("small").classes(
                        "w-full bg-blue-600 text-white text-xs"
                    )

                # Vitals Cards
                def v_card(title, key):
                    with ui.card().classes(
                        "w-58 h-40 p-4 items-center justify-center shadow-sm text-center"
                    ):
                        ui.label(title).classes(
                            "text-xs font-bold text-gray-400 uppercase mb-1"
                        )
                        ui.label().classes(
                            "text-5xl font-mono text-slate-800"
                        ).bind_text_from(ui_state, key)

                v_card("Current Cap", "capacity")
                v_card("In Use", "in_use")
                v_card("Queue", "queue")

                # Utilization
                with ui.card().classes(
                    "w-58 h-40 p-4 items-center justify-center shadow-sm text-center"
                ):
                    ui.label("UTIL (%)").classes(
                        "text-xs font-bold text-gray-400 uppercase mb-1"
                    )
                    (
                        ui.circular_progress(
                            value=0, max=100, show_value=True, size="70px"
                        )
                        .props("thickness=0.2")
                        .bind_value_from(ui_state, "util")
                    )

            # --- MIDDLE ROW: KPIs, CHART, FEED ---
            with ui.row().classes("w-full gap-4 items-stretch"):
                # Stacked KPIs
                with ui.column().classes("w-56 gap-4"):

                    def k_card(t, k, bg):
                        with ui.card().classes(
                            f"w-full h-[104px] p-4 {bg} shadow-sm justify-center"
                        ):
                            ui.label(t).classes(
                                "text-xs font-bold text-gray-500 uppercase"
                            )
                            ui.label().classes("text-2xl font-black").bind_text_from(
                                ui_state, k
                            )

                    k_card("Throughput", "throughput", "bg-blue-50")
                    k_card("Avg Cycle", "avg", "bg-teal-50")
                    k_card("Max Cycle", "max", "bg-white")

                # Chart
                with ui.card().classes("flex-grow p-4 shadow-sm h-[344px]"):
                    ui.label("REAL-TIME TRENDS").classes(
                        "text-xs font-bold text-gray-400"
                    )
                    trend_chart = ui.echart(
                        {
                            "tooltip": {"trigger": "axis"},
                            "legend": {"data": ["Queue", "Avg (s)"], "bottom": 0},
                            "grid": {"top": 30, "bottom": 60, "left": 40, "right": 40},
                            "xAxis": {"type": "category", "data": []},
                            "yAxis": [
                                {"type": "value", "name": "Items"},
                                {"type": "value", "name": "Sec", "position": "right"},
                            ],
                            "series": [
                                {
                                    "name": "Queue",
                                    "type": "bar",
                                    "data": [],
                                    "itemStyle": {"color": "#f87171"},
                                },
                                {
                                    "name": "Avg (s)",
                                    "type": "line",
                                    "yAxisIndex": 1,
                                    "smooth": True,
                                    "data": [],
                                    "itemStyle": {"color": "#0d9488"},
                                },
                            ],
                            "animation": False,
                        }
                    ).classes("w-full h-full")

                # Ticker
                with ui.card().classes("w-72 p-0 shadow-sm overflow-hidden h-[344px]"):
                    ui.label("LIVE FEED").classes(
                        "text-xs font-bold text-gray-500 p-2 bg-gray-100 w-full"
                    )
                    feed = ui.log(max_lines=30).classes(
                        "w-full h-full p-2 bg-slate-900 text-emerald-400 font-mono text-[10px]"
                    )

        def process_data():
            v = admin.get_vitals()
            keys = [k for k in v.keys() if k.endswith(".capacity")]
            if not keys:
                return

            if ui_state["selected_resource"] is None:
                ui_state["selected_resource"] = keys[0].replace(".capacity", "")

            base_path = ui_state["selected_resource"]

            ui_state["capacity"] = int(v.get(f"{base_path}.capacity", 0))
            ui_state["in_use"] = int(v.get(f"{base_path}.in_use", 0))
            ui_state["queue"] = int(v.get(f"{base_path}.queue_length", 0))
            ui_state["util"] = round(float(v.get(f"{base_path}.utilization", 0.0)), 1)
            ui_state["last_sync"] = datetime.now().strftime("%H:%M:%S")

            state = admin.get_state()
            durations, finish_times = [], []
            sim_id = base_path.split(".")[0]

            if sim_id in state:
                # Add .copy() here to prevent event loop dict mutation errors
                for service_name, tasks in list(state[sim_id].items()):
                    for task_id, timestamps in list(tasks.items()):
                        if "queued" in timestamps and "finished" in timestamps:
                            q, f = (
                                datetime.fromisoformat(timestamps["queued"]),
                                datetime.fromisoformat(timestamps["finished"]),
                            )
                            duration = (f - q).total_seconds()
                            durations.append(duration)
                            finish_times.append(f)
                            if task_id not in seen_tasks:
                                seen_tasks.append(task_id)
                                feed.push(
                                    f"[{f.strftime('%H:%M:%S')}] {task_id}: {duration:.2f}s"
                                )

            if durations:
                ui_state["avg"] = f"{sum(durations) / len(durations):.2f}s"
                ui_state["min"] = f"{min(durations):.2f}s"
                ui_state["max"] = f"{max(durations):.2f}s"
                if len(finish_times) > 1:
                    span = (max(finish_times) - min(finish_times)).total_seconds()
                    ui_state["throughput"] = (
                        f"{(len(finish_times) / span) * 60:.1f} /min"
                        if span > 0
                        else "0.0 /min"
                    )

            chart_history["times"].append(ui_state["last_sync"])
            chart_history["queue"].append(ui_state["queue"])
            chart_history["avg"].append(float(ui_state["avg"].replace("s", "")))

            for k in chart_history:
                if len(chart_history[k]) > 30:
                    chart_history[k].pop(0)

            trend_chart.options["xAxis"]["data"] = chart_history["times"][:]
            trend_chart.options["series"][0]["data"] = chart_history["queue"][:]
            trend_chart.options["series"][1]["data"] = chart_history["avg"][:]
            trend_chart.update()

        # Timer is bound to the lifespan of this specific client's page
        ui.timer(2.0, process_data)

    ui.run(title="Real-time Capacity Simulator", port=8080, show=False, reload=False)

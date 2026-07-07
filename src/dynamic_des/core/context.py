import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

import numpy as np

from dynamic_des.core.environment import DynamicRealtimeEnvironment
from dynamic_des.core.sampler import Sampler
from dynamic_des.models.params import CapacityConfig, DistributionConfig, SimParameter
from dynamic_des.resources.resource import DynamicResource

logger = logging.getLogger(__name__)


class SimulationContext:
    """
    Builder and Orchestrator for Event-Driven Digital Twins.

    The `SimulationContext` implements the Builder Pattern to construct the
    foundational architecture of a Digital Twin. It acts as the central control
    plane, aggregating discrete machine capacities, continuous physical states
    (wear and tear), and statistical distributions into a unified `SimParameter`
    object.

    Architectural Guarantees:
    1.  **Immutability During Configuration**: The underlying `DynamicRealtimeEnvironment`
        and its background asynchronous I/O threads are completely deferred until
        `.run()` is explicitly invoked. This prevents partial state leaks or
        unintended clock starts.
    2.  **Centralized Switchboard**: All registered configurations are flattened
        and managed by the internal `SimulationRegistry`, allowing external
        Kafka Admin commands to mutate these states safely during runtime.
    3.  **Deterministic Stochasticity**: By exposing a central `Sampler` driven
        by a seeded NumPy generator, all background arrivals, decorators, and
        user-defined physical generators share the same random state, ensuring
        100% reproducible Digital Twin executions.

    Attributes:
        sim_id (str): The unique namespace for this simulation instance (e.g., 'HotRolling').
        factor (float): The real-time synchronization multiplier. A factor of 1.0
            syncs strictly with wall-clock time, while 0.0 executes instantly in a
            fast-forward loop (ideal for historical data processing).
        random_seed (Optional[int]): The deterministic seed for the NumPy RNG.
        sampler (Optional[Sampler]): The centralized Random Number Generator engine.
            Accessible to user-defined raw generators only after `.run()` is called.
    """

    def __init__(
        self,
        sim_id: str,
        factor: float = 1.0,
        random_seed: Optional[int] = None,
        logical_start_time: Optional[datetime] = None,
    ):
        """
        Initializes the context builder with a designated namespace and temporal factor.

        Args:
            sim_id: The unique prefix for this group of parameters. All generated
                telemetry and event paths will be prefixed with this ID.
            factor: The real-time synchronization factor. Defaults to 1.0.
            random_seed: Optional seed for deterministic execution.
            logical_start_time: Optional override for the environment's base clock,
                forwarded to `DynamicRealtimeEnvironment`. Crucial for historical
                backfilling (e.g., generating data from last week).
        """
        self.sim_id = sim_id
        self.factor = factor
        self.random_seed = random_seed
        self.logical_start_time = logical_start_time

        # Builder State (Pre-Compilation)
        self._ingress_providers: List[Any] = []
        self._egress_providers: List[Any] = []
        self._batch_size = 500
        self._flush_interval = 1.0

        self._resources_config: Dict[str, CapacityConfig] = {}
        self._containers_config: Dict[str, CapacityConfig] = {}
        self._services_config: Dict[str, DistributionConfig] = {}
        self._arrivals_config: Dict[str, DistributionConfig] = {}
        self._variables_config: Dict[str, Any] = {}

        # Runtime State (Post-Compilation)
        self._env: Optional[DynamicRealtimeEnvironment] = None
        self.sampler: Optional[Sampler] = None
        self._resources_map: Dict[str, DynamicResource] = {}
        self._startup_loops: List[Tuple[Callable, Optional[str]]] = []

    # ==========================================
    # BUILDER METHODS (Fluent API)
    # ==========================================

    def add_ingress(self, provider: Any) -> "SimulationContext":
        """
        Registers an asynchronous ingress provider to listen for external mutations.

        Args:
            provider: An initialized connector subclassing `BaseIngress`
                (e.g., `KafkaIngress`).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._ingress_providers.append(provider)
        return self

    def add_egress(self, provider: Any) -> "SimulationContext":
        """
        Registers an asynchronous egress provider for data exfiltration.

        Args:
            provider: An initialized connector subclassing `BaseEgress`
                (e.g., `KafkaEgress` or `ParquetStorageEgress`).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._egress_providers.append(provider)
        return self

    def with_batching(
        self, batch_size: int, flush_interval: float
    ) -> "SimulationContext":
        """
        Configures the internal egress buffering strategy to mitigate I/O lock contention.

        Args:
            batch_size: The maximum number of standard events to hold in memory
                before forcing an asynchronous flush to the egress thread.
            flush_interval: The maximum simulation time (in seconds) to wait
                before forcefully flushing the buffer, regardless of capacity.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        return self

    def add_resource(
        self, name: str, current_cap: int, max_cap: int
    ) -> "SimulationContext":
        """
        Registers a discrete logical resource capable of processing tasks.

        Resources act as the primary gating mechanism in the simulation, blocking
        concurrent processes to prevent physical collisions (e.g., restricting
        the number of slabs inside a mill stand).

        Args:
            name: The internal identifier (e.g., 'mill_structural').
            current_cap: The active physical token capacity at simulation start.
            max_cap: The absolute physical maximum bound of tokens allowed.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._resources_config[name] = CapacityConfig(
            current_cap=current_cap, max_cap=max_cap
        )
        return self

    def add_container(
        self, name: str, current_cap: float, max_cap: float
    ) -> "SimulationContext":
        """
        Registers a continuous state container representing fluid or gradual levels.

        Unlike Resources, Containers map to floating-point metrics. They are
        specifically utilized for modeling continuous physical states such as
        concept drift (wear and tear), fluid levels, or thermal degradation.

        Args:
            name: The internal identifier (e.g., 'wear_structural').
            current_cap: The starting floating-point level (e.g., 0.001 for a new machine).
            max_cap: The absolute maximum ceiling (e.g., 100.0 for catastrophic failure).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._containers_config[name] = CapacityConfig(
            current_cap=current_cap, max_cap=max_cap
        )
        return self

    def add_variable(self, name: str, value: Any) -> "SimulationContext":
        """
        Registers a dynamic, untyped variable accessible via the central Registry.

        Variables act as arbitrary state payloads that can be targeted by the
        Control Plane. For example, storing a JSON dictionary defining the
        velocity and frequency of a gradual wear injection loop.

        Args:
            name: The internal identifier (e.g., 'velocity_structural').
            value: The initial payload (supports primitives, lists, and dicts).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._variables_config[name] = value
        return self

    def add_service(
        self,
        name: str,
        dist: Literal["exponential", "normal", "lognormal"],
        mean: float = 0.0,
        std: float = 0.0,
        rate: float = 0.0,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining a process service duration.

        Args:
            name: The internal identifier (e.g., 'pass_roughing').
            dist: The distribution family ('normal', 'exponential', 'lognormal').
            mean: The mean (mu) for normal/lognormal distributions.
            std: The standard deviation (sigma) for normal/lognormal distributions.
            rate: The rate (lambda) for exponential distributions.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._services_config[name] = DistributionConfig(
            dist=dist, mean=mean, std=std, rate=rate
        )
        return self

    def add_arrival(
        self,
        name: str,
        dist: Literal["exponential", "normal", "lognormal"],
        rate: float = 0.0,
        mean: float = 0.0,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining the inter-arrival times of entities.

        Args:
            name: The internal identifier (e.g., 'structural').
            dist: The distribution type ('exponential', 'normal', 'lognormal').
            rate: The rate (lambda) for exponential distributions.
            mean: The mean (mu) for normal distributions.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._arrivals_config[name] = DistributionConfig(
            dist=dist, rate=rate, mean=mean
        )
        return self

    # ==========================================
    # DECORATORS (Execution Logic)
    # ==========================================

    def task(self, service_id: str, resource_id: str) -> Callable:
        """
        Transforms a standard Python function into an event-driven SimPy task.

        This decorator completely abstracts the boilerplate of resource acquisition,
        clock synchronization, and standardized telemetry lineage.

        Lifecycle Execution:
        1. Emits a strict `queued` event payload to the telemetry stream.
        2. Blocks asynchronous execution until the specified `resource_id` token is acquired.
        3. Emits a strict `started` event payload upon acquisition.
        4. Executes the wrapped user function to extract custom payload data.
        5. Samples the live `service_id` distribution and yields the temporal timeout.
        6. Emits a strict `finished` event containing the user's custom payload.

        Args:
            service_id: The ID of the configured distribution dictating the execution time.
            resource_id: The ID of the configured resource this task exclusively requires.

        Returns:
            Callable: A decorator that wraps the target function into a SimPy generator.

        Example:
            ```python
            @app.task(service_id="milling", resource_id="lathe")
            def process_part(task_id: int):
                return {"status": "success", "part_id": task_id}
            ```
        """

        def decorator(user_func):
            def wrapper(task_id: int, *args, **kwargs):
                if not self._env or not self.sampler:
                    raise RuntimeError("Simulation has not been built yet.")

                res = self._resources_map[resource_id]
                task_key = f"task-{task_id}"
                path_id = f"{self.sim_id}.service.{service_id}"

                self._env.publish_event(
                    task_key, {"path_id": path_id, "status": "queued"}
                )

                with res.request() as req:
                    yield req
                    self._env.publish_event(
                        task_key, {"path_id": path_id, "status": "started"}
                    )

                    payload = user_func(task_id, *args, **kwargs)

                    service_cfg = self._env.registry.get_config(path_id)
                    yield self._env.timeout(self.sampler.sample(service_cfg))

                    self._env.publish_event(task_key, payload)

            return wrapper

        return decorator

    def arrival_loop(self, arrival_id: str) -> Callable:
        """
        Registers an infinite generator loop to automatically start on boot.

        The wrapped function will be injected with the `SimulationContext`
        instance at runtime, granting it access to `context.wait_for_arrival()`.

        Args:
            arrival_id: The ID of the configured arrival rate to dynamically sample.
        """

        def decorator(user_func):
            self._startup_loops.append((user_func, arrival_id))
            return user_func

        return decorator

    def telemetry_loop(self, interval: float) -> Callable:
        """
        Registers an infinite background daemon loop for periodic data publishing.

        This is primarily used for observing and extracting continuous state
        (like wear containers) at strict intervals.

        Args:
            interval: The simulation time (in seconds) to wait between iterations.
        """

        def decorator(user_func):
            def wrapper():
                if not self._env:
                    raise RuntimeError("Simulation has not been built yet.")
                while True:
                    user_func(self)
                    yield self._env.timeout(interval)

            self._startup_loops.append((wrapper, None))
            return user_func

        return decorator

    # ==========================================
    # RUNTIME HELPERS (For Raw Generators)
    # ==========================================

    @property
    def env(self) -> DynamicRealtimeEnvironment:
        """
        Read-only handle to the active `DynamicRealtimeEnvironment`.

        Grants raw generators access to environment primitives such as
        `publish_event`, `timeout`, `now`, and `start_datetime` without
        reaching into private builder state.

        Raises:
            RuntimeError: If accessed during the Builder Phase (before `.run()`).
        """
        if self._env is None:
            raise RuntimeError("Environment is not attached until context is run.")
        return self._env

    def wait_for_arrival(self, arrival_id: str) -> Any:
        """
        Yields a dynamic SimPy timeout based on the live arrival distribution.

        Raises:
            RuntimeError: If called during the Builder Phase (before `.run()`).
        """
        if not self._env or not self.sampler:
            raise RuntimeError("Cannot sample arrival before context is run.")

        cfg = self._env.registry.get_config(f"{self.sim_id}.arrival.{arrival_id}")
        return self._env.timeout(self.sampler.sample(cfg))

    def get_resource(self, resource_id: str) -> DynamicResource:
        """
        Retrieves an active SimPy `DynamicResource` instance for raw lock management.

        Raises:
            RuntimeError: If called during the Builder Phase.
        """
        if not self._env:
            raise RuntimeError("Resources are not instantiated until context is run.")
        return self._resources_map[resource_id]

    def publish(self, metric_name: str, value: Any) -> None:
        """
        Publishes a scalar telemetry metric to the non-blocking egress buffer.
        """
        if not self._env:
            raise RuntimeError("Cannot publish telemetry before context is run.")
        self._env.publish_telemetry(f"{self.sim_id}.{metric_name}", value)

    def spawn(self, process: Any) -> None:
        """
        Registers a new asynchronous generator with the active SimPy event loop.

        This acts as the "escape hatch" for complex logic (like physical slab
        tracking) that outgrows the strict abstraction of the `@task` decorator.
        """
        if not self._env:
            raise RuntimeError("Cannot spawn processes before context is run.")
        self._env.process(process)

    # ==========================================
    # ORCHESTRATION (The Compilation Phase)
    # ==========================================

    def run(self, until: Any = None) -> None:
        """
        Compiles the defined infrastructure architecture and triggers the simulation loop.

        Phase 1: Instantiates the foundational `DynamicRealtimeEnvironment` and `Sampler`.
        Phase 2: Compiles all builder dictionaries into a monolithic `SimParameter`
                 object and registers it with the Switchboard Registry.
        Phase 3: Connects and boots the asynchronous Kafka/Redis background threads.
        Phase 4: Hydrates physical SimPy limits based on the registry boundaries.
        Phase 5: Spawns all registered `@arrival_loop` and `@telemetry_loop` generators.
        Phase 6: Relinquishes the main thread to the SimPy environment execution clock.

        Args:
            until: The absolute simulation termination time. Can be a numeric float
                (representing base seconds) or a human-readable string parsed by
                the utility module (e.g., "1 week", "8 hours"). If None, the
                simulation runs infinitely.
        """
        logger.info(f"Building SimulationContext for '{self.sim_id}'...")

        env_kwargs: Dict[str, Any] = {"factor": self.factor}
        if self.logical_start_time is not None:
            env_kwargs["logical_start_time"] = self.logical_start_time
        self._env = DynamicRealtimeEnvironment(**env_kwargs)

        # Initializes RNG with the deterministic seed
        self.sampler = Sampler(rng=np.random.default_rng(self.random_seed))

        # Compile Master Parameter Object
        params = SimParameter(
            sim_id=self.sim_id,
            resources=self._resources_config,
            containers=self._containers_config,
            service=self._services_config,
            arrival=self._arrivals_config,
            variables=self._variables_config,
        )
        self._env.registry.register_sim_parameter(params)

        # Connect External Infrastructure
        if self._ingress_providers:
            self._env.setup_ingress(self._ingress_providers)

        if self._egress_providers:
            self._env.setup_egress(
                self._egress_providers,
                batch_size=self._batch_size,
                flush_interval=self._flush_interval,
            )

        # Hydrate Physical SimPy Resources
        for res_id in self._resources_config.keys():
            self._resources_map[res_id] = DynamicResource(
                self._env, self.sim_id, res_id
            )

        # Boot Registered Daemon Generators
        for loop_func, meta in self._startup_loops:
            if meta:
                # Arrival loops require context injection
                self._env.process(loop_func(self))
            else:
                # Telemetry loops run blindly
                self._env.process(loop_func())

        try:
            logger.info("Simulation engine started.")
            if isinstance(until, str):
                from dynamic_des.utils import time_to_seconds

                until = time_to_seconds(until)

            self._env.run(until=until)
        finally:
            self._env.teardown()

from typing import Any, Dict

from simpy import Environment, Store

from dynamic_des.models.params import SimParameter


class DynamicValue:
    """
    The atom of the registry. Holds a value and a SimPy event
    that triggers whenever the value changes.
    """

    def __init__(self, env: Environment, name: str, initial_value: Any):
        self.env = env
        self.name = name
        self._value = initial_value
        # Use a Store to signal changes (prevents missed events)
        self._signal = Store(env, capacity=1)

    @property
    def value(self):
        return self._value

    def update(self, new_value: Any):
        if self._value != new_value:
            self._value = new_value
            # Signal a change if not already signaled
            if len(self._signal.items) == 0:
                self._signal.put(True)

    def wait_for_change(self):
        """Returns a get-request for the signal."""
        return self._signal.get()


class SimulationRegistry:
    """
    A centralized 'Switchboard' that maps dot-notation paths
    to DynamicValue objects and their parent configuration objects.
    """

    def __init__(self, env: Environment):
        self.env = env
        self._values: Dict[str, DynamicValue] = {}
        self._configs: Dict[str, Any] = {}

    def get(self, path: str) -> DynamicValue:
        """Retrieve the DynamicValue object at a specific path."""
        if path not in self._values:
            raise KeyError(f"Path '{path}' not found in Simulation Registry.")
        return self._values[path]

    def get_config(self, path: str) -> Any:
        """Retrieve the live DistributionConfig or CapacityConfig object."""
        if path not in self._configs:
            raise KeyError(f"Config path '{path}' not found in Simulation Registry.")
        return self._configs[path]

    def update(self, path: str, new_value: Any):
        """Update value and synchronize the parent configuration object."""
        if path in self._values:
            # Update the DynamicValue (triggers SimPy events)
            self._values[path].update(new_value)

            # Synchronize the attribute on the original config object
            if "." in path:
                parent_path, attr = path.rsplit(".", 1)
                if parent_path in self._configs:
                    setattr(self._configs[parent_path], attr, new_value)
        else:
            print(f"Warning: Attempted to update non-existent path: {path}")

    def register_sim_parameter(self, param: SimParameter):
        """
        Takes a SimParameter instance and flattens it into the registry.
        Stores references to config objects to allow retrieval via get_config.
        """
        prefix = param.sim_id

        # Register Arrivals
        for name, config in param.arrival.items():
            path = f"{prefix}.arrival.{name}"
            self._configs[path] = config
            self._register_dist(path, config)

        # Register Service Steps
        for name, config in param.service.items():
            path = f"{prefix}.service.{name}"
            self._configs[path] = config
            self._register_dist(path, config)

        # Register Resources
        for name, config in param.resources.items():
            path = f"{prefix}.resources.{name}"
            self._configs[path] = config
            self._register_cap(path, config)

        # Register Containers
        for name, config in param.containers.items():
            path = f"{prefix}.containers.{name}"
            self._configs[path] = config
            self._register_cap(path, config)

        # Register Stores
        for name, config in param.stores.items():
            path = f"{prefix}.stores.{name}"
            self._configs[path] = config
            self._register_cap(path, config)

    def _register_dist(self, path_prefix: str, dist_config: Any):
        """Internal: Flattens a DistributionConfig."""
        if dist_config.dist == "exponential":
            self._values[f"{path_prefix}.rate"] = DynamicValue(
                self.env, f"{path_prefix}.rate", dist_config.rate
            )
        else:
            self._values[f"{path_prefix}.mean"] = DynamicValue(
                self.env, f"{path_prefix}.mean", dist_config.mean
            )
            self._values[f"{path_prefix}.std"] = DynamicValue(
                self.env, f"{path_prefix}.std", dist_config.std
            )

    def _register_cap(self, path_prefix: str, cap_config: Any):
        """Internal: Flattens a CapacityConfig."""
        self._values[f"{path_prefix}.current_cap"] = DynamicValue(
            self.env, f"{path_prefix}.current_cap", cap_config.current_cap
        )
        self._values[f"{path_prefix}.max_cap"] = DynamicValue(
            self.env, f"{path_prefix}.max_cap", cap_config.max_cap
        )

from typing import Any, Dict

from simpy import Environment

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
        # This event allows resources to 'yield' until a change occurs
        self.change_event = env.event()

    @property
    def value(self):
        return self._value

    def update(self, new_value: Any):
        if self._value != new_value:
            self._value = new_value
            # Trigger the event to wake up any 'yield'ing processes
            if not self.change_event.triggered:
                self.change_event.succeed(value=self._value)
            # Replace with a fresh event for the next update
            self.change_event = self.env.event()


class SimulationRegistry:
    """
    A centralized 'Switchboard' that maps dot-notation paths
    to DynamicValue objects.
    """

    def __init__(self, env: Environment):
        self.env = env
        self._values: Dict[str, DynamicValue] = {}

    def get(self, path: str) -> DynamicValue:
        """Retrieve the DynamicValue object at a specific path."""
        if path not in self._values:
            raise KeyError(f"Path '{path}' not found in Simulation Registry.")
        return self._values[path]

    def update(self, path: str, new_value: Any):
        """Surgically update a value via its path (e.g. 'Line_A.service.milling.mean')"""
        if path in self._values:
            self._values[path].update(new_value)
        else:
            print(f"Warning: Attempted to update non-existent path: {path}")

    def register_sim_parameter(self, param: SimParameter):
        """
        Takes a SimParameter instance and flattens it into the registry.
        Supports nested dictionaries (service/resources) and dataclasses.
        """
        prefix = param.param_id

        # 1. Register Arrival
        self._register_dist(f"{prefix}.arrival", param.arrival)

        # 2. Register Service Steps (Dictionary of DistributionConfig)
        for step_name, dist_config in param.service.items():
            self._register_dist(f"{prefix}.service.{step_name}", dist_config)

        # 3. Register Resources (Dictionary of ResourceConfig)
        for res_name, res_config in param.resources.items():
            self._register_resource(f"{prefix}.resources.{res_name}", res_config)

    def _register_dist(self, path_prefix: str, dist_config: Any):
        """Internal: Flattens a DistributionConfig."""
        if dist_config.dist == "exponential":
            self._values[f"{path_prefix}.rate"] = DynamicValue(
                self.env, f"{path_prefix}.rate", dist_config.rate
            )
        elif dist_config.dist == "normal":
            self._values[f"{path_prefix}.mean"] = DynamicValue(
                self.env, f"{path_prefix}.mean", dist_config.mean
            )
            self._values[f"{path_prefix}.std"] = DynamicValue(
                self.env, f"{path_prefix}.std", dist_config.std
            )

    def _register_resource(self, path_prefix: str, res_config: Any):
        """Internal: Flattens a ResourceConfig."""
        self._values[f"{path_prefix}.current_cap"] = DynamicValue(
            self.env, f"{path_prefix}.current_cap", res_config.current_cap
        )
        self._values[f"{path_prefix}.max_cap"] = DynamicValue(
            self.env, f"{path_prefix}.max_cap", res_config.max_cap
        )

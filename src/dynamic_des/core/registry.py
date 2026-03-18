import logging
from typing import Any, Dict

from simpy import Environment, Store

from dynamic_des.models.params import SimParameter

logger = logging.getLogger(__name__)


class DynamicValue:
    """
    The atom of the registry. Holds a value and a SimPy event that triggers whenever the value changes.

    This class is used internally by the `SimulationRegistry` to wrap configuration
    values so that SimPy processes can yield and wait for them to change.

    Attributes:
        env (Environment): The active SimPy environment.
        name (str): The dot-notation path/name of this value (e.g., 'Line_A.lathe.capacity').
    """

    def __init__(self, env: Environment, name: str, initial_value: Any):
        self.env = env
        self.name = name
        self._value = initial_value
        # Use a Store to signal changes (prevents missed events)
        self._signal = Store(env, capacity=1)

    @property
    def value(self):
        """The current underlying value."""
        return self._value

    def update(self, new_value: Any):
        """
        Updates the value and signals any waiting SimPy processes.

        Args:
            new_value (Any): The new value to store.
        """
        if self._value != new_value:
            self._value = new_value
            # Signal a change if not already signaled
            if len(self._signal.items) == 0:
                self._signal.put(True)

    def wait_for_change(self):
        """
        Returns a SimPy event that will be triggered when the value updates.

        Returns:
            simpy.events.StoreGet: A get-request for the internal signal.
        """
        return self._signal.get()


class SimulationRegistry:
    """
    A centralized 'Switchboard' that maps dot-notation paths to dynamic simulation parameters.

    The Registry acts as the single source of truth for the simulation's state. It allows
    external streams (like Kafka or Redis) to update parameters on the fly, seamlessly
    synchronizing them with the underlying SimPy processes.

    Attributes:
        env (Environment): The active SimPy environment.
    """

    def __init__(self, env: Environment):
        self.env = env
        self._values: Dict[str, DynamicValue] = {}
        self._configs: Dict[str, Any] = {}

    def get(self, path: str) -> DynamicValue:
        """
        Retrieve the `DynamicValue` object at a specific path.

        Args:
            path (str): The dot-notation path (e.g., 'Line_A.arrival.standard.rate').

        Returns:
            DynamicValue: The wrapper object for the requested parameter.

        Raises:
            KeyError: If the path does not exist in the registry.
        """
        if path not in self._values:
            raise KeyError(f"Path '{path}' not found in Simulation Registry.")
        return self._values[path]

    def get_config(self, path: str) -> Any:
        """
        Retrieve the live configuration object (e.g., `DistributionConfig`).

        Args:
            path (str): The dot-notation path (e.g., 'Line_A.service.milling').

        Returns:
            Any: The synchronized configuration object.

        Raises:
            KeyError: If the config path does not exist in the registry.
        """
        if path not in self._configs:
            raise KeyError(f"Config path '{path}' not found in Simulation Registry.")
        return self._configs[path]

    def update(self, path: str, new_value: Any):
        """
        Update a value safely and synchronize its parent configuration object.

        Includes dynamic type casting to protect the simulation from crashing if
        external systems send improperly typed data (e.g., sending the string "5"
        instead of the integer 5).

        Args:
            path (str): The dot-notation path to update.
            new_value (Any): The new value to apply.
        """
        if path in self._values:
            current_val = self._values[path].value
            validated_value = new_value

            # Type Validation & Casting
            if current_val is not None:
                expected_type = type(current_val)
                # If types don't match, attempt a safe cast (e.g., "5" -> 5)
                if not isinstance(new_value, expected_type):
                    try:
                        validated_value = expected_type(new_value)
                        logger.debug(
                            f"Cast '{new_value}' to {expected_type.__name__} for '{path}'"
                        )
                    except (ValueError, TypeError):
                        logger.error(
                            f"Type mismatch for '{path}'. Expected {expected_type.__name__}, "
                            f"got {type(new_value).__name__} with value '{new_value}'. Update ignored."
                        )
                        return  # Exit early to prevent crashing the simulation

            # Update the DynamicValue (triggers SimPy events)
            self._values[path].update(validated_value)

            # Synchronize the attribute on the original config object
            if "." in path:
                parent_path, attr = path.rsplit(".", 1)
                if parent_path in self._configs:
                    setattr(self._configs[parent_path], attr, validated_value)
        else:
            logger.warning(f"Attempted to update non-existent path: {path}")

    def register_sim_parameter(self, param: SimParameter):
        """
        Takes a `SimParameter` instance and flattens it into the registry.

        Args:
            param (SimParameter): The initial state schema to register.
        """
        prefix = param.sim_id

        # Register Arrivals
        for name, dist_config in param.arrival.items():
            path = f"{prefix}.arrival.{name}"
            self._configs[path] = dist_config
            self._register_dist(path, dist_config)

        # Register Service Steps
        for name, dist_config in param.service.items():
            path = f"{prefix}.service.{name}"
            self._configs[path] = dist_config
            self._register_dist(path, dist_config)

        # Register Resources
        for name, cap_config in param.resources.items():
            path = f"{prefix}.resources.{name}"
            self._configs[path] = cap_config
            self._register_cap(path, cap_config)

        # Register Containers
        for name, cap_config in param.containers.items():
            path = f"{prefix}.containers.{name}"
            self._configs[path] = cap_config
            self._register_cap(path, cap_config)

        # Register Stores
        for name, cap_config in param.stores.items():
            path = f"{prefix}.stores.{name}"
            self._configs[path] = cap_config
            self._register_cap(path, cap_config)

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

import simpy

from dynamic_des.core.registry import DynamicValue


class BaseDynamicResource:
    def __init__(self, env: simpy.Environment, sim_id: str, obj_id: str, category: str):
        self.env = env
        self.sim_id = sim_id
        self.obj_id = obj_id

        path = f"{sim_id}.{category}.{obj_id}"
        self._current_cap_val: DynamicValue = env.registry.get(f"{path}.current_cap")
        self._max_cap_val: DynamicValue = env.registry.get(f"{path}.max_cap")

        self.env.process(self._watch_capacity())

    def _watch_capacity(self):
        while True:
            # Wakes up immediately if a signal is in the Store
            yield self._current_cap_val.wait_for_change()
            self._handle_capacity_change(int(self._current_cap_val.value))

    def _handle_capacity_change(self, new_target: int):
        pass  # To be implemented by subclasses

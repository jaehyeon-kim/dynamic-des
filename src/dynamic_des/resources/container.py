from simpy import Environment

from dynamic_des.resources.base import BaseDynamicResource


class DynamicContainer(BaseDynamicResource):
    """
    A wrapper for SimPy Container with dynamic capacity updates.
    Planned for a future release.
    """

    def __init__(self, env: Environment, sim_id: str, container_id: str):
        super().__init__(env, sim_id, container_id, "containers")
        raise NotImplementedError("DynamicContainer is planned for a future release.")

    def _handle_capacity_change(self, new_target: int):
        pass

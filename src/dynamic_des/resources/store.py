from simpy import Environment

from dynamic_des.resources.base import BaseDynamicResource


class DynamicStore(BaseDynamicResource):
    """
    A wrapper for SimPy Store or PriorityStore with dynamic capacity updates.
    Planned for a future release.
    """

    def __init__(self, env: Environment, sim_id: str, store_id: str):
        super().__init__(env, sim_id, store_id, "stores")
        raise NotImplementedError("DynamicStore is planned for a future release.")

    def _handle_capacity_change(self, new_target: int):
        pass

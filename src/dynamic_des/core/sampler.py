from typing import Optional

import numpy as np

from dynamic_des.models.params import DistributionConfig


class Sampler:
    """Handles stochastic sampling with safety clipping and parameter conversion."""

    def __init__(self, rng: Optional[np.random.Generator] = None):
        self.rng = rng

    def sample(self, config: DistributionConfig, min_delay: float = 0.1) -> float:
        """Samples a value. Returns the mean (clipped by min_delay) if self.rng is None."""

        if config.dist == "exponential":
            scale = 1.0 / config.rate if config.rate and config.rate > 0 else 1.0
            if self.rng is None:
                return max(min_delay, scale)
            return max(min_delay, self.rng.exponential(scale))

        if config.dist == "normal":
            m = config.mean or 0.0
            if self.rng is None:
                return max(min_delay, m)
            val = self.rng.normal(m, config.std or 0.0)
            return max(min_delay, val)

        if config.dist == "lognormal":
            m, s = config.mean or 1.0, config.std or 0.1
            if self.rng is None:
                return max(min_delay, m)

            # mu/sigma conversion
            mu = np.log(m**2 / np.sqrt(s**2 + m**2))
            sigma = np.sqrt(np.log(1 + (s**2 / m**2)))
            return max(min_delay, self.rng.lognormal(mu, sigma))

        return min_delay

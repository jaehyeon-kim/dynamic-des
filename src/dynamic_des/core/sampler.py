from typing import Optional

import numpy as np

from dynamic_des.models.params import DistributionConfig


class Sampler:
    """
    Generates random numbers based on live `DistributionConfig` objects.

    The Sampler evaluates the configuration *at the exact moment* it is called.
    This allows the simulation to dynamically respond to external parameter changes.

    Attributes:
        rng (np.random.Generator): The NumPy random number generator instance.
    """

    def __init__(self, rng: Optional[np.random.Generator] = None):
        """
        Initializes the Sampler.

        Args:
            rng (np.random.Generator, optional): A seeded NumPy random generator for reproducible runs.
                If None, a default unseeded generator is used.
        """
        self.rng = rng

    def sample(self, config: DistributionConfig, min_delay: float = 0.00001) -> float:
        """
        Draws a random sample using the current parameters in the provided config.

        Args:
            config (DistributionConfig): The live configuration object retrieved from the Registry.

        Returns:
            float: A sampled float representing a time duration (e.g., arrival gap or service time).
                   Returns 0.0 if the resulting sample is negative.

        Raises:
            ValueError: If the `dist` type string is not supported.
        """

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

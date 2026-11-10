"""EdgeRush's service pods are each given the others as seeds, to join by."""

import unittest

import systems


class Seeds(unittest.TestCase):
    def test_every_pod_seeds_from_the_other_two(self):
        _, files = systems.build_edgerush(2)
        for pod, ip in enumerate(systems.SERVICE_IPS):
            others = [f'"{other}:7000"' for other in systems.SERVICE_IPS if other != ip]
            self.assertIn(f"store: {{ seeds: [{', '.join(others)}] }}\n", files[f"rl{pod}.yaml"])


if __name__ == "__main__":
    unittest.main()

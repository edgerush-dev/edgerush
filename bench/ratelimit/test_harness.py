"""The harness finds the owner of EdgeRush's one key from its service pods' scrapes."""

import unittest

from harness import owner_in

SCRAPE = """# TYPE edgerush_rate_limit_keys gauge
edgerush_rate_limit_keys {held}
# TYPE edgerush_rate_limit_keys_owned gauge
edgerush_rate_limit_keys_owned {owned}
"""


def scrape(held, owned):
    return SCRAPE.format(held=held, owned=owned)


class Owner(unittest.TestCase):
    def test_the_pod_owning_a_key_is_the_owner(self):
        self.assertEqual(owner_in([scrape(1, 0), scrape(0, 0), scrape(1, 1)]), 2)

    def test_no_pod_owning_a_key_is_refused(self):
        with self.assertRaisesRegex(ValueError, r"owned: \[0, 0, 0\]"):
            owner_in([scrape(0, 0)] * 3)

    def test_two_pods_owning_keys_are_refused(self):
        with self.assertRaisesRegex(ValueError, r"owned: \[1, 0, 1\]"):
            owner_in([scrape(1, 1), scrape(1, 0), scrape(1, 1)])

    def test_a_scrape_without_the_gauge_is_refused(self):
        with self.assertRaisesRegex(ValueError, "pod 1"):
            owner_in([scrape(1, 1), "edgerush_rate_limit_keys 1\n", scrape(0, 0)])

    def test_a_series_whose_name_only_begins_so_is_not_read(self):
        more = "edgerush_rate_limit_keys_owned_total 5\n" + scrape(1, 0)
        self.assertEqual(owner_in([more, scrape(1, 1)]), 1)


if __name__ == "__main__":
    unittest.main()

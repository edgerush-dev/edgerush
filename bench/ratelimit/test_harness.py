"""The harness finds the owner of EdgeRush's one key, and whether its service pods serve one
table, from their scrapes."""

import unittest

from harness import owner_in, table_served

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


TABLE = """# TYPE edgerush_rate_limit_pod_serving gauge
edgerush_rate_limit_pod_serving {serving}
# TYPE edgerush_rate_limit_table_term gauge
edgerush_rate_limit_table_term {term}
# TYPE edgerush_rate_limit_table_version gauge
edgerush_rate_limit_table_version {version}
"""


def table(serving=1, term=4294967298, version=7):
    return TABLE.format(serving=serving, term=term, version=version)


class Served(unittest.TestCase):
    def test_every_pod_serving_one_table_serves_it(self):
        self.assertEqual(table_served([table()] * 3), (4294967298, 7))

    def test_a_pod_not_serving_serves_none(self):
        self.assertIsNone(table_served([table(), table(serving=0), table()]))

    def test_a_pod_with_no_table_serves_none(self):
        # A pod no cluster took in: no table, and not serving.
        self.assertIsNone(table_served([table(), table(), table(serving=0, term=0, version=0)]))

    def test_pods_on_tables_apart_serve_none(self):
        self.assertIsNone(table_served([table(), table(version=8), table()]))
        self.assertIsNone(table_served([table(), table(), table(term=8589934593)]))

    def test_a_scrape_without_the_gauges_serves_none(self):
        self.assertIsNone(table_served([table(), "edgerush_rate_limit_keys 1\n", table()]))


if __name__ == "__main__":
    unittest.main()

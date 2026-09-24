"""The soak's table calls out what grew between its first minutes and its last, and nothing else."""

import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest

import soak


def summary(rows, load=None):
    with tempfile.TemporaryDirectory() as directory:
        out = pathlib.Path(directory)
        lines = ["seconds,rss_kb,fds,client_sockets,storage_bytes,exchanges,idle_upstream"]
        lines += [",".join(str(value) for value in row) for row in rows]
        (out / "soak.csv").write_text("\n".join(lines) + "\n")
        if load is not None:
            (out / "soak.load.json").write_text(json.dumps(load))
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            sys.argv = ["soak.py", str(out)]
            soak.main()
        return printed.getvalue()


def steady(storage_at_end=1000, rss_at_end=100_000):
    rows = []
    for seconds in range(0, 1810, 10):
        late = seconds >= 1500
        rows.append([seconds, rss_at_end if late else 100_000, 40, 2064,
                     storage_at_end if late else 1000, 10, 64])
    return rows


class Soak(unittest.TestCase):
    def test_a_steady_process_is_flat(self):
        self.assertIn("FLAT", summary(steady()))

    def test_storage_that_grows_is_called_out(self):
        printed = summary(steady(storage_at_end=2000))
        self.assertIn("GROWS: storage_bytes", printed)

    def test_memory_that_grows_is_called_out(self):
        self.assertIn("GROWS: rss_kb", summary(steady(rss_at_end=120_000)))

    def test_what_the_load_came_to_is_counted(self):
        load = {"statusCodeDistribution": {"200": 90, "502": 10}, "errorDistribution": {"timeout": 3}}
        self.assertIn("load: 100 answered, 10 not 2xx, 3 errors", summary(steady(), load))


if __name__ == "__main__":
    unittest.main()

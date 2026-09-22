"""The idle-memory collector includes workers once, even with multiple parent threads."""

import pathlib
import tempfile
import unittest

from rss import rss_tree


class ProcessMemory(unittest.TestCase):
    def test_descendants_are_counted_once(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = pathlib.Path(directory)
            for pid, rss, children in [(1, 10, '2 3'), (2, 20, '4'), (3, 30, ''), (4, 40, '')]:
                process = proc / str(pid)
                task = process / 'task' / str(pid)
                task.mkdir(parents=True)
                (process / 'status').write_text(f'VmRSS:\t{rss} kB\n')
                (task / 'children').write_text(children)
            thread = proc / '1' / 'task' / '5'
            thread.mkdir()
            (thread / 'children').write_text('2 99')
            self.assertEqual(rss_tree(1, proc), 100)
            with self.assertRaises(FileNotFoundError):
                rss_tree(99, proc)


if __name__ == '__main__':
    unittest.main()

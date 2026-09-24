"""An idle connection's answer is read whole, however it arrives, so the next one starts clean."""

import socket
import threading
import unittest

from idle import answer


def served(*pieces):
    """The client's end of a connection whose other end writes `pieces` and closes."""
    client, server = socket.socketpair()

    def write():
        for piece in pieces:
            server.sendall(piece)
        server.close()

    threading.Thread(target=write).start()
    return client


class Answer(unittest.TestCase):
    def test_a_head_and_its_body_are_read_whole(self):
        connection = served(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhel", b"lo", b"HTTP/1.1 204 ")
        self.assertEqual(answer(connection), b"HTTP/1.1 200 OK\r\ncontent-length: 5")
        connection.close()

    def test_a_head_in_pieces_is_read_whole(self):
        connection = served(b"HTTP/1.1 2", b"00 OK\r\nConten", b"t-Length: 0\r\n\r", b"\n")
        self.assertTrue(answer(connection).startswith(b"HTTP/1.1 200 OK"))
        connection.close()

    def test_an_answer_cut_short_is_an_error(self):
        for pieces in [(b"HTTP/1.1 200 OK\r\n",), (b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\n\r\nabc",)]:
            connection = served(*pieces)
            with self.assertRaises(ConnectionError):
                answer(connection)
            connection.close()


if __name__ == "__main__":
    unittest.main()
